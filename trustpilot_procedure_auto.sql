CREATE OR REPLACE PROCEDURE development.sp_get_trustpilot_invitation_list(
  report_date DATE
, invitation_limit INT64
, metric STRING
, percentage_threshold INT64
, threshold FLOAT64
, first_time_meet BOOL
)
OPTIONS (
   description = """
   The idea of this query is to get the list of users who have passed the specified threshold regarding the specified metric in the selected date;
   excluding those clients whose exceeded the threshold in any previous date. The metric is calculated over the specified time range.
   Then getting users in each active country based on:
      - If active client count below invitation_limit, we take all active clients in that country
      - If active client count above invitation_limit but less than threshold, we take all active clients in that country
      - Else we take percentage of active clients based on country precentage

   PARAMETERS
      - report_date          : The date to get the list for
      - invitation_limit     : Number of invitations to send
      - metric               : Metric name
      - percentage_threshold : If percentage of active users in a country was below this threshod, the whole users of that country will be considered
      - threshold            : Metric threshold, users who exceed this threshold will be selected
      - first_time_meet      : If true, users who have passed the threshold in the selected date for the first time will be selected

   VARIABLES
      - lookback        : Time range for calculating metrics, in Months

   Available Metrics :
      'bo_turnover_usd', 'bo_winning_turnover_usd', 'bo_pnl_usd', 'bo_profit_usd', 'bo_win_count', 'bo_profit_percentage'
      , 'bo_contract_count', 'deposit_usd', 'withdrawal_usd', 'deposit_count', 'withdrawal_count', 'withdrawal_deposit_percentage'
      , 'mt5_pnl_usd', 'mt5_profit_usd', 'mt5_win_count', 'mt5_contract_count' , 'cumulative_pnl_usd' , 'cumulative_withdrawal_to_deposit'
      , 'auto' 

 """ )
BEGIN
DECLARE _user_daily_summary STRING;
DECLARE _active_users STRING;
DECLARE _active_users_auto STRING;
DECLARE _final_users STRING;
DECLARE _query_string STRING;
DECLARE lookback INT64;
SET lookback = 3;
SET _user_daily_summary = """
WITH user_daily_summary AS (
   WITH bo_trades AS (
      SELECT *
        FROM (
              SELECT date
                   , binary_user_id
                   , bo_turnover_usd
                   , SUM(bo_turnover_usd) OVER(w) AS cumulative_bo_turnover_usd
                   , bo_winning_turnover_usd
                   , SUM(bo_winning_turnover_usd) OVER(w) AS cumulative_bo_winning_turnover_usd
                   , bo_win_count
                   , SUM(bo_win_count) OVER(w) AS cumulative_bo_win_count
                   , bo_pnl_usd
                   , SUM(bo_pnl_usd) OVER(w) AS cumulative_bo_pnl_usd
                   , SAFE_DIVIDE(bo_pnl_usd,bo_turnover_usd)*100 AS bo_profit_percentage
                   , SAFE_DIVIDE(SUM(bo_pnl_usd) OVER(w),SUM(bo_turnover_usd) OVER(w))*100 AS cumulative_bo_profit_percentage
                   , bo_contract_count
                   , SUM(bo_contract_count) OVER(w) AS cumulative_bo_contract_count
                FROM (
                      SELECT DATE(sell_txn_date) AS date
                           , binary_user_id
                           , SUM(sum_buy_price_usd) AS bo_turnover_usd
                           , SUM(winning_turnover_usd) AS bo_winning_turnover_usd
                           , SUM(client_win_count) As bo_win_count
                           , -SUM(sum_buy_price_usd_minus_sell_price_usd) AS bo_pnl_usd
                           , SUM(total_contracts) AS bo_contract_count
                        FROM `business-intelligence-240201.bi.mv_bo_pnl_summary`
                       WHERE sell_txn_date >= DATE_SUB('"""||report_date||"""', INTERVAL """||lookback||""" MONTH)
                         AND sell_txn_date < '"""||report_date||"""'
                       GROUP BY 1,2
                     ) AS daily
              WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
            ) AS daily_summary
       WHERE date = DATE_SUB ('"""||report_date||"""', INTERVAL 1 DAY)
      )
   ,payments AS (
      SELECT *
        FROM (
              SELECT date
                   , binary_user_id
                   , deposit_usd
                   , SUM(deposit_usd) OVER(w) AS cumulative_deposit_usd
                   , withdrawal_usd
                   , SUM(withdrawal_usd) OVER(w) AS cumulative_withdrawal_usd
                   , deposit_count
                   , SUM(deposit_count) OVER(w) AS cumulative_deposit_count
                   , withdrawal_count
                   , SUM(withdrawal_count) OVER(w) AS cumulative_withdrawal_count
                FROM (
                      SELECT DATE(transaction_time) AS date
                           , binary_user_id
                           , SUM(IF(category IN ('Client Deposit')
                                , amount_usd
                                , 0)) AS deposit_usd
                           , SUM(IF(category IN ('Client Withdrawal')
                                , -amount_usd
                                , 0)) AS withdrawal_usd
                           , SUM(IF(category IN ('Client Deposit')
                                , 1
                                , 0)) AS deposit_count
                           , SUM(IF(category IN ('Client Withdrawal')
                                , 1
                                , 0)) AS withdrawal_count
                             FROM `business-intelligence-240201.bi.bo_payment_model`
                            WHERE category IN ('Client Withdrawal', 'Client Deposit')
                              AND DATE(transaction_time) >= DATE_SUB('"""||report_date||"""', INTERVAL """||lookback||""" MONTH)
                              AND DATE(transaction_time) < '"""||report_date||"""'
                            GROUP BY 1, 2
                      ) AS daily
              WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
              ) AS daily_summary
       WHERE date = DATE_SUB ('"""||report_date||"""', INTERVAL 1 DAY)
      )
   ,mt5_trades AS (
      SELECT *
        FROM (
              SELECT date
                   , binary_user_id
                   , closed_pnl_usd as mt5_pnl_usd
                   , SUM(closed_pnl_usd) OVER(w) AS cumulative_mt5_pnl_usd
                   , mt5_win_count
                   , SUM(mt5_win_count) OVER(w) AS cumulative_mt5_win_count
                   , number_of_trades as mt5_contract_count
                   , SUM(number_of_trades) OVER(w) AS cumulative_mt5_contract_count
                FROM (
                      SElECT DATE(deal_date) AS date
                           , binary_user_id
                           , ROUND(SUM(CASE WHEN mv_mt5_deal_aggregated.entry IN ('out','out_by')
                              THEN (mv_mt5_deal_aggregated.sum_profit + mv_mt5_deal_aggregated.sum_storage) * r.rate
                              ELSE 0 END),4) AS closed_pnl_usd
                           , SUM(count_win_deals) AS mt5_win_count
                           , SUM(count_deals) AS number_of_trades
                        FROM bi.mv_mt5_deal_aggregated
                        JOIN bi.mt5_user ON mt5_user.login=mv_mt5_deal_aggregated.login
                        LEFT JOIN bi.mt5_trading_group g ON mt5_user.group = g.group AND mt5_user.srvid = g.srvid
                        LEFT JOIN bi.bo_exchange_rate r ON DATE(r.date) = DATE_SUB(DATE(mv_mt5_deal_aggregated.deal_date), INTERVAL 1 DAY)
                         AND g.currency = r.source_currency
                         AND r.target_currency = 'USD'
                       WHERE DATE(deal_date) >= DATE_SUB('"""||report_date||"""', INTERVAL """||lookback||""" MONTH)
                         AND DATE(deal_date) < '"""||report_date||"""'
                       GROUP BY 1,2
                      ) AS daily
              WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
              ) AS daily_summary
       WHERE date = DATE_SUB ('"""||report_date||"""', INTERVAL 1 DAY)
      )
   SELECT COALESCE(bo_trades.binary_user_id, payments.binary_user_id, mt5_trades.binary_user_id) AS binary_user_id
        , COALESCE(bo_trades.date, payments.date, mt5_trades.date) AS date
        , bo_trades.bo_turnover_usd
        , bo_trades.cumulative_bo_turnover_usd
        , bo_trades.bo_winning_turnover_usd
        , bo_trades.cumulative_bo_winning_turnover_usd
        , bo_trades.bo_pnl_usd  
        , bo_trades.cumulative_bo_pnl_usd
        , bo_trades.bo_win_count
        , bo_trades.cumulative_bo_win_count
        , bo_trades.bo_profit_percentage
        , bo_trades.cumulative_bo_profit_percentage
        , bo_trades.bo_contract_count
        , bo_trades.cumulative_bo_contract_count
        , payments.deposit_usd
        , payments.cumulative_deposit_usd
        , payments.withdrawal_usd
        , payments.cumulative_withdrawal_usd
        , payments.deposit_count
        , payments.cumulative_deposit_count
        , payments.withdrawal_count
        , payments.cumulative_withdrawal_count
        , mt5_trades.mt5_pnl_usd
        , mt5_trades.cumulative_mt5_pnl_usd
        , mt5_trades.mt5_win_count
        , mt5_trades.cumulative_mt5_win_count
        , mt5_trades.mt5_contract_count
        , mt5_trades.cumulative_mt5_contract_count
        , COALESCE(bo_trades.bo_pnl_usd, 0) 
            + COALESCE(mt5_trades.mt5_pnl_usd, 0) AS pnl_usd
        , COALESCE(bo_trades.cumulative_bo_pnl_usd,0) 
            + COALESCE(mt5_trades.cumulative_mt5_pnl_usd,0) AS cumulative_pnl_usd
        , COALESCE(bo_trades.bo_contract_count,0) 
            + COALESCE(mt5_trades.mt5_contract_count,0) 
            + COALESCE(payments.deposit_count,0) 
            + COALESCE(payments.withdrawal_count,0) AS contract_count
        , COALESCE(bo_trades.cumulative_bo_contract_count,0) 
            + COALESCE(mt5_trades.cumulative_mt5_contract_count,0) 
            + COALESCE(payments.cumulative_deposit_count,0) 
            + COALESCE(payments.cumulative_withdrawal_count,0) AS cumulative_contract_count
     FROM bo_trades
     FULL JOIN payments
          ON payments.binary_user_id = bo_trades.binary_user_id
          AND payments.date = bo_trades.date
     FULL JOIN mt5_trades
          ON mt5_trades.binary_user_id = COALESCE(bo_trades.binary_user_id, payments.binary_user_id)
          AND mt5_trades.date = COALESCE(bo_trades.date, payments.date)
    ORDER BY binary_user_id, date) """ ;
SET _active_users = """
,active_users AS (
   SELECT *
     FROM (
            SELECT summary.binary_user_id
                 , summary.date
                 , cumulative_"""||metric||""" AS metric_value
                 , '"""||metric||"""' AS metric
                 , CASE
                    WHEN """||first_time_meet||"""= TRUE
                        THEN (cumulative_"""||metric||""" >= """||threshold||""" AND cumulative_"""||metric||""" - """||metric||""" < """||threshold||""")
                    WHEN """||first_time_meet||"""= FALSE
                        THEN cumulative_"""||metric||""" >= """||threshold||"""
                    ELSE FALSE END AS meet
                 , up.residence AS country
                 , up.email
                 , up.loginid_list
              FROM user_daily_summary summary
              JOIN bi.user_profile up ON up.binary_user_id = summary.binary_user_id
             WHERE date =  DATE_SUB('"""||report_date||"""' , INTERVAL 1 DAY)
         )
    WHERE meet = TRUE
)
""" ;
SET _active_users_auto = """
,active_users AS (
   SELECT *
     FROM (
            SELECT summary.binary_user_id
                 , summary.date
                 , cumulative_withdrawal_count AS metric_value
                 , 'Auto' AS metric
                 , (cumulative_contract_count >= 1000 AND cumulative_contract_count - contract_count < 1000) 
                    AND (cumulative_pnl_usd >= 100) AS meet
                 , up.residence AS country
                 , up.email
                 , up.loginid_list
              FROM user_daily_summary summary
              JOIN bi.user_profile up ON up.binary_user_id = summary.binary_user_id
             WHERE date =  DATE_SUB('"""||report_date||"""' , INTERVAL 1 DAY)
         )
    WHERE meet = TRUE
)
""" ;
SET _final_users = """
, active_users_non_internal AS (
   SELECT * EXCEPT(client_type)
     FROM (
         SELECT active_users.*
              , CASE
                 WHEN landing_company IS NOT NULL THEN 'internal'
                 WHEN status_code = 'internal_client' THEN 'internal'
                 ELSE 'external' END AS client_type
              , COALESCE(unsubscribed, 'false') AS unsubscribed
            FROM active_users
            LEFT JOIN bi.dict_internal_email die
                 ON die.domain = REGEXP_EXTRACT(active_users.email, r'@(.*)')
            LEFT JOIN bi.bo_client_status
                 ON bo_client_status.binary_user_id = active_users.binary_user_id
            LEFT JOIN http_api_production.users api
                 ON api.id = CAST(active_users.binary_user_id AS STRING)
         ) AS tmp
   WHERE client_type <> 'internal' AND unsubscribed <> 'true'
)
,top_country AS (
   SELECT country
        , count_user
        , percentage
        , CASE
           WHEN count_user <= """||invitation_limit||""" THEN count_user
           WHEN count_user > """||invitation_limit||""" AND status='below' THEN count_user
           WHEN count_user > """||invitation_limit||""" AND status='above' THEN ROUND(GREATEST("""||invitation_limit||""" - count_user_below,0)/(count_country_above))
           END AS final_number_user
     FROM (
         SELECT country
              , count_user
              , percentage
              , status
              , COUNT(IF(status='above',1,0)) OVER (partition BY status) AS count_country_above
              , SUM(IF(status='below',count_user,0)) OVER () AS count_user_below
           FROM (
               SELECT country
                    , COUNT(DISTINCT binary_user_id) AS count_user
                    , ROUND(COUNT(DISTINCT binary_user_id) / SUM(COUNT(DISTINCT binary_user_id)) OVER (),2) * 100 AS percentage
                    , CASE
                       WHEN (ROUND(COUNT(DISTINCT binary_user_id) / SUM(COUNT(DISTINCT binary_user_id)) OVER (), 2) * 100) > """||percentage_threshold||"""
                       THEN 'above'
                       ELSE 'below' END AS status
                 FROM active_users_non_internal
                GROUP BY 1
                ORDER BY 2 DESC
            ) AS tmp
         )
)
,final_users AS (
   SELECT row_number() OVER (partition BY active_users_non_internal.country ORDER BY metric_value DESC) AS rownum
        , binary_user_id
        , metric
        , metric_value
        , active_users_non_internal.country
        , email
        , tc.final_number_user
        , loginid_list
     FROM active_users_non_internal
     LEFT JOIN top_country AS tc ON active_users_non_internal.country = tc.country
)
SELECT binary_user_id
     , MAX(first_name) AS first_name
     , MAX(last_name) AS last_name
     , MAX(email) AS email
  FROM (
    SELECT final_users.binary_user_id
         , LAST_VALUE(bc.first_name) OVER(PARTITION BY bc.binary_user_id ORDER BY date_joined) AS first_name
         , LAST_VALUE(bc.last_name) OVER(PARTITION BY bc.binary_user_id ORDER BY date_joined) AS last_name
         , final_users.email
      FROM final_users
      LEFT JOIN bi.bo_client bc ON bc.binary_user_id = final_users.binary_user_id
     WHERE rownum <= final_number_user
   )
GROUP BY 1 """ ;
IF metric = 'auto' 
THEN SET _query_string = _user_daily_summary  || _active_users_auto || _final_users; 
ELSE SET _query_string = _user_daily_summary  || _active_users || _final_users;
END IF;
EXECUTE IMMEDIATE (_query_string);
END;
