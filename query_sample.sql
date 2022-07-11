WITH user_daily_summary AS (
   WITH bo_trades AS (
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
             WHERE sell_txn_date >= DATE_SUB('2022-03-01', INTERVAL 3 MONTH)
                   AND sell_txn_date < '2022-03-01'
             GROUP BY 1,2
            ) AS daily
      WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
   )
   ,payments AS(
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
                         , amount_usd
                         , 0)) AS withdrawal_usd
                 , SUM(IF(category IN ('Client Deposit')
                         , 1
                         , 0)) AS deposit_count
                 , SUM(IF(category IN ('Client Withdrawal')
                         , 1
                         , 0)) AS withdrawal_count
              FROM `business-intelligence-240201.bi.bo_payment_model`
             WHERE category IN ('Client Withdrawal', 'Client Deposit')
                   AND DATE(transaction_time) >= DATE_SUB('2022-03-01', INTERVAL 3 MONTH)
                   AND DATE(transaction_time) < '2022-03-01'
             GROUP BY 1, 2
            ) AS daily
      WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
   )
   ,mt5_trades AS (
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
              LEFT JOIN bi.bo_exchange_rate r
                   ON DATE(r.date) = DATE_SUB(DATE(mv_mt5_deal_aggregated.deal_date), INTERVAL 1 DAY)
                   AND g.currency = r.source_currency
                   AND r.target_currency = 'USD'
             WHERE DATE(deal_date) >= DATE_SUB('2022-03-01', INTERVAL 3 MONTH)
                   AND DATE(deal_date) < '2022-03-01'
             GROUP BY 1,2
            )
      WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
   )
   SELECT COALESCE(bo_trades.binary_user_id, payments.binary_user_id, mt5_trades.binary_user_id) AS binary_user_id
        , COALESCE(bo_trades.date, payments.date, mt5_trades.date) AS date
        , COALESCE(bo_trades.bo_turnover_usd,0) AS bo_turnover_usd
        , COALESCE(bo_trades.cumulative_bo_turnover_usd,0) AS cumulative_bo_turnover_usd
        , COALESCE(bo_trades.bo_winning_turnover_usd,0) AS bo_winning_turnover_usd
        , COALESCE(bo_trades.cumulative_bo_winning_turnover_usd,0) AS cumulative_bo_winning_turnover_usd
        , COALESCE(bo_trades.bo_pnl_usd,0) AS bo_pnl_usd
        , COALESCE(bo_trades.cumulative_bo_pnl_usd,0) AS cumulative_bo_pnl_usd
        , COALESCE(bo_trades.bo_win_count,0) AS bo_win_count
        , COALESCE(bo_trades.cumulative_bo_win_count) AS cumulative_bo_win_count
        , COALESCE(bo_trades.bo_profit_percentage,0) AS bo_profit_percentage
        , COALESCE(bo_trades.cumulative_bo_profit_percentage,0) AS cumulative_bo_profit_percentage
        , COALESCE(bo_trades.bo_contract_count,0) AS bo_contract_count
        , COALESCE(bo_trades.cumulative_bo_contract_count,0) AS cumulative_bo_contract_count
        , COALESCE(payments.deposit_usd,0) AS deposit_usd
        , COALESCE(payments.cumulative_deposit_usd,0) AS cumulative_deposit_usd
        , COALESCE(payments.withdrawal_usd,0) AS withdrawal_usd
        , COALESCE(payments.cumulative_withdrawal_usd,0) AS cumulative_withdrawal_usd
        , COALESCE(payments.deposit_count,0) AS deposit_count
        , COALESCE(payments.cumulative_deposit_count,0) AS cumulative_deposit_count
        , COALESCE(payments.withdrawal_count,0) AS withdrawal_count
        , COALESCE(payments.cumulative_withdrawal_count,0) AS cumulative_withdrawal_count
        , COALESCE(mt5_trades.mt5_pnl_usd,0) AS mt5_pnl_usd
        , COALESCE(mt5_trades.cumulative_mt5_pnl_usd,0) AS cumulative_mt5_pnl_usd
        , COALESCE(mt5_trades.mt5_win_count,0) AS mt5_win_count
        , COALESCE(mt5_trades.cumulative_mt5_win_count,0) AS cumulative_mt5_win_count
        , COALESCE(mt5_trades.mt5_contract_count,0) AS mt5_contract_count
        , COALESCE(mt5_trades.cumulative_mt5_contract_count,0) AS cumulative_mt5_contract_count
     FROM bo_trades
     FULL JOIN payments
          ON payments.binary_user_id = bo_trades.binary_user_id
          AND payments.date = bo_trades.date
     FULL JOIN mt5_trades
          ON mt5_trades.binary_user_id = COALESCE(bo_trades.binary_user_id, payments.binary_user_id)
          AND mt5_trades.date = COALESCE(bo_trades.date, payments.date)
    ORDER BY binary_user_id, date)
,active_users AS (
   SELECT *
     FROM (
            SELECT summary.*
                 , cumulative_bo_turnover_usd AS metric_value
                 , 'bo_turnover_usd' AS metric
                 , CASE
                    WHEN true= TRUE
                        THEN (cumulative_bo_turnover_usd >= 1000 AND cumulative_bo_turnover_usd - bo_turnover_usd < 1000)
                    WHEN true= FALSE
                        THEN cumulative_bo_turnover_usd >= 1000
                    ELSE FALSE END AS meet
                 , up.residence AS country
                 , up.email
                 , up.loginid_list
              FROM user_daily_summary summary
              JOIN bi.user_profile up ON up.binary_user_id = summary.binary_user_id
             WHERE date =  DATE_SUB('2022-03-01' , INTERVAL 1 DAY)
         )
    WHERE meet = TRUE
)
, active_users_non_internal AS (
   SELECT * EXCEPT(client_type)
     FROM (
         SELECT active_users.*
              , CASE
                 WHEN landing_company IS NOT NULL THEN 'internal'
                 WHEN status_code = 'internal_client' THEN 'internal'
                 ELSE 'external' END AS client_type
            FROM active_users
            LEFT JOIN bi.dict_internal_email die
                 ON die.domain = REGEXP_EXTRACT(active_users.email, r'@(.*)')
            LEFT JOIN bi.bo_client_status
                 ON bo_client_status.binary_user_id = active_users.binary_user_id
         ) AS tmp
   WHERE client_type <> 'internal'
)
,top_country AS (
   SELECT country
        , count_user
        , percentage
        , CASE
           WHEN count_user <= 300 THEN count_user
           WHEN count_user > 300 AND status='below' THEN count_user
           WHEN count_user > 300 AND status='above' THEN ROUND(GREATEST(300 - count_user_below,0)/(count_country_above))
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
                       WHEN (ROUND(COUNT(DISTINCT binary_user_id) / SUM(COUNT(DISTINCT binary_user_id)) OVER (), 2) * 100) > 5
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
GROUP BY 1  
