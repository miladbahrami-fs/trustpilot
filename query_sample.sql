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
              , SUM(IF(sum_buy_price_usd_minus_sell_price_usd<0
                       , sum_buy_price_usd
                       , 0)) AS bo_winning_turnover_usd
              , SUM(IF(sum_buy_price_usd_minus_sell_price_usd<0
                       , 1
                       , 0)) As bo_win_count
              , -SUM(sum_buy_price_usd_minus_sell_price_usd) AS bo_pnl_usd
              , SUM(total_contracts) AS bo_contract_count
           FROM `business-intelligence-240201.bi.mv_bo_pnl_summary` 
          WHERE DATE(year_month)>= DATE_TRUNC(DATE_SUB(current_date(), INTERVAL 90 DAY),MONTH) 
                 AND sell_txn_date >=DATE_SUB(current_date(), INTERVAL 90 DAY)
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
              , SUM(IF(category IN ('Client Deposit','Payment Agent Deposit')
                       , amount_usd
                       , 0)) AS deposit_usd
              , SUM(IF(category IN ('Client Withdrawal','Payment Agent Withdrawal')
                       , amount_usd
                       , 0)) AS withdrawal_usd
              , SUM(IF(category IN ('Client Deposit','Payment Agent Deposit')
                       , 1
                       , 0)) AS deposit_count
              , SUM(IF(category IN ('Client Withdrawal','Payment Agent Withdrawal')
                       , 1
                       , 0)) AS withdrawal_count   
           FROM `business-intelligence-240201.bi.bo_payment_model`
          WHERE category IN ('Client Withdrawal', 'Payment Agent Withdrawal'
                             , 'Client Deposit', 'Payment Agent Deposit')
                 AND DATE(transaction_time)>= DATE_SUB(current_date(), INTERVAL 90 DAY)
          GROUP BY 1, 2
         ) AS daily
   WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ))
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
              , SUM(sum_profit) AS closed_pnl_usd
              , SUM(count_win_deals) AS mt5_win_count
              , SUM(count_deals) AS number_of_trades
           FROM bi.mv_mt5_deal_aggregated
           JOIN bi.mt5_user ON mt5_user.login=mv_mt5_deal_aggregated.login
          WHERE DATE(deal_date) >= DATE_SUB(current_date(), INTERVAL 90 DAY)
          GROUP BY 1,2
         )
   WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
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
     FROM bo_trades
     FULL JOIN payments
          ON payments.binary_user_id = bo_trades.binary_user_id
          AND payments.date = bo_trades.date
     FULL JOIN mt5_trades 
          ON mt5_trades.binary_user_id = COALESCE(bo_trades.binary_user_id, payments.binary_user_id)
          AND mt5_trades.date = COALESCE(bo_trades.date, payments.date)
    ORDER BY binary_user_id, date)
,active_users AS (
SELECT summary.*
     , cumulative_bo_turnover_usd AS metric_value
     , 'bo_turnover_usd' AS metric
     , CASE
           WHEN true= TRUE 
              THEN (cumulative_bo_turnover_usd >= 1000 AND cumulative_bo_turnover_usd - bo_turnover_usd < 1000) 
           WHEN true= FALSE
              THEN cumulative_bo_turnover_usd >= 1000 
           ELSE FALSE
        END AS meet
     , up.residence AS country
     , up.email
     , up.loginid_list
  FROM user_daily_summary summary
  JOIN bi.user_profile up ON up.binary_user_id = summary.binary_user_id
 WHERE date >= '2022-02-01' AND date < '2022-02-02'
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
                    ELSE 'below'
                 END AS status
           FROM active_users
          WHERE meet = TRUE
          GROUP BY 1
          ORDER BY 2 DESC
         ) AS tmp
      )
)
,final_users AS (
SELECT row_number() OVER (partition BY active_users.country ORDER BY metric_value DESC) AS rownum
     , binary_user_id
     , metric
     , metric_value
     , active_users.country
     , email
     , tc.final_number_user
     , loginid_list
  FROM active_users 
  LEFT JOIN top_country AS tc ON active_users.country = tc.country
 WHERE meet = TRUE
)
SELECT binary_user_id
     , metric
     , MAX(metric_value) AS metric_value
     , MAX(first_name) AS first_name
     , MAX(last_name) AS last_name
     , MAX(email) AS email
     , MAX(country) AS country
     , MAX(loginid_list) AS loginid_list
  FROM(
    SELECT final_users.binary_user_id
         , LAST_VALUE(bc.first_name) OVER(PARTITION BY bc.binary_user_id ORDER BY date_joined) AS first_name
         , LAST_VALUE(bc.last_name) OVER(PARTITION BY bc.binary_user_id ORDER BY date_joined) AS last_name
         , final_users.email
         , final_users.loginid_list
         , metric
         , metric_value
         , country
      FROM final_users
      LEFT JOIN bi.bo_client bc ON bc.binary_user_id = final_users.binary_user_id
     WHERE rownum <= final_number_user
   )
GROUP BY 1,2 
ORDER BY metric_value 