CREATE OR REPLACE TABLE FUNCTION development.trustpilot_invitation(start_date DATE, end_date DATE, invitation_limit INT64, threshold INT64, bo_turnover_limit FLOAT64, withdrawal_limit FLOAT64, profit_limit FLOAT64, mt5_pnl_limit FLOAT64) AS
WITH active_users AS(
  SELECT stats.*
        ,CASE
            WHEN (cumulative_bo_turnover_usd >= bo_turnover_limit AND prev_cum_bo_turnover <bo_turnover_limit) THEN 'cumulative_bo_turnover_usd'
            WHEN (cumulative_withdrawal_usd >= withdrawal_limit AND prev_cum_withdrawal < withdrawal_limit) THEN 'cumulative_withdrawal_usd'
            WHEN bo_profit_percentage>= profit_limit THEN 'bo_profit_percentage'
            WHEN (cumulative_mt5_pnl_usd >= mt5_pnl_limit AND prev_cum_mt5_pnl < mt5_pnl_limit) THEN 'cumulative_mt5_pnl_usd'
        ELSE 'none'
            END AS metric
        ,up.residence AS country
        ,up.email
        ,up.loginid_list
    FROM (
        SELECT *
          FROM (
            SELECT binary_user_id
                    ,date
                    ,bo_turnover_usd
                    ,cumulative_bo_turnover_usd
                    ,LAG(cumulative_bo_turnover_usd) OVER (PARTITION BY binary_user_id ORDER BY date) AS prev_cum_bo_turnover
                    ,bo_pnl_usd
                    ,cumulative_bo_pnl_usd
                    ,LAG(cumulative_bo_pnl_usd) OVER (PARTITION BY binary_user_id ORDER BY date) AS prev_cum_bo_pnl
                    ,bo_profit_percentage
                    ,bo_contract_count
                    ,cumulative_bo_contract_count
                    ,LAG(cumulative_bo_contract_count) OVER (PARTITION BY binary_user_id ORDER BY date) AS prev_cum_bo_contract_count
                    ,deposit_usd
                    ,cumulative_deposit_usd
                    ,LAG(cumulative_deposit_usd) OVER (PARTITION BY binary_user_id ORDER BY date) AS prev_cum_deposit
                    ,withdrawal_usd
                    ,cumulative_withdrawal_usd
                    ,LAG(cumulative_withdrawal_usd) OVER (PARTITION BY binary_user_id ORDER BY date) AS prev_cum_withdrawal
                    ,deposit_count
                    ,cumulative_deposit_count
                    ,LAG(cumulative_deposit_count) OVER (PARTITION BY binary_user_id ORDER BY date) AS prev_cum_deposit_count
                    ,withdrawal_count
                    ,cumulative_withdrawal_count
                    ,LAG(cumulative_withdrawal_count) OVER (PARTITION BY binary_user_id ORDER BY date) AS prev_cum_withdrawal_count
                    ,mt5_pnl_usd
                    ,cumulative_mt5_pnl_usd
                    ,LAG(cumulative_mt5_pnl_usd) OVER (PARTITION BY binary_user_id ORDER BY date) AS prev_cum_mt5_pnl
                    ,mt5_contract_count
                    ,cumulative_mt5_contract_count
                    ,LAG(cumulative_mt5_contract_count) OVER (PARTITION BY binary_user_id ORDER BY date) AS prev_cum_mt5_contract_count
            FROM development.user_daily_summary
            )
        WHERE date >= start_date AND date < end_date
        ) AS stats
    JOIN bi.user_profile up ON up.binary_user_id = stats.binary_user_id
   )
,top_country AS (
  SELECT country
        ,metric
        ,count_user
        ,percentage
        ,CASE
            WHEN count_user <= invitation_limit THEN count_user
            WHEN count_user > invitation_limit AND status='below' THEN count_user
            WHEN count_user > invitation_limit AND status='above' THEN 230
        END AS final_number_user
    FROM (
      SELECT country
            ,active_users.metric
            ,COUNT(DISTINCT binary_user_id) AS count_user
            ,ROUND(COUNT(DISTINCT binary_user_id) / SUM(COUNT(DISTINCT binary_user_id)) OVER (),2) * 100 AS percentage
            ,CASE
                WHEN (ROUND(COUNT(DISTINCT binary_user_id) / SUM(COUNT(DISTINCT binary_user_id)) OVER (), 2) * 100) > threshold THEN 'above'
                ELSE 'below'
            END AS status
       FROM active_users
      WHERE metric <> 'none'
      GROUP BY 1,2
      ORDER BY 3 DESC
      )
)
,final_users AS (
  SELECT row_number() OVER (partition BY au.country,au.metric ORDER BY au.bo_turnover_usd DESC) AS rownum
        ,au.metric
        ,au.binary_user_id
        ,au.country
        ,au.email
        ,tc.final_number_user
        ,au.loginid_list
    FROM (SELECT * FROM active_users WHERE metric = 'cumulative_bo_turnover_usd') AS au
    LEFT JOIN top_country AS tc ON au.country = tc.country
   UNION ALL
  SELECT row_number() OVER (partition BY au.country,au.metric ORDER BY au.withdrawal_usd DESC) AS rownum
        ,au.metric
        ,au.binary_user_id
        ,au.country
        ,au.email
        ,tc.final_number_user
        ,au.loginid_list
    FROM (SELECT * FROM active_users WHERE metric = 'cumulative_withdrawal_usd') AS au
    LEFT JOIN top_country AS tc ON au.country = tc.country
   UNION ALL
  SELECT row_number() OVER (partition BY au.country,au.metric ORDER BY au.bo_profit_percentage DESC) AS rownum
        ,au.metric
        ,au.binary_user_id
        ,au.country
        ,au.email
        ,tc.final_number_user
        ,au.loginid_list
    FROM (SELECT * FROM active_users WHERE metric = 'bo_profit_percentage') AS au
    LEFT JOIN top_country AS tc ON au.country = tc.country
   UNION ALL
  SELECT row_number() OVER (partition BY au.country,au.metric ORDER BY au.mt5_pnl_usd DESC) AS rownum
        ,au.metric
        ,au.binary_user_id
        ,au.country
        ,au.email
        ,tc.final_number_user
        ,au.loginid_list
   FROM (SELECT * FROM active_users WHERE metric = 'cumulative_mt5_pnl_usd') AS au
   LEFT JOIN top_country AS tc ON au.country = tc.country
)
SELECT binary_user_id
      ,metric
      ,MAX(first_name) AS first_name
      ,MAX(last_name) AS last_name
      ,MAX(email) AS email
      ,MAX(country) AS country
      ,MAX(loginid_list) AS loginid_list
  FROM(
    SELECT final_users.binary_user_id
          ,metric
          ,LAST_VALUE(bc.first_name) OVER(PARTITION BY bc.binary_user_id ORDER BY date_joined) AS first_name
          ,LAST_VALUE(bc.last_name) OVER(PARTITION BY bc.binary_user_id ORDER BY date_joined) AS last_name
          ,final_users.email
          ,final_users.loginid_list
          ,country
      FROM final_users
      LEFT JOIN bi.bo_client bc ON bc.binary_user_id = final_users.binary_user_id
     WHERE rownum <= final_number_user
   )
GROUP BY 1,2
