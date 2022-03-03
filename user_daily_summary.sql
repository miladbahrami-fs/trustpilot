CREATE VIEW `business-intelligence-240201.development.user_daily_summary` AS
WITH bo_trades AS (
  SELECT date
        ,binary_user_id
        ,bo_turnover_usd
        ,SUM(bo_turnover_usd) OVER(w) AS cumulative_bo_turnover_usd
        ,bo_pnl_usd
        ,SUM(bo_pnl_usd) OVER(w) AS cumulative_bo_pnl_usd
        ,SAFE_DIVIDE(bo_pnl_usd,bo_turnover_usd)*100 AS bo_profit_percentage
        ,bo_contract_count
        ,SUM(bo_contract_count) OVER(w) AS cumulative_bo_contract_count
    FROM (
        SELECT DATE(sell_txn_date) AS date
            ,binary_user_id
            ,SUM(sum_buy_price_usd) AS bo_turnover_usd
            ,-SUM(sum_buy_price_usd_minus_sell_price_usd) AS bo_pnl_usd
            ,SUM(total_contracts) AS bo_contract_count
        FROM `business-intelligence-240201.bi.mv_bo_pnl_summary` where year_month='2022-01-01'
        GROUP BY 1,2
        ) AS daily
  WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
),
payments AS(
  SELECT date
        ,binary_user_id
        ,deposit_usd
        ,SUM(deposit_usd) OVER(w) AS cumulative_deposit_usd
        ,withdrawal_usd
        ,SUM(withdrawal_usd) OVER(w) AS cumulative_withdrawal_usd
        ,deposit_count
        ,SUM(deposit_count) OVER(w) AS cumulative_deposit_count
        ,withdrawal_count
        ,SUM(withdrawal_count) OVER(w) AS cumulative_withdrawal_count
    FROM (
        SELECT date
              ,binary_user_id
              ,SUM(IF(type='Deposit',amount_usd,0)) AS deposit_usd
              ,SUM(IF(type='Withdrawal',amount_usd,0)) AS withdrawal_usd
              ,COUNT(IF(type='Deposit',payment_id,0)) AS deposit_count
              ,COUNT(IF(type='Withdrawal',payment_id,0)) AS withdrawal_count
         FROM (
            SELECT DATE(transaction_time) AS date
                  ,binary_user_id
                  ,client_loginid as loginid
                  ,amount_usd
                  ,payment_id
                  ,CASE
                     WHEN(amount_usd>0 AND category IN ('Client Deposit','Payment Agent Deposit')) THEN 'Deposit'
                     WHEN(amount_usd<0 AND category IN('Client Withdrawal','Payment Agent Withdrawal')) THEN 'Withdrawal'
                     ELSE 'Other' END AS type
             FROM `business-intelligence-240201.bi.bo_payment_model` where date(transaction_time)>='2022-01-01'
             ) AS pm
        WHERE type IN ('Deposit','Withdrawal')
        GROUP BY 1,2
        ) AS daily
  WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
),
mt5_trades AS (
  SELECT date
        ,binary_user_id
        ,closed_pnl_usd as mt5_pnl_usd
        ,SUM(closed_pnl_usd) OVER(w) AS cumulative_mt5_pnl_usd
        ,number_of_trades as mt5_contract_count
        ,SUM(number_of_trades) OVER(w) AS cumulative_mt5_contract_count
    FROM(
        SElECT date
              ,binary_user_id
              ,SUM(closed_pnl_usd) AS closed_pnl_usd
              ,SUM(number_of_trades) AS number_of_trades
          FROM bi.trades
         WHERE platform = 'MT5' AND date >= '2022-01-01'
         GROUP BY 1,2
        )
  WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
)
SELECT COALESCE(tmp.binary_user_id,mt5_trades.binary_user_id) AS binary_user_id
      ,COALESCE(tmp.date,mt5_trades.date) AS date
      ,bo_turnover_usd
      ,cumulative_bo_turnover_usd
      ,bo_pnl_usd
      ,cumulative_bo_pnl_usd
      ,bo_profit_percentage
      ,bo_contract_count
      ,cumulative_bo_contract_count
      ,deposit_usd
      ,cumulative_deposit_usd
      ,withdrawal_usd
      ,cumulative_withdrawal_usd
      ,deposit_count
      ,cumulative_deposit_count
      ,withdrawal_count
      ,cumulative_withdrawal_count
      ,mt5_trades.mt5_pnl_usd
      ,mt5_trades.cumulative_mt5_pnl_usd
      ,mt5_trades.mt5_contract_count
      ,mt5_trades.cumulative_mt5_contract_count
 FROM (
      SELECT COALESCE(bo_trades.binary_user_id,payments.binary_user_id) AS binary_user_id
            ,COALESCE(bo_trades.date,payments.date) as date
            ,bo_trades.bo_turnover_usd
            ,bo_trades.cumulative_bo_turnover_usd
            ,bo_trades.bo_pnl_usd
            ,bo_trades.cumulative_bo_pnl_usd
            ,bo_trades.bo_profit_percentage
            ,bo_trades.bo_contract_count
            ,bo_trades.cumulative_bo_contract_count
            ,payments.deposit_usd
            ,payments.cumulative_deposit_usd
            ,payments.withdrawal_usd
            ,payments.cumulative_withdrawal_usd
            ,payments.deposit_count
            ,payments.cumulative_deposit_count
            ,payments.withdrawal_count
            ,payments.cumulative_withdrawal_count
      FROM bo_trades
      FULL JOIN payments on payments.binary_user_id = bo_trades.binary_user_id  AND payments.date = bo_trades.date
      ) AS tmp
 FULL JOIN mt5_trades on mt5_trades.binary_user_id = tmp.binary_user_id AND mt5_trades.date = tmp.date
ORDER BY binary_user_id,date
