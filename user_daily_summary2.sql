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
       , bo_profit_usd
       , SUM(bo_profit_usd) OVER(w) AS cumulative_bo_profit_usd
       , bo_loss_usd
       , SUM(bo_loss_usd) OVER(w) AS cumulative_bo_loss_usd
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
           , SUM(IF(sum_buy_price_usd_minus_sell_price_usd<0
                   , -sum_buy_price_usd_minus_sell_price_usd
                   , 0)) As bo_profit_usd
           , SUM(IF(sum_buy_price_usd_minus_sell_price_usd>0
                   , -sum_buy_price_usd_minus_sell_price_usd
                   , 0)) As bo_loss_usd
           , SUM(-sum_buy_price_usd_minus_sell_price_usd) AS bo_pnl_usd
           , SUM(total_contracts) AS bo_contract_count
        FROM `business-intelligence-240201.bi.mv_bo_pnl_summary` where year_month='2022-01-01'
       GROUP BY 1,2
        ) AS daily
  WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
),
payments AS(
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
        , SAFE_DIVIDE(-withdrawal_usd,deposit_usd)*100 AS withdrawal_deposit_percentage
        , SAFE_DIVIDE(SUM(-withdrawal_usd) OVER(w),SUM(deposit_usd) OVER(w))*100 AS cumulative_withdrawal_deposit_percentage
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
             AND DATE(transaction_time)>='2022-01-01'
       GROUP BY 1, 2
        ) AS daily
  WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
),
mt5_trades AS (
  SELECT date
       , binary_user_id
       , closed_pnl_usd as mt5_pnl_usd
       , SUM(closed_pnl_usd) OVER(w) AS cumulative_mt5_pnl_usd
       , mt5_profit_usd 
       , SUM(mt5_profit_usd) OVER(w) AS cumulative_mt5_profit_usd
       , mt5_loss_usd
       , SUM(mt5_loss_usd) OVER(w) AS cumulative_mt5_loss_usd 
       , mt5_win_count 
       , SUM(mt5_win_count) OVER(w) AS cumulative_mt5_win_count 
       , number_of_trades as mt5_contract_count
       , SUM(number_of_trades) OVER(w) AS cumulative_mt5_contract_count
    FROM (
        SElECT date
             , binary_user_id
             , SUM(-closed_pnl_usd) AS closed_pnl_usd
             , SUM(IF(closed_pnl_usd<0
                  , 1
                  , 0)) AS mt5_win_count
             , SUM(IF(closed_pnl_usd<0
                  , -closed_pnl_usd
                  , 0)) AS mt5_profit_usd
             , SUM(IF(closed_pnl_usd>0
                  , -closed_pnl_usd
                  , 0)) AS mt5_loss_usd
             , SUM(number_of_trades) AS number_of_trades
          FROM bi.trades
         WHERE platform = 'MT5' AND date >= '2022-01-01'
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
     , bo_trades.bo_profit_usd
     , bo_trades.cumulative_bo_profit_usd
     , bo_trades.bo_loss_usd
     , bo_trades.cumulative_bo_loss_usd
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
     , payments.withdrawal_deposit_percentage
     , payments.cumulative_withdrawal_deposit_percentage
     , mt5_trades.mt5_pnl_usd
     , mt5_trades.cumulative_mt5_pnl_usd
     , mt5_trades.mt5_profit_usd
     , mt5_trades.cumulative_mt5_profit_usd
     , mt5_trades.mt5_loss_usd
     , mt5_trades.cumulative_mt5_loss_usd
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
 ORDER BY binary_user_id, date
