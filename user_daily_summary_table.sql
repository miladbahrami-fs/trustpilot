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
                       WHERE sell_txn_date >= DATE_SUB('2022-05-01', INTERVAL 3 MONTH)
                         AND sell_txn_date < '2022-05-01'
                       GROUP BY 1,2
                     ) AS daily
              WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
            ) AS daily_summary
       WHERE date = DATE_SUB ('2022-05-01', INTERVAL 1 DAY)
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
                              AND DATE(transaction_time) >= DATE_SUB('2022-05-01', INTERVAL 3 MONTH)
                              AND DATE(transaction_time) < '2022-05-01'
                            GROUP BY 1, 2
                      ) AS daily
              WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
              ) AS daily_summary
       WHERE date = DATE_SUB ('2022-05-01', INTERVAL 1 DAY)
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
                           , -ROUND(SUM(CASE WHEN mv_mt5_deal_aggregated.entry IN ('out','out_by')
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
                       WHERE DATE(deal_date) >= DATE_SUB('2022-05-01', INTERVAL 3 MONTH)
                         AND DATE(deal_date) < '2022-05-01'
                       GROUP BY 1,2
                      ) AS daily
              WINDOW w AS ( PARTITION BY binary_user_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
              ) AS daily_summary
       WHERE date = DATE_SUB ('2022-05-01', INTERVAL 1 DAY)
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
    ORDER BY binary_user_id, date) 

SELECT * FROM user_daily_summary 
