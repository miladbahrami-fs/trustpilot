     SELECT ROUND(APPROX_QUANTILES(cumulative_bo_turnover_usd, 100)[OFFSET(80)]) AS cumulative_bo_turnover_usd_threshold
          , ROUND(APPROX_QUANTILES(cumulative_bo_winning_turnover_usd, 100)[OFFSET(80)]) AS cumulative_bo_winning_turnover_usd_threshold
          , ROUND(APPROX_QUANTILES(cumulative_bo_win_count, 100)[OFFSET(90)]) AS cumulative_bo_win_count_threshold
          , ROUND(APPROX_QUANTILES(cumulative_bo_profit_percentage, 100)[OFFSET(80)]) AS cumulative_bo_profit_percentage_threshold
          , ROUND(APPROX_QUANTILES(cumulative_bo_contract_count, 100)[OFFSET(80)]) AS cumulative_bo_contract_count_threshold
          , ROUND(APPROX_QUANTILES(cumulative_withdrawal_usd, 100)[OFFSET(80)]) AS cumulative_withdrawal_usd_threshold
          , ROUND(APPROX_QUANTILES(cumulative_deposit_count, 100)[OFFSET(80)]) AS cumulative_deposit_count_threshold
          , ROUND(APPROX_QUANTILES(cumulative_deposit_usd, 100)[OFFSET(80)]) AS cumulative_deposit_usd_threshold
          , ROUND(APPROX_QUANTILES(cumulative_withdrawal_count, 100)[OFFSET(80)]) AS cumulative_withdrawal_count_threshold
          , ROUND(APPROX_QUANTILES(cumulative_bo_pnl_usd, 100)[OFFSET(80)]) AS cumulative_bo_pnl_usd_threshold
          , ROUND(APPROX_QUANTILES(cumulative_pnl_usd, 100)[OFFSET(80)]) AS cumulative_pnl_usd_threshold
          , ROUND(APPROX_QUANTILES(cumulative_contract_count, 100)[OFFSET(80)]) AS cumulative_contract_count_threshold
       FROM `business-intelligence-240201.development.user_daily_summary_5_1` 