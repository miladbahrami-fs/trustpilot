# trustpilot
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
        'bo_turnover_usd'                        : Binary/Deriv Turnover in USD
      , 'bo_winning_turnover_usd'                : Binary/Deriv Turnover of Wins in USD
      , 'bo_pnl_usd'                             : Binary/Deriv Profit/Loss in USD
      , 'bo_profit_usd'                          : Binary/Deriv Profit
      , 'bo_win_count'                           : Binary/Deriv Number of Wins
      , 'bo_profit_percentage'                   : Binary/Deriv Profit Percentage (Profit/Turnover)
      , 'bo_contract_count'                      : Binary/Deriv Total Number of contracts 
      , 'deposit_usd'                            : Total Deposits in USD
      , 'withdrawal_usd'                         : Total Withdrawals in USD
      , 'deposit_count'                          : Total Number of Deposits
      , 'withdrawal_count'                       : Total Numver of Withdrawalas
      , 'withdrawal_deposit_percentage'          : Withdrawal Deposit Percentage (Withdrawal/Deposit)
      , 'mt5_pnl_usd'                            : MT5 Profit/Loss in USD
      , 'mt5_profit_usd'                         : MT5 Profit in USD
      , 'mt5_win_count'                          : MT5 Number of Wins
      , 'mt5_contract_count'                     : MT5 total Number of contracts
      , 'pnl_usd'                                : Total Profit/Loss in Binary/Deriv and MT5 
      , 'contract_count'                         : Total Number of contracts in Binary/Deriv and MT5
      , 'auto'                                   : A Custom Metric prepared by BI Team