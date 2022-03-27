DECLARE start_date STRING DEFAULT '2022-02-01';
DECLARE end_date STRING DEFAULT '2022-02-02';
DECLARE invitation_limit INT64 DEFAULT 300;
DECLARE interval_days INT64 DEFAULT 90;
DECLARE metric STRING DEFAULT 'bo_turnover_usd';
DECLARE percentage_threshold INT64 DEFAULT 5;
DECLARE threshold FLOAT64 DEFAULT 1000;
DECLARE first_time_meet BOOL DEFAULT TRUE; 
CALL `business-intelligence-240201.development.trustpilot_invitation_func`(start_date, end_date, invitation_limit, interval_days, metric, percentage_threshold, threshold, first_time_meet);