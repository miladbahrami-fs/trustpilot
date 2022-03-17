CREATE OR REPLACE PROCEDURE development.trustpilot_invitation_func(
  start_date STRING -- start date
, end_date STRING -- end date
, invitation_limit INT64 -- number of invitations
, metric STRING -- Available Metrics: 
, percentage_threshold INT64 -- if percentage of active users in a country was below this threshod, the whole users of that country will be considered  
, threshold FLOAT64 -- metric threshold
, first_time_meet BOOL 
) 
BEGIN
DECLARE _query_string STRING;
SET _query_string="""
WITH active_users AS(
  SELECT summary.*
       , cumulative_"""||metric||""" AS metric_value
       , '"""||metric||"""' AS metric
       , CASE
             WHEN """||first_time_meet||"""= TRUE 
                THEN (cumulative_"""||metric||""" >= """||threshold||""" AND cumulative_"""||metric||""" - """||metric||""" < """||threshold||""") 
             WHEN """||first_time_meet||"""= FALSE
                THEN cumulative_"""||metric||""" >= """||threshold||""" 
             ELSE FALSE
          END AS meet
      , up.residence AS country
      , up.email
      , up.loginid_list
    FROM development.user_daily_summary summary
    JOIN bi.user_profile up ON up.binary_user_id = summary.binary_user_id
   WHERE date >= '"""||start_date||"""' AND date < '"""||end_date||"""'
   )
,top_country AS (
  SELECT country
       , count_user
       , percentage
       , CASE
             WHEN count_user <= """||invitation_limit||""" THEN count_user
             WHEN count_user > """||invitation_limit||""" AND status='below' THEN count_user
             WHEN count_user > """||invitation_limit||""" AND status='above' THEN ROUND(GREATEST(200 - count_user_below,0)/(count_country_above)) 
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
ORDER BY metric_value """;
EXECUTE IMMEDIATE (_query_string);
END;
