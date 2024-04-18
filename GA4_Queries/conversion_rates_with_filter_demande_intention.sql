WITH EntryPages AS(
  SELECT 
    session_user,
    entry_time,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')  AS entry_page,
  FROM 
    (
      SELECT 
        CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) as session_user,
        event_timestamp AS entry_time,
        event_params,
        ROW_NUMBER() OVER(PARTITION BY CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) ORDER BY event_timestamp) as rn
      FROM 
        ``
      WHERE
        event_name = 'page_view'
    ) t
  WHERE
    rn = 1
    AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'demande_intention') IN ('acquisition_terminal', 'acquisition_internet_fibre')
),
Conversions AS (
  SELECT
    CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) as session_user,
    event_timestamp AS conversion_time
  FROM 
    ``
  WHERE
    ecommerce.transaction_id is not NULL
    AND event_name = "purchase"
    AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'demande_intention') IN ('acquisition_terminal', 'acquisition_internet_fibre')
  GROUP BY
   session_user, conversion_time
),

ConversionRates AS (
  SELECT
    EP.entry_page,
    COUNT(DISTINCT C.session_user) AS num_conversions,
    COUNT(DISTINCT EP.session_user) AS num_sessions
  FROM
    EntryPages EP
  LEFT JOIN
    Conversions C ON EP.session_user = C.session_user AND C.conversion_time >= EP.entry_time
  GROUP BY
    EP.entry_page
)
SELECT
    entry_page,
    num_sessions,
    num_conversions,
    IF(num_sessions = 0, 0, ROUND((num_conversions / num_sessions) * 100, 2)) AS conversion_rate
FROM
    ConversionRates
ORDER BY
    num_sessions DESC