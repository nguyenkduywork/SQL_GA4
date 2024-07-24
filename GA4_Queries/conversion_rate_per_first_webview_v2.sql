DECLARE yesterday_suffix STRING;
SET yesterday_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

INSERT INTO ofr-dlm-datalake-media-prd.analyse_parcours_clients.analyse_date (
    entry_page,
    event_date,
    univers_affichage,
    sous_univers,
    type_page,
    id_tracking,
    id_tracking_page,
    demande_intention,
    total_sessions,
    logged_sessions,
    progressed_sessions,
    converted_sessions,
    logged_converted_sessions,
    average_pages_per_session,
    average_unique_pages_per_session,
    min_pages_to_conversion,
    max_pages_to_conversion,
    avg_pages_to_conversion,
    progression_rate,
    conversion_rate
)

WITH unnest_table AS (
    SELECT
        CONCAT(user_pseudo_id, '-', 
               (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_user,
        event_timestamp,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        event_name,
        (SELECT AS STRUCT
            MAX(IF(key = 'page_location', value.string_value, NULL)) AS page_location,
            MAX(IF(key = 'demande_intention', value.string_value, NULL)) AS demande_intention,
            MAX(IF(key = 'type_page', value.string_value, NULL)) AS type_page,
            MAX(IF(key = 'is_id_tracking', value.string_value, NULL)) AS is_id_tracking,
            MAX(IF(key = 'id_tracking', value.string_value, NULL)) AS id_tracking,
            MAX(IF(key = 'univers_affichage', value.string_value, NULL)) AS univers_affichage,
            MAX(IF(key = 'sous_univers', value.string_value, NULL)) AS sous_univers
         FROM UNNEST(event_params)
        ) AS params,
        (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "user_logged") AS user_logged,
        ecommerce.transaction_id
    FROM
        `events_*` --GA4 table
    WHERE
        _TABLE_SUFFIX BETWEEN yesterday_suffix AND yesterday_suffix
        AND event_name IN ('debug', 'page_view', 'purchase')
),

id_tracking AS (
    SELECT
        session_user,
        event_date,
        ARRAY_AGG(params.id_tracking ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS id_tracking,
        REGEXP_EXTRACT(ARRAY_AGG(params.page_location ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)], r'([^?]*)') AS id_tracking_page
    FROM unnest_table
    WHERE event_name = 'debug' AND params.is_id_tracking = "true" AND params.id_tracking IS NOT NULL
    GROUP BY session_user, event_date
),

non_null_demande_intention_data AS (
    SELECT
        session_user,
        event_date,
        ARRAY_AGG(params.demande_intention ORDER BY event_timestamp ASC)[OFFSET(0)] AS first_demande_intention,
        ARRAY_AGG(
            CASE 
                WHEN params.demande_intention IS NOT NULL AND params.demande_intention != 'nr' 
                THEN params.demande_intention 
            END 
            IGNORE NULLS
            ORDER BY event_timestamp ASC
        )[OFFSET(0)] AS first_valid_demande_intention
    FROM unnest_table
    WHERE params.demande_intention IS NOT NULL
    GROUP BY session_user, event_date
),

session_data AS (
    SELECT
        ut.session_user,
        ut.event_date,
        ARRAY_AGG(
            STRUCT(
                ut.event_timestamp, 
                REGEXP_EXTRACT(ut.params.page_location, r'([^?]*)') AS page_location, 
                ut.params.univers_affichage,
                ut.params.sous_univers, 
                ut.params.type_page
            ) 
        ORDER BY ut.event_timestamp ASC LIMIT 1)[OFFSET(0)] AS first_page_data,
        COALESCE(MAX(did.first_valid_demande_intention), MAX(did.first_demande_intention)) AS demande_intention,
        MAX(it.id_tracking) AS id_tracking,
        MAX(it.id_tracking_page) AS id_tracking_page,
        MAX(IF(ut.user_logged = 'oui', TRUE, FALSE)) AS is_user_logged,
        MAX(IF(ut.params.type_page LIKE '%configurateur%' OR ut.params.type_page LIKE '%tunnel%', TRUE, FALSE)) AS is_progressed,
        COUNT(IF(ut.event_name = 'page_view', 1, NULL)) AS page_views,
        COUNT(DISTINCT IF(ut.event_name = 'page_view', ut.params.page_location, NULL)) AS unique_page_views,
        MAX(IF(ut.event_name = 'purchase' 
                AND ut.transaction_id IS NOT NULL 
                AND ut.params.univers_affichage IN ('internet', 'mobile')
                AND ut.params.sous_univers IN ('fibre', 'terminal'), TRUE, FALSE)) AS is_converted,
        COUNTIF(ut.event_name = 'page_view' AND ut.event_timestamp <= (
            SELECT MIN(event_timestamp) 
            FROM unnest_table
            WHERE session_user = ut.session_user
              AND event_date = ut.event_date
              AND event_name = 'purchase' 
              AND transaction_id IS NOT NULL 
              AND params.univers_affichage IN ('internet', 'mobile')
              AND params.sous_univers IN ('fibre', 'terminal')
        )) AS pages_before_purchase
    FROM
        unnest_table as ut
    JOIN
        id_tracking as it
    USING (session_user, event_date)
    LEFT JOIN
        non_null_demande_intention_data as did
    USING (session_user, event_date)
    WHERE
        ut.params.univers_affichage IN ('internet', 'mobile')
        AND ut.params.sous_univers IN ('fibre', 'terminal')
    GROUP BY
        ut.session_user, ut.event_date
),

FINAL_TABLE AS (
    SELECT
        first_page_data.page_location AS entry_page,
        event_date,
        first_page_data.univers_affichage,
        first_page_data.sous_univers,
        first_page_data.type_page,
        id_tracking,
        id_tracking_page,
        demande_intention,
        COUNT(*) AS total_sessions,
        COUNTIF(is_user_logged) AS logged_sessions,
        COUNTIF(is_progressed) AS progressed_sessions,
        COUNTIF(is_converted) AS converted_sessions,
        COUNTIF(is_user_logged AND is_converted) AS logged_converted_sessions,
        ROUND(AVG(page_views), 2) AS avg_pages_per_session,
        ROUND(AVG(unique_page_views), 2) AS avg_unique_pages_per_session,
        MIN(IF(is_converted, pages_before_purchase, NULL)) AS min_pages_to_conversion,
        MAX(IF(is_converted, pages_before_purchase, NULL)) AS max_pages_to_conversion,
        ROUND(AVG(IF(is_converted, pages_before_purchase, NULL)), 2) AS avg_pages_to_conversion,
        ROUND(COUNTIF(is_progressed) * 100 / COUNT(*), 2) AS progression_rate,
        ROUND(COUNTIF(is_converted) * 100 / COUNT(*), 2) AS conversion_rate
    FROM 
        session_data
    WHERE
        first_page_data.page_location IS NOT NULL
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8
    HAVING
        COUNT(*) >= 10
)

SELECT *
FROM FINAL_TABLE
ORDER BY event_date, total_sessions DESC
