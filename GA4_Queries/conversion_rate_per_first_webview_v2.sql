WITH hits_to_session AS (
    SELECT
        CONCAT(user_pseudo_id, '-', 
               (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_user,
        event_timestamp,
        event_date,
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
        `events_20240710`
    WHERE
        event_name IN ('debug', 'page_view', 'purchase')
        AND (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "user_logged") IS NOT NULL
),

id_tracking AS (
    SELECT
        session_user,
        ARRAY_AGG(params.id_tracking ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS id_tracking,
        REGEXP_EXTRACT(ARRAY_AGG(params.page_location ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)], r'([^?]*)') AS id_tracking_page
    FROM hits_to_session
    WHERE event_name = 'debug' AND params.is_id_tracking = "true" AND params.id_tracking IS NOT NULL
    GROUP BY session_user
),

demande_intention_data AS (
    SELECT
        session_user,
        ARRAY_AGG(params.demande_intention ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS demande_intention
    FROM hits_to_session
    WHERE params.demande_intention IS NOT NULL AND params.demande_intention != 'nr'
    GROUP BY session_user
),

session_data AS (
    SELECT
        hts.session_user,
        ARRAY_AGG(
            STRUCT(
                hts.event_timestamp, 
                hts.event_date, 
                REGEXP_EXTRACT(hts.params.page_location, r'([^?]*)') AS page_location, 
                hts.params.univers_affichage,
                hts.params.sous_univers, 
                hts.params.type_page
            ) 
        ORDER BY hts.event_timestamp ASC LIMIT 1)[OFFSET(0)] AS first_page_data,
        did.demande_intention,
        MAX(it.id_tracking) AS id_tracking,
        MAX(it.id_tracking_page) AS id_tracking_page,
        MAX(IF(hts.user_logged = 'oui', TRUE, FALSE)) AS is_user_logged,
        MAX(IF(hts.params.type_page LIKE '%configurateur%' OR hts.params.type_page LIKE '%tunnel%', TRUE, FALSE)) AS is_progressed,
        COUNT(IF(hts.event_name = 'page_view', 1, NULL)) AS page_views,
        COUNT(DISTINCT IF(hts.event_name = 'page_view', hts.params.page_location, NULL)) AS unique_page_views,
        MAX(IF(hts.event_name = 'purchase' AND hts.transaction_id IS NOT NULL 
                AND hts.params.univers_affichage IN ('internet', 'mobile')
                AND hts.params.sous_univers IN ('fibre', 'terminal'), TRUE, FALSE)) AS is_converted,
        COUNTIF(hts.event_name = 'page_view') AS pages_before_purchase
    FROM
        hits_to_session as hts
    JOIN
        id_tracking as it
    USING (session_user)
    LEFT JOIN
        demande_intention_data as did
    USING (session_user)
    WHERE
        hts.params.univers_affichage IN ('internet', 'mobile')
        AND hts.params.sous_univers IN ('fibre', 'terminal')
    GROUP BY
        hts.session_user, did.demande_intention
),


FINAL_TABLE AS (
    SELECT
        first_page_data.page_location AS entry_page,
        first_page_data.event_date,
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
        --AND demande_intention IS NOT NULL
    GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8
    HAVING
        COUNT(*) > 15
)

SELECT *
FROM FINAL_TABLE
ORDER BY total_sessions DESC
