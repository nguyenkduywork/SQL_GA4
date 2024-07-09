DECLARE yesterday_suffix STRING;
SET yesterday_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

INSERT INTO analyse_parcours_clients.analyse_date (
    entry_page,
    event_date,
    univers_affichage,
    sous_univers,
    id_tracking,
    total_sessions,
    LOGGED_SESSIONS,
    PROGRESSED_SESSIONS,
    CONVERTED_SESSIONS,
    LOGGED_CONVERTED_SESSIONS,
    average_pages_per_session,
    min_pages_to_conversion,
    max_pages_to_conversion,
    avg_pages_to_conversion,
    progression_rate,
    conversion_rate
)

WITH hits_to_session AS (
    SELECT
        CONCAT(user_pseudo_id, '-', 
               (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_user,
        event_timestamp,
        event_date,
        event_name,
        (SELECT AS STRUCT
            MAX(IF(key = 'page_location', value.string_value, NULL)) AS page_location,
            MAX(IF(key = 'type_page', value.string_value, NULL)) AS type_page,
            MAX(IF(key = 'demande_intention', value.string_value, NULL)) AS demande_intention,
            MAX(IF(key = 'is_id_tracking', value.string_value, NULL)) AS is_id_tracking,
            MAX(IF(key = 'id_tracking', value.string_value, NULL)) AS id_tracking,
            MAX(IF(key = 'univers_affichage', value.string_value, NULL)) AS univers_affichage,
            MAX(IF(key = 'sous_univers', value.string_value, NULL)) AS sous_univers
         FROM UNNEST(event_params)
        ) AS params,
        (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "user_logged") AS user_logged,
        ecommerce.transaction_id AS transaction_id
    FROM
        `events_*`
    WHERE
        _TABLE_SUFFIX = yesterday_suffix
        AND user_pseudo_id NOT LIKE "00000%"
),
  
-- Extraire les id_tracking dans les évènements "debug"
id_tracking AS (
    SELECT
        session_user,
        ARRAY_AGG(params.id_tracking ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS id_tracking
    FROM hits_to_session
    WHERE event_name = 'debug' AND params.is_id_tracking = "true" AND params.id_tracking IS NOT NULL
    GROUP BY session_user
),
  
-- Extraire les entry pages (page_location dans les évènements "session_start")
entry_pages AS (
    SELECT
        session_user,
        ARRAY_AGG(STRUCT(event_timestamp, event_date, params.page_location) 
                  ORDER BY event_timestamp ASC 
                  LIMIT 1)[OFFSET(0)].*,
        REGEXP_EXTRACT(ARRAY_AGG(params.page_location ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)], r'([^?]*)') AS entry_page
    FROM hits_to_session
    WHERE event_name = 'session_start'
    GROUP BY session_user
),

--Jointure des sous tables de entry_pages, id_tracking avec user_logged et nb de sessions qui sont allées jusqu'au tunnel (type_page configurateur ou tunnel)
entry_tracking_user_logged AS (
    SELECT
        ep.session_user,
        ep.event_timestamp AS entry_time,
        PARSE_DATE('%Y%m%d', ep.event_date) AS event_date,
        ep.entry_page,
        MAX(IF(hits.user_logged = 'oui', TRUE, FALSE)) AS is_user_logged,
        MAX(IF(hits.params.type_page LIKE '%configurateur%' OR hits.params.type_page LIKE '%tunnel%', TRUE, FALSE)) AS is_progressed,
        idt.id_tracking
    FROM
        entry_pages AS ep
    JOIN
        hits_to_session AS hits ON ep.session_user = hits.session_user
    LEFT JOIN
        id_tracking AS idt ON ep.session_user = idt.session_user
    GROUP BY
        ep.session_user, ep.event_timestamp, ep.event_date, ep.entry_page, idt.id_tracking
),
  
-- Extraire les données des évènements "purchase"
conversion AS (
    SELECT
        session_user,
        params.univers_affichage,
        params.sous_univers
    FROM
        hits_to_session
    WHERE
        transaction_id IS NOT NULL
        AND event_name = "purchase"
        AND params.univers_affichage IN ("internet","mobile")
        AND params.sous_univers IN ("fibre","terminal")
),
  
-- Extraire les données des univers_affichage et sous_univers
univers_affichage AS (
    SELECT
        *
    FROM (
        SELECT
            params.univers_affichage,
            params.sous_univers,
            session_user,
            params.demande_intention,
            event_timestamp,
            ROW_NUMBER() OVER (PARTITION BY session_user ORDER BY event_timestamp ASC) as row_number
        FROM
            hits_to_session
        WHERE
          params.univers_affichage IN ("internet","mobile")
          AND params.sous_univers IN ("fibre","terminal")
    )
    WHERE
        row_number = 1
),
 
-- Compter le nombre de pages vues par session
pages_per_session AS (
    SELECT
        session_user,
        COUNT(params.page_location) AS page_views
    FROM
        hits_to_session
    WHERE
        event_name = 'page_view'
    GROUP BY
        session_user
),
  
-- Combiner les CTEs afin d'avoir deux grands CTEs: total_sessions et sessions_with_conversion
sessions_info AS (
    SELECT
        e.session_user,
        e.entry_time,
        e.event_date,
        e.entry_page,
        e.id_tracking,
        e.is_user_logged,
        e.is_progressed,
        ua.univers_affichage,
        ua.sous_univers,
        pps.page_views
    FROM
        entry_tracking_user_logged AS e
    INNER JOIN
        univers_affichage AS ua
    ON
        e.session_user = ua.session_user
    LEFT JOIN  -- Use LEFT JOIN to keep all sessions, even those without page views
        pages_per_session AS pps
    ON
        e.session_user = pps.session_user
),
  
converted_sessions_info AS (
    SELECT
        e.session_user,
        e.entry_page,
        e.id_tracking,
        e.is_user_logged,
        ua.univers_affichage,
        ua.sous_univers,
        pps.page_views
    FROM
        entry_tracking_user_logged AS e
    INNER JOIN
        univers_affichage AS ua
    ON
        e.session_user = ua.session_user
    LEFT JOIN
        pages_per_session AS pps
    ON
        e.session_user = pps.session_user
    WHERE
        e.session_user IN (SELECT session_user FROM conversion)
),
  
total_sessions AS (
    SELECT
        entry_page,
        event_date,
        univers_affichage,
        sous_univers,
        id_tracking,
        COUNT(DISTINCT session_user) AS total_sessions,
        COUNT(DISTINCT IF(is_user_logged = TRUE, session_user, NULL)) AS LOGGED_SESSIONS,
        COUNT(DISTINCT IF(is_user_logged = FALSE, session_user, NULL)) AS UNLOGGED_SESSIONS,
        COUNT(DISTINCT IF(is_progressed = TRUE, session_user, NULL)) AS PROGRESSED_SESSIONS,
        ROUND(AVG(page_views),2) AS average_pages_per_session
    FROM
        sessions_info
    GROUP BY
        entry_page, event_date, univers_affichage, sous_univers, id_tracking
),
  
sessions_with_conversion AS (
    SELECT
        entry_page,
        id_tracking,
        univers_affichage,
        sous_univers,
        COUNT(DISTINCT session_user) AS total_sessions_with_conversion,
        COUNT(DISTINCT IF(is_user_logged = TRUE, session_user, NULL)) AS logged_in_users_with_conversion,
        COUNT(DISTINCT IF(is_user_logged = FALSE, session_user, NULL)) AS not_logged_in_users_with_conversion,
        MIN(page_views) AS min_pages_to_conversion,
        MAX(page_views) AS max_pages_to_conversion,
        ROUND(AVG(page_views),2) AS avg_pages_to_conversion
    FROM
        converted_sessions_info
    GROUP BY
        entry_page, univers_affichage, sous_univers, id_tracking
),
  
-- Table finale
final_table AS (
    SELECT
        ts.entry_page,
        ts.event_date,
        ts.univers_affichage,
        ts.sous_univers,
        ts.id_tracking,
        ts.total_sessions,
        ts.LOGGED_SESSIONS,
        ts.PROGRESSED_SESSIONS,
        swc.total_sessions_with_conversion AS CONVERTED_SESSIONS,
        swc.logged_in_users_with_conversion AS LOGGED_CONVERTED_SESSIONS,
        ts.average_pages_per_session,
        swc.min_pages_to_conversion,
        swc.max_pages_to_conversion,
        swc.avg_pages_to_conversion,
        ROUND((ts.PROGRESSED_SESSIONS * 100.0 / ts.total_sessions), 1) AS progression_rate,
        ROUND((swc.total_sessions_with_conversion * 100.0 / ts.total_sessions), 1) AS conversion_rate
    FROM
        total_sessions ts
    INNER JOIN
        sessions_with_conversion swc ON ts.entry_page = swc.entry_page AND ts.id_tracking = swc.id_tracking AND ts.univers_affichage = swc.univers_affichage AND ts.sous_univers = swc.sous_univers
)
 
SELECT * FROM final_table WHERE total_sessions > 20
