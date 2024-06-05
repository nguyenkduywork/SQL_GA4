WITH hits_to_session AS (
    SELECT
        CONCAT(user_pseudo_id, '-', (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) as session_user,
        event_timestamp,
        event_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')  AS page_location,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'type_page') AS type_page,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'demande_intention') AS demande_intention,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "is_id_tracking") AS is_id_tracking,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "id_tracking") AS id_tracking,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'univers_affichage') AS univers_affichage,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'sous_univers') AS sous_univers,
        (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = "user_logged") AS user_logged,
        ecommerce.transaction_id AS transaction_id
    FROM
        `nom_de_la_table_ga4`
    WHERE user_pseudo_id NOT LIKE "00000%"
),
  
-- Extraire les id_tracking dans les évènements "debug"
id_tracking AS (
    SELECT
        session_user,
        is_id_tracking,
        id_tracking
    FROM (
        SELECT
            session_user,
            is_id_tracking,
            id_tracking,
            ROW_NUMBER() OVER (PARTITION BY session_user ORDER BY event_timestamp ASC) as row_number
        FROM
            hits_to_session
        WHERE
            event_name = 'debug'
            AND is_id_tracking = "true"
            AND id_tracking IS NOT NULL
    ) WHERE row_number = 1
),
  
-- Extraire les entry pages (page_location dans les évènements "session_start")
entry_pages AS (
    SELECT
      session_user,
      event_timestamp AS entry_time,
      REGEXP_EXTRACT(page_location, r'([^?]*)') AS entry_page
    FROM (
        SELECT
            session_user,
            event_timestamp,
            page_location,
            ROW_NUMBER() OVER (PARTITION BY session_user ORDER BY event_timestamp ASC) as row_number
        FROM
            hits_to_session
        WHERE
            event_name = 'session_start'
    ) WHERE row_number = 1
),
  
-- Joindre les CTEs entry_pages, hits_to_session et id_tracking afin d'avoir entry page, user_logged et id_tracking dans un seul CTE
entry_tracking_user_logged AS (
    SELECT
      ep.session_user,
      ep.entry_time,
      ep.entry_page,
      IF(LOGICAL_OR(hits_to_session.user_logged = 'oui'), TRUE, FALSE) AS is_user_logged,
      idt.is_id_tracking,
      idt.id_tracking
    FROM
      entry_pages AS ep
    INNER JOIN
      hits_to_session ON ep.session_user = hits_to_session.session_user
    INNER JOIN
      id_tracking AS idt ON ep.session_user = idt.session_user
    GROUP BY
      ep.session_user, ep.entry_time, ep.entry_page, idt.is_id_tracking, idt.id_tracking
),
  
-- Extraire les données des évènements "purchase"
conversion AS (
    SELECT
        session_user,
        univers_affichage,
        sous_univers
    FROM
        hits_to_session
    WHERE
        transaction_id IS NOT NULL
        AND event_name = "purchase"
        AND univers_affichage IN ("internet","mobile")
        AND sous_univers IN ("fibre","terminal")
),
  
-- Extraire les données des univers_affichage et sous_univers
univers_affichage AS (
    SELECT
        *
    FROM (
        SELECT
            univers_affichage,
            sous_univers,
            session_user,
            demande_intention,
            event_timestamp,
            ROW_NUMBER() OVER (PARTITION BY session_user ORDER BY event_timestamp ASC) as row_number
        FROM
            hits_to_session
        WHERE
          univers_affichage IN ("internet","mobile")
          AND sous_univers IN ("fibre","terminal")
    )
    WHERE
        row_number = 1
),
 
-- Compter le nombre de pages vues par session
pages_per_session AS (
    SELECT
        session_user,
        COUNT(page_location) AS page_views
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
        e.entry_page,
        e.is_id_tracking,
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
        univers_affichage,
        sous_univers,
        id_tracking,
        COUNT(DISTINCT session_user) AS total_sessions,
        COUNT(DISTINCT IF(is_user_logged = TRUE, session_user, NULL)) AS LOGGED_SESSIONS,
        COUNT(DISTINCT IF(is_user_logged = FALSE, session_user, NULL)) AS UNLOGGED_SESSIONS,
        ROUND(AVG(page_views),2) AS average_pages_per_session
    FROM
        sessions_info
    GROUP BY
        entry_page, univers_affichage, sous_univers, id_tracking
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
        ts.univers_affichage,
        ts.sous_univers,
        ts.id_tracking,
        ts.total_sessions,
        ts.LOGGED_SESSIONS,
        swc.total_sessions_with_conversion AS CONVERTED_SESSIONS,
        swc.logged_in_users_with_conversion AS LOGGED_CONVERTED_SESSIONS,
        ts.average_pages_per_session,
        swc.min_pages_to_conversion,
        swc.max_pages_to_conversion,
        swc.avg_pages_to_conversion,
        ROUND((swc.total_sessions_with_conversion * 100.0 / ts.total_sessions), 1) AS conversion_rate
    FROM
        total_sessions ts
    INNER JOIN
        sessions_with_conversion swc ON ts.entry_page = swc.entry_page AND ts.id_tracking = swc.id_tracking AND ts.univers_affichage = swc.univers_affichage AND ts.sous_univers = swc.sous_univers
)
 
SELECT * FROM final_table ORDER BY total_sessions DESC
