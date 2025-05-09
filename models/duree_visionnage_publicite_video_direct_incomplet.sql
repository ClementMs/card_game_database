
{{config( materialized='view' )}}

WITH vue_test AS (

SELECT


    a.season_id,

    a.game_id,

    a.user_id,

    a.timestamp_utc,

    a.message_type_id,

    b.timestamp_utc AS next_timestamp

FROM dev.card_game_events a

LEFT JOIN dev.card_game_events b ON b.season_id = a.season_id AND b.game_id = a.game_id AND b.user_id = a.user_id AND a.timestamp_utc < b.timestamp_utc

WHERE a.user_id = '8303228580898163950'

ORDER BY 1,2,3,4,5


),

vue_test_bis AS (


SELECT

    a.season_id,

    a.game_id,

    a.user_id,

    a.timestamp_utc,

    a.message_type_id,

    a.next_timestamp,

    TIMESTAMPDIFF(SECONDS, a.timestamp_utc, a.next_timestamp) AS intervalle_temps_evenements


FROM vue_test a



),

final_table AS (

SELECT

    a.season_id,

    a.game_id,

    a.user_id,

    a.timestamp_utc,

    a.message_type_id,
    
 --   a.next_timestamp,

    MIN(intervalle_temps_evenements) AS intervalle_temps

FROM vue_test_bis a

GROUP BY 1,2,3,4,5

),

vue_minimum_horodatage AS (


SELECT

    season_id,

    game_id,

    user_id,

    MIN(timestamp_utc) AS timestamp_utc,

    NULL AS intervalle_temps,

    NULL AS next_timestamp

    

FROM dev.card_game_events

WHERE user_id = '8303228580898163950'

GROUP BY 1,2,3

),

events_table AS (

SELECT

DISTINCT

    season_id,
    game_id,
    user_id,
    timestamp_utc,
    intervalle_temps,
    DATEADD(SECOND, intervalle_temps, timestamp_utc) AS next_timestamp

FROM final_table

WHERE intervalle_temps / 60 >= 30


UNION 

SELECT

    season_id,
    game_id,
    user_id,
    timestamp_utc,
    intervalle_temps,
    next_timestamp

FROM vue_minimum_horodatage

ORDER BY 1,2,3,4

),

second_events_table AS (

SELECT

    a.season_id,
    a.game_id,
    a.user_id,
    a.timestamp_utc AS session_start,
    a.next_timestamp,
    a.intervalle_temps AS intervalle_temps_previous_session_end_current_session_start,
    b.timestamp_utc AS session_end,
    b.next_timestamp AS timestamp_session_start_next_session,
    b.intervalle_temps AS intervalle_temps_session_end_next_session_start,
    TIMESTAMPDIFF(SECONDS, a.timestamp_utc, b.timestamp_utc) AS intervalle_temps_bis
    

FROM events_table a

LEFT JOIN events_table b ON b.season_id = a.season_id AND b.game_id = a.game_id AND b.user_id = a.user_id AND a.timestamp_utc < b.timestamp_utc

),

third_events_table AS (

SELECT

    a.season_id,
    a.game_id,
    a.user_id,
    a.session_start,
    a.intervalle_temps_previous_session_end_current_session_start,
    a.next_timestamp,
    MIN(a.session_end) AS session_end

FROM second_events_table a


-- GROUP BY 1,2,3,4,5,6

GROUP BY 1,2,3,4,5,6

)


SELECT

    a.season_id,
    a.game_id,
    a.user_id,
     CASE
    -- quand la durée de session est bien calculée, renommer le champs next time stamp qui doit être la session start
    WHEN a.session_end IS NOT NULL AND a.next_timestamp IS NOT NULL THEN a.next_timestamp
    -- quand session end manquante, utiliser next time stamp comme session start
    WHEN a.session_end IS NULL THEN a.next_timestamp
    ELSE a.session_start
    END AS session_start,
    a.session_end,
    a.intervalle_temps_previous_session_end_current_session_start,
    a.next_timestamp,
    CASE WHEN a.session_id IS NULL THEN 1 ELSE 0 END AS donnees_manquantes
  --  TIMESTAMPDIFF(SECOND, a.next_timestamp, a.session_end) / 60 AS session_duration

FROM third_events_table a

-- FROM events_table a

ORDER BY 1,2,3,4