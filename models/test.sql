{{config(materialized='view')}}


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

),

resultat AS (

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
    a.next_timestamp
  --  TIMESTAMPDIFF(SECOND, a.next_timestamp, a.session_end) / 60 AS session_duration

FROM third_events_table a

-- FROM events_table a

ORDER BY 1,2,3,4

),

resultat_sans_ligne_manquante AS (

SELECT

    season_id,

    game_id,

    user_id,

    session_start,

    session_end,

    intervalle_temps_previous_session_end_current_session_start,

    next_timestamp,

    TIMESTAMPDIFF(SECOND, session_start, session_end) / 60 AS session_duration



FROM resultat

WHERE session_end IS NOT NULL


),

lignes_manquantes AS (

SELECT

    season_id,

    game_id,

    user_id,

    session_start,

    session_end,

    intervalle_temps_previous_session_end_current_session_start,

    next_timestamp,

    TIMESTAMPDIFF(SECOND, session_start, session_end) / 60 AS session_duration



FROM resultat

WHERE session_end IS  NULL


),


prev_final_table AS (

SELECT

    a.season_id,

    a.game_id,

    a.user_id,

    a.timestamp_utc,

    a.message_type_id,
    
    a.next_timestamp,

    MIN(intervalle_temps_evenements) AS intervalle_temps

FROM vue_test_bis a

GROUP BY 1,2,3,4,5,6


),

lignes_manquantes_join AS (

SELECT

    a.season_id,

    a.game_id,

    a.user_id,

    a.session_start,

  --  a.session_end,

    b.next_timestamp AS session_end,

    b.intervalle_temps AS session_duration,

    a.intervalle_temps_previous_session_end_current_session_start



FROM lignes_manquantes a


-- LEFT JOIN events_table b ON b.season_id = a.season_id AND b.game_id = a.game_id AND b.user_id = a.user_id AND b.timestamp_utc > a.session_start

LEFT JOIN prev_final_table b ON b.season_id = a.season_id AND b.game_id = a.game_id AND b.user_id = a.user_id AND b.timestamp_utc > a.session_start

),

lignes_manquantes_nouvelle_vue_un AS (


SELECT

    a.season_id,

    a.game_id,

    a.user_id,

    a.session_start,

    a.session_end,

    a.intervalle_temps_previous_session_end_current_session_start,

 --   a.next_timestamp,

    a.session_duration,

  --  a.timestamp_utc,

  --  a.intervalle_temps,

 --   a.prev_next_timestamp,

    TIMESTAMPDIFF(SECONDS, a.session_end, b.timestamp_utc) / 60 AS intervalle_fin_session_evenement_successif


FROM lignes_manquantes_join a

LEFT JOIN dev.card_game_events b ON b.season_id = a.season_id AND b.game_id = a.game_id AND b.user_id = a.user_id AND b.timestamp_utc > a.session_end

-- WHERE session_start < DATE('2023-12-05')

WHERE TIMESTAMPDIFF(SECONDS, a.session_end, b.timestamp_utc) / 60 > 30

ORDER BY a.session_start, a.session_end



),

lignes_manquantes_nouvelle_vue_deux AS (

SELECT

    a.season_id,

    a.game_id,

    a.user_id,

    a.session_start,

--    a.session_end,

 --   a.intervalle_temps_previous_session_end_current_session_start,

 --   a.next_timestamp,

 --   a.session_duration,

 --   a.timestamp_utc,

  --  a.intervalle_temps,

 --   a.prev_next_timestamp,

    MIN(a.session_end) AS fin_session

FROM lignes_manquantes_nouvelle_vue_un a

GROUP BY 1,2,3,4

)

SELECT

*

FROM lignes_manquantes_nouvelle_vue_deux

