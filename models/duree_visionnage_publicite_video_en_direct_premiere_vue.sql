{{ config(materialized="view") }} 

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
-- WHERE a.user_id = '8303228580898163950'

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

    TIMESTAMPDIFF(MINUTES, a.timestamp_utc, a.next_timestamp) AS intervalle_temps_evenements


FROM vue_test a



),

final_table AS (

SELECT

    a.season_id,

    a.game_id,

    a.user_id,

    a.timestamp_utc,

    a.message_type_id,
    
    -- a.next_timestamp,

    MIN(intervalle_temps_evenements) AS intervalle_temps

FROM vue_test_bis a

GROUP BY 1,2,3,4,5

)

SELECT

    season_id,
    game_id,
    user_id,
    timestamp_utc,
    message_type_id,
    message_type_id,
    intervalle_temps

FROM final_table

ORDER BY 1,2,3,4

;



