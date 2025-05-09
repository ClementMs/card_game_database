{{config(materialized='table')}}

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

donnees_manquantes AS (

SELECT

*

FROM cards_analytics.staging.duree_visionnage_publicite_video_direct_incomplet

WHERE donnees_manquantes = 1 

),

donnees_manquantes_jointure_evenements_successifs AS (

SELECT

a.season_id,

a.game_id,

a.user_id,

a.session_start,

a.session_end,

a.donnees_manquantes,

b.timestamp_utc AS evenement_successif_horodatage,

TIMESTAMPDIFF(SECONDS, a.session_start, b.timestamp_utc) / 60 AS intervalle_temps_entre_debut_visionnage_evenements_ulterieurs

FROM donnees_manquantes a

LEFT JOIN dev.card_game_events b ON b.season_id = a.season_id AND b.game_id = a.game_id AND b.user_id = a.user_id AND a.session_start < b.timestamp_utc

WHERE a.user_id = '8303228580898163950'

),

evenements_successifs_donnees_manquantes AS (

SELECT


    a.*,

    b.timestamp_utc AS evenement_successif_horodatage_bis,



FROM donnees_manquantes_jointure_evenements_successifs a

LEFT JOIN dev.card_game_events b ON b.season_id = a.season_id AND b.game_id = a.game_id AND b.user_id = a.user_id AND a.evenement_successif_horodatage < b.timestamp_utc

WHERE a.user_id = '8303228580898163950'

AND TIMESTAMPDIFF(SECONDS, evenement_successif_horodatage, b.timestamp_utc) / 60 > 30


),

donnees_manquantes_completees AS (

SELECT

    a.season_id,

    a.game_id,

    a.user_id,

    a.session_start AS fin_visionnage_publicite_video_direct,

    CASE
    WHEN MAX(a.evenement_successif_horodatage) IS NULL THEN a.session_start
    ELSE MAX(a.evenement_successif_horodatage) 
    END AS debut_visionnage_publicite_video_direct,


FROM donnees_manquantes_jointure_evenements_successifs a


GROUP BY 1,2,3,4

ORDER BY 1,2,3,4


)

SELECT

    season_id,

    game_id,

    user_id,

    fin_visionnage_publicite_video_direct,
    
    debut_visionnage_publicite_video_direct,

    TIMESTAMPDIFF(SECONDS, debut_visionnage_publicite_video_direct, fin_visionnage_publicite_video_direct) AS duree_visionnage_publicite_video_direct


FROM donnees_manquantes_completees