{{ config(materialized="view") }} 

WITH evenements_publicites_video_direct AS (

SELECT

    a.season_id AS id_saison,

    a.game_id AS id_jeu_video,

    a.user_id AS id_spectateur,

    a.timestamp_utc AS horodatage,

    a.message_type_id AS action_spectateur,

    b.timestamp_utc AS horodatage_ulterieur

FROM dev.card_game_events a

LEFT JOIN dev.card_game_events b ON b.season_id = a.season_id AND b.game_id = a.game_id AND b.user_id = a.user_id AND a.timestamp_utc < b.timestamp_utc

 WHERE a.user_id = '8303228580898163950'

ORDER BY 1,2,3,4,5


),

intervalles_evenements_publicites_video_direct AS (


SELECT

    a.id_saison,

    a.id_jeu_video,

    a.id_spectateur,

    a.horodatage,

    a.action_spectateur,

    a.horodatage_ulterieur,

    TIMESTAMPDIFF(SECONDS, a.horodatage, a.horodatage_ulterieur) AS intervalle_temps_evenements


FROM evenements_publicites_video_direct a



),

intervalles_temps_evenements_consecutifs_publicites_video_direct AS (

SELECT

    a.id_saison,

    a.id_jeu_video,

    a.id_spectateur,

    a.horodatage,

    a.action_spectateur,
    
    MIN(intervalle_temps_evenements) AS intervalle_temps_evenements_consecutifs

FROM intervalles_evenements_publicites_video_direct a

GROUP BY 1,2,3,4,5

),

evenements_introductifs_publicites_video_direct AS (


SELECT

    season_id AS id_saison,

    game_id AS id_jeu_video,

    user_id AS id_spectateur,

    MIN(timestamp_utc) AS horodatage,

    NULL AS intervalle_temps_evenements_consecutifs,

    NULL AS horodatage_debut_visionnage_publicite_video_consecutive

    

FROM dev.card_game_events

 WHERE user_id = '8303228580898163950'

GROUP BY 1,2,3

),

intervalles_temps_evenements_consecutifs_publicites_video_direct_concatenes AS (

SELECT

DISTINCT

    id_saison,

    id_jeu_video,

    id_spectateur,

    horodatage,

    intervalle_temps_evenements_consecutifs,

    DATEADD(SECOND, intervalle_temps_evenements_consecutifs, horodatage) AS horodatage_debut_visionnage_publicite_video_consecutive

FROM intervalles_temps_evenements_consecutifs_publicites_video_direct

WHERE intervalle_temps_evenements_consecutifs / 60 >= 30


UNION 

SELECT

    id_saison,

    id_jeu_video,

    id_spectateur,

    horodatage,

    intervalle_temps_evenements_consecutifs,

    horodatage_debut_visionnage_publicite_video_consecutive

FROM evenements_introductifs_publicites_video_direct

ORDER BY 1,2,3,4

),

intervalles_temps_evenements_consecutifs_publicites_video_direct_concatenes_pivot AS (

SELECT

    a.id_saison,

    a.id_jeu_video,

    a.id_spectateur,

    a.horodatage AS horodatage_fin_visionnage_publicite_video_anterieure,

    a.horodatage_debut_visionnage_publicite_video_consecutive,

    a.intervalle_temps_evenements_consecutifs AS intervalle_temps_fin_publicite_video_anterieure_debut_publicite_video_contextualisee,

    b.horodatage AS horodatage_fin_visionnage_publicite_video,

    b.horodatage_debut_visionnage_publicite_video_consecutive AS horodatage_debut_visionnage_publicite_video,
 --   b.intervalle_temps_evenements_consecutifs AS intervalle_temps_session_end_next_session_start,
   -- TIMESTAMPDIFF(SECONDS, a.horodatage, b.horodatage) AS due_visionnage_publicite_video
    

FROM intervalles_temps_evenements_consecutifs_publicites_video_direct_concatenes a

LEFT JOIN intervalles_temps_evenements_consecutifs_publicites_video_direct_concatenes b ON b.id_saison = a.id_saison AND b.id_jeu_video = a.id_jeu_video AND b.id_spectateur = a.id_spectateur AND a.horodatage < b.horodatage

),

duree_visionnage_publicite_video_direct AS (

SELECT

    a.id_saison,

    a.id_jeu_video,

    a.id_spectateur,

    a.horodatage_fin_visionnage_publicite_video_anterieure,

    a.intervalle_temps_fin_publicite_video_anterieure_debut_publicite_video_contextualisee,

    a.horodatage_debut_visionnage_publicite_video,

    MIN(a.horodatage_fin_visionnage_publicite_video) AS horodatage_fin_visionnage_publicite_video

FROM intervalles_temps_evenements_consecutifs_publicites_video_direct_concatenes_pivot a


GROUP BY 1,2,3,4,5,6

)

SELECT

    a.id_saison,

    a.id_jeu_video,

    a.id_spectateur,

    a.horodatage_debut_visionnage_publicite_video,

    a.horodatage_fin_visionnage_publicite_video,

    a.intervalle_temps_fin_publicite_video_anterieure_debut_publicite_video_contextualisee,

    a.horodatage_fin_visionnage_publicite_video_anterieure,

    TIMESTAMPDIFF(SECOND, a.horodatage_debut_visionnage_publicite_video, a.horodatage_fin_visionnage_publicite_video) / 60 AS duree_visionnage_publicite_video_direct

FROM duree_visionnage_publicite_video_direct a


ORDER BY 1,2,3,4



