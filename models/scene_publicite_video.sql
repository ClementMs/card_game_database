{{config(materialized='table')}}

WITH premiere_vue_scene_publicite_video AS (

SELECT

    a.user_id AS id_utilisateur,

    a.game_id AS id_jeu,

    a.season_id AS id_saison,

    a.event_id AS id_evenement,

    b.event_id AS id_evenement_successif,

    a.timestamp_utc AS horodatage_utc,

    b.timestamp_utc AS horodatage_utc_evenement_successif,

    a.message_type_id AS id_type_evenement,

    b.message_type_id AS id_type_evenement_successif,

    TIMESTAMPDIFF(SECOND, a.timestamp_utc, b.timestamp_utc) AS intervalle_temps_evenements_secondes


FROM dev.card_game_events a

LEFT JOIN dev.card_game_events b ON b.user_id = a.user_id AND b.season_id = a.season_id AND b.game_id = a.game_id AND b.timestamp_utc >= a.timestamp_utc AND b.message_type_id > a.message_type_id


),

deuxieme_vue_scene_publicite_video AS (

SELECT

    id_utilisateur,

    id_jeu,

    id_saison,

    id_evenement,

    horodatage_utc,
    
    id_type_evenement,

    MIN(id_evenement_successif) AS id_evenement_successif_fin_scene_publicite_video

FROM premiere_vue_scene_publicite_video

WHERE id_type_evenement_successif IN (3,4, 5,6,2)

AND id_type_evenement = 0

GROUP BY 1,2,3,4,5,6


),

troisieme_vue_scene_publicite_video AS (

SELECT


    id_utilisateur,

    id_jeu,

    id_saison,

    id_evenement,

    horodatage_utc,
    
    id_type_evenement,

    id_evenement_successif_fin_scene_publicite_video,

    MIN(id_evenement) AS id_evenement_debut_scene_publicite_video


FROM deuxieme_vue_scene_publicite_video

GROUP BY 1,2,3,4,5,6,7



),

quatrieme_vue_scene_publicite_video AS (

SELECT 


     id_evenement_successif_fin_scene_publicite_video,

     MIN(id_evenement_debut_scene_publicite_video) AS id_evenement_debut_scene_publicite_video


FROM troisieme_vue_scene_publicite_video 


GROUP BY 1

ORDER BY 1,2

),

cinquieme_vue_scene_publicite_video AS (

SELECT

     c.user_id AS id_spectateur,

     b.game_id AS id_jeu,

     b.season_id AS id_saison,

     a.id_evenement_debut_scene_publicite_video,

     a.id_evenement_successif_fin_scene_publicite_video,

     b.timestamp_utc AS horodatage_fin_scene_publicite_video,

     c.timestamp_utc AS horodatage_debut_scene_publicite_video,

     b.message_type AS resultat_prediction_spectateur_evenement_concluant_scene_publicite_video,

     c.HAND_SHUFFLED_CARD_IDS AS premiere_main_cartes_classiques,

     c.MATCH_END_CARD_IDS AS premiere_main_cartes_speciales,

     TIMESTAMPDIFF(SECOND, horodatage_debut_scene_publicite_video, horodatage_fin_scene_publicite_video) AS duree_scene_publicite_video

FROM quatrieme_vue_scene_publicite_video a 


INNER JOIN dev.card_game_events b ON b.event_id = a.id_evenement_successif_fin_scene_publicite_video

INNER JOIN dev.card_game_events c ON c.event_id = a.id_evenement_debut_scene_publicite_video

),

evenement_predit_premiere_vue AS (

SELECT

     a.id_evenement_debut_scene_publicite_video,

     b.selected_card_id AS evenement_predit_id


FROM cinquieme_vue_scene_publicite_video a

LEFT JOIN dev.card_game_events b ON b.user_id = a.id_spectateur AND b.season_id = a.id_saison AND b.game_id = a.id_jeu AND b.timestamp_utc >= a.horodatage_debut_scene_publicite_video AND b.timestamp_utc <= a.horodatage_fin_scene_publicite_video AND b.message_type_id = 1


),

predictions_obtenues_premiere_vue AS (

SELECT

     a.id_evenement_debut_scene_publicite_video,

     b.event_id AS id_evenement_predictions_choisies,

     b.hand_shuffled_card_ids AS predictions_choisies_classiques,

     b.match_end_card_ids AS predictions_choisies_speciales


FROM cinquieme_vue_scene_publicite_video a

LEFT JOIN dev.card_game_events b ON b.user_id = a.id_spectateur AND b.season_id = a.id_saison AND b.game_id = a.id_jeu AND b.timestamp_utc >= a.horodatage_debut_scene_publicite_video AND b.timestamp_utc <= a.horodatage_fin_scene_publicite_video AND b.message_type_id = 0

AND a.id_evenement_debut_scene_publicite_video != b.event_id


),

predictions_obtenues_deuxieme_vue AS (

SELECT

    id_evenement_debut_scene_publicite_video,

    MAX(id_evenement_predictions_choisies) AS id_evenement_predictions_choisies,

    COUNT(id_evenement_predictions_choisies) AS nombre_changements_predictions


FROM predictions_obtenues_premiere_vue

GROUP BY 1

),

predictions_obtenues_troisieme_vue AS (

SELECT

    a.id_evenement_debut_scene_publicite_video,

    a.id_evenement_predictions_choisies,

    a.nombre_changements_predictions,

    b.hand_shuffled_card_ids,

    b.match_end_card_ids

FROM predictions_obtenues_deuxieme_vue a

LEFT JOIN dev.card_game_events b ON b.event_id = a.id_evenement_predictions_choisies

)

SELECT
   
     a.id_spectateur,

     a.id_jeu,

     a.id_saison,

     a.id_evenement_debut_scene_publicite_video,

     a.id_evenement_successif_fin_scene_publicite_video AS id_evenement_fin_scene_publicite_video,

     c.id_evenement_predictions_choisies,

     a.horodatage_debut_scene_publicite_video,

     a.horodatage_fin_scene_publicite_video,

     a.duree_scene_publicite_video,

     c.nombre_changements_predictions,

     a.premiere_main_cartes_classiques AS premieres_predictions_obtenues_classiques,

     a.premiere_main_cartes_speciales AS premieres_predictions_obtenues_speciales,

     CASE 
     WHEN c.nombre_changements_predictions = 0 THEN a.premiere_main_cartes_classiques
     WHEN c.nombre_changements_predictions > 0 THEN c.hand_shuffled_card_ids
     ELSE NULL
     END AS predictions_choisies_classiques,

     CASE  
     WHEN c.nombre_changements_predictions = 0 THEN a.premiere_main_cartes_speciales
     WHEN c.nombre_changements_predictions > 0 THEN c.match_end_card_ids
     ELSE NULL
     END AS predictions_choisies_speciales,

     b.evenement_predit_id AS id_prediction_spectateur,

     a.resultat_prediction_spectateur_evenement_concluant_scene_publicite_video AS resultat_prediction_spectateur


FROM cinquieme_vue_scene_publicite_video a 

INNER JOIN evenement_predit_premiere_vue b ON b.id_evenement_debut_scene_publicite_video = a.id_evenement_debut_scene_publicite_video

INNER JOIN predictions_obtenues_troisieme_vue c ON c.id_evenement_debut_scene_publicite_video = a.id_evenement_debut_scene_publicite_video

