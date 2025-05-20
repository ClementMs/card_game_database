
{{config( materialized='table' )}}

WITH premiere_vue_visionnage_publicites AS (

SELECT


    a.season_id AS id_saison,

    a.game_id AS id_jeu,

    a.user_id AS id_spectateur,

    a.timestamp_utc AS horodatage_utc,

    a.message_type_id AS id_type_message,

    b.timestamp_utc AS horodatage_suivant_utc

FROM dev.card_game_events a

LEFT JOIN dev.card_game_events b ON b.season_id = a.season_id AND b.game_id = a.game_id AND b.user_id = a.user_id AND a.timestamp_utc < b.timestamp_utc

ORDER BY 1,2,3,4,5


),

deuxieme_vue_visionnage_publicites AS (


SELECT

    a.id_saison,

    a.id_jeu,

    a.id_spectateur,

    a.horodatage_utc,

    a.id_type_message,

    a.horodatage_suivant_utc,

    TIMESTAMPDIFF(SECONDS, a.horodatage_utc, a.horodatage_suivant_utc) AS intervalle_temps_evenements


FROM premiere_vue_visionnage_publicites a



),

troisieme_vue_visionnage_publicites AS (

SELECT

    a.id_saison,

    a.id_jeu,

    a.id_spectateur,

    a.horodatage_utc,

    a.id_type_message,
    
    MIN(intervalle_temps_evenements) AS intervalle_temps

FROM deuxieme_vue_visionnage_publicites a

GROUP BY 1,2,3,4,5

),

vue_minimum_horodatage AS (


SELECT

    season_id AS id_saison,

    game_id AS id_jeu,

    user_id AS id_spectateur,

    MIN(timestamp_utc) AS horodatage_utc,

    NULL AS intervalle_temps,

    NULL AS horodatage_suivant_utc

    

FROM dev.card_game_events

GROUP BY 1,2,3

),

quatrieme_vue_visionnage_publicites AS (

SELECT

DISTINCT

    id_saison,
    id_jeu,
    id_spectateur,
    horodatage_utc,
    intervalle_temps,
    DATEADD(SECOND, intervalle_temps, horodatage_utc) AS horodatage_suivant_utc

FROM troisieme_vue_visionnage_publicites

WHERE intervalle_temps / 60 >= 30


UNION 

SELECT

    id_saison,
    id_jeu,
    id_spectateur,
    horodatage_utc,
    intervalle_temps,
    horodatage_suivant_utc

FROM vue_minimum_horodatage

ORDER BY 1,2,3,4

),

cinquieme_vue_visionnage_publicites AS (

SELECT

    a.id_saison,
    a.id_jeu,
    a.id_spectateur,
    a.horodatage_utc AS debut_session,
    a.horodatage_suivant_utc,
    a.intervalle_temps AS intervalle_temps_fin_session_precedente_debut_session_en_cours,
    b.horodatage_utc AS fin_session,
    b.horodatage_suivant_utc AS  horodatage_debut_session_prochaine,
    b.intervalle_temps AS intervalle_temps_fin_session_debut_session_prochaine,
    TIMESTAMPDIFF(SECONDS, a.horodatage_utc, b.horodatage_utc) AS intervalle_temps_bis
    

FROM quatrieme_vue_visionnage_publicites a

LEFT JOIN quatrieme_vue_visionnage_publicites b ON b.id_saison = a.id_saison AND b.id_jeu = a.id_jeu AND b.id_spectateur = a.id_spectateur AND a.horodatage_utc < b.horodatage_utc

),

sixieme_vue_visionnage_publicites AS (

SELECT

    a.id_saison,
    a.id_jeu,
    a.id_spectateur,
    a.debut_session,
    a.intervalle_temps_fin_session_precedente_debut_session_en_cours,
    a.horodatage_suivant_utc,
    MIN(a.fin_session) AS fin_session

FROM cinquieme_vue_visionnage_publicites a



GROUP BY 1,2,3,4,5,6

),

septieme_vue_visionnage_publicites AS (


SELECT

    a.id_saison,
    a.id_jeu,
    a.id_spectateur,
    CASE
    WHEN a.fin_session IS NOT NULL AND a.horodatage_suivant_utc IS NOT NULL THEN a.horodatage_suivant_utc
    WHEN a.fin_session IS NULL THEN a.horodatage_suivant_utc
    ELSE a.debut_session
    END AS debut_session,
    a.fin_session,
    a.intervalle_temps_fin_session_precedente_debut_session_en_cours,
    a.horodatage_suivant_utc,
    CASE WHEN a.fin_session IS NULL THEN 1 ELSE 0 END AS donnees_manquantes

FROM sixieme_vue_visionnage_publicites a


ORDER BY 1,2,3,4

),

donnees_manquantes AS (

SELECT

    *

FROM septieme_vue_visionnage_publicites

WHERE donnees_manquantes = 1 

),

donnees_manquantes_jointure_evenements_successifs AS (

SELECT

    a.id_saison,

    a.id_jeu,

    a.id_spectateur,

    a.debut_session,

    a.fin_session,

    a.donnees_manquantes,

    b.timestamp_utc AS evenement_successif_horodatage,

    TIMESTAMPDIFF(SECONDS, a.debut_session, b.timestamp_utc) / 60 AS intervalle_temps_entre_debut_visionnage_evenements_ulterieurs

FROM donnees_manquantes a

LEFT JOIN dev.card_game_events b ON b.season_id = a.id_saison AND b.game_id = a.id_jeu AND b.user_id = a.id_spectateur AND a.debut_session < b.timestamp_utc

),

evenements_successifs_donnees_manquantes AS (

SELECT


    a.*,

    b.timestamp_utc AS evenement_successif_horodatage_bis,



FROM donnees_manquantes_jointure_evenements_successifs a

LEFT JOIN dev.card_game_events b ON b.season_id = a.id_saison AND b.game_id = a.id_jeu AND b.user_id = a.id_spectateur AND a.evenement_successif_horodatage < b.timestamp_utc

WHERE TIMESTAMPDIFF(SECONDS, evenement_successif_horodatage, b.timestamp_utc) / 60 > 30


),

premiere_vue_donnees_manquantes_completees AS (

SELECT

    a.id_saison,

    a.id_jeu,

    a.id_spectateur,

    a.debut_session AS debut_visionnage_publicite_video_direct,

    CASE
    WHEN MAX(a.evenement_successif_horodatage) IS NULL THEN a.debut_session
    ELSE MAX(a.evenement_successif_horodatage) 
    END AS fin_visionnage_publicite_video_direct,


FROM donnees_manquantes_jointure_evenements_successifs a


GROUP BY 1,2,3,4

ORDER BY 1,2,3,4


),

deuxieme_vue_donnees_manquantes_completees AS (

SELECT

    id_saison,

    id_jeu,

    id_spectateur,

    fin_visionnage_publicite_video_direct,
    
    debut_visionnage_publicite_video_direct,

    TIMESTAMPDIFF(SECONDS, debut_visionnage_publicite_video_direct, fin_visionnage_publicite_video_direct) AS duree_visionnage_publicite_video_direct


FROM premiere_vue_donnees_manquantes_completees

),

duree_visionnage_publicite_video_direct_complet AS (

SELECT 

    id_saison,

    id_jeu,

    id_spectateur,

    debut_visionnage_publicite_video_direct,

    fin_visionnage_publicite_video_direct,

    duree_visionnage_publicite_video_direct


FROM deuxieme_vue_donnees_manquantes_completees


UNION 

SELECT

    id_saison,

    id_jeu,

    id_spectateur,

    debut_session AS debut_visionnage_publicite_video_direct,

    fin_session AS fin_visionnage_publicite_video_direct,

    intervalle_temps_fin_session_precedente_debut_session_en_cours AS duree_visionnage_publicite_video_direct

FROM septieme_vue_visionnage_publicites

WHERE donnees_manquantes = 0


ORDER BY 1,2,3,4

)

SELECT

    id_saison,

    id_jeu,

    id_spectateur,

    debut_visionnage_publicite_video_direct,

    fin_visionnage_publicite_video_direct,

    CASE 
    WHEN duree_visionnage_publicite_video_direct IS NULL THEN TIMESTAMPDIFF(SECOND, debut_visionnage_publicite_video_direct, fin_visionnage_publicite_video_direct) 
    ELSE duree_visionnage_publicite_video_direct
    END AS duree_visionnage_publicite_video_direct


FROM duree_visionnage_publicite_video_direct_complet

ORDER BY 1,2,3,4