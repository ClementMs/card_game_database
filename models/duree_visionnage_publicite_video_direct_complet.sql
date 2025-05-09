{{config( materialized='table' )}}

WITH duree_visionnage_publicite_video_direct_complet AS (

SELECT 

    season_id AS id_saison,

    game_id AS id_jeu_video,

    user_id AS id_spectateur,

    debut_visionnage_publicite_video_direct,

    fin_visionnage_publicite_video_direct,

    duree_visionnage_publicite_video_direct


FROM cards_analytics.staging.duree_visionnages_publicite_video_direct_donnees_manquantes


UNION 

SELECT

    season_id AS id_saison,

    game_id AS id_jeu_video,

    user_id AS id_spectateur,

    session_start AS debut_visionnage_publicite_video_direct,

    session_end AS fin_visionnage_publicite_video_direct,

    intervalle_temps_previous_session_end_current_session_start AS duree_visionnage_publicite_video_direct

FROM cards_analytics.staging.duree_visionnage_publicite_video_direct_incomplet

WHERE donnees_manquantes = 0


ORDER BY 1,2,3,4

)

SELECT

    id_saison,

    id_jeu_video,

    id_spectateur,

    debut_visionnage_publicite_video_direct,

    fin_visionnage_publicite_video_direct,

    CASE 
    WHEN duree_visionnage_publicite_video_direct IS NULL THEN TIMESTAMPDIFF(SECOND, debut_visionnage_publicite_video_direct, fin_visionnage_publicite_video_direct) 
    ELSE duree_visionnage_publicite_video_direct
    END AS duree_visionnage_publicite_video_direct


FROM duree_visionnage_publicite_video_direct_complet

ORDER BY 1,2,3,4