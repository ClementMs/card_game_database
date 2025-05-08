{{ config(materialized="view") }} 



WITH phase_precedente_tour_de_cartes AS (

SELECT

    season_id,

    game_id,

    user_id,

    timestamp_utc,

    message_type_id,

    LAG(timestamp_utc) OVER (PARTITION BY season_id, game_id, user_id ORDER BY timestamp_utc, message_type_id) AS prev_timestamp

FROM dev.card_game_events

ORDER BY 1,2,3,4

),

intervalle_temps_entre_les_phases_tour_de_cartes AS (

SELECT


    a.*,

    TIMESTAMPDIFF(DAYS, prev_timestamp, timestamp_utc) AS time_elapsed_days,

    MOD(TIMESTAMPDIFF(HOURS, prev_timestamp, timestamp_utc), 24) AS time_elapsed_hours,

    MOD(TIMESTAMPDIFF(MINUTES, prev_timestamp, timestamp_utc), 60) AS time_elapsed_minutes,

    MOD(TIMESTAMPDIFF(SECONDS, prev_timestamp, timestamp_utc), 60) AS time_elapsed_seconds,


FROM phase_precedente_tour_de_cartes a


),

journaux_vues_publicites_video_en_direct AS (

SELECT

    a.*,

    SUM(CASE WHEN time_elapsed_days >= 1 OR  time_elapsed_hours >= 1 OR time_elapsed_minutes >= 30  THEN 1 ELSE 0 END) OVER (PARTITION BY season_id, game_id, user_id ORDER BY timestamp_utc, message_type_id) AS id_vue_publicite_video_en_direct

FROM intervalle_temps_entre_les_phases_tour_de_cartes a

),

telemetrie_vues_publicite_video_en_direct AS (


SELECT

    season_id,

    game_id,

    user_id,

    id_vue_publicite_video_en_direct,

    MIN(timestamp_utc) AS detection_debut_vue_publicite_video_en_direct,

    MAX(timestamp_utc) AS detection_fin_vue_publicite_video_en_direct,

FROM journaux_vues_publicites_video_en_direct

GROUP BY 1,2,3,4


)

SELECT

    season_id,

    game_id,

    user_id,

    id_vue_publicite_video_en_direct,

    TIMESTAMPDIFF(MINUTES, detection_debut_vue_publicite_video_en_direct , detection_fin_vue_publicite_video_en_direct) AS duree_visionnage_publicite_video_en_direct_minutes,

    MOD(TIMESTAMPDIFF(SECONDS, detection_debut_vue_publicite_video_en_direct , detection_fin_vue_publicite_video_en_direct), 60) AS duree_visionnage_publicite_video_en_direct_secondes

FROM telemetrie_vues_publicite_video_en_direct

-- FETCH FIRST 100 ROWS ONLY

ORDER BY 1,2,3,4



