{{ config(
    materialized='view'
) }}



WITH first_table AS (

SELECT

    user_id,
    game_id,
    card_round,
    MIN(timestamp_utc) AS card_round_start,
    MAX(timestamp_utc) AS card_round_end,
    

FROM {{ ref('livestream_views_events') }} a



GROUP BY 1,2,3


),

second_table AS (

SELECT

    user_id,
    game_id,
    TIMESTAMPDIFF(SECOND, card_round_start, card_round_end) AS card_round_duration

FROM first_table




)

SELECT

    a.game_id,

    b.name AS game,

    COUNT(a.*) AS nb_card_rounds,

    SUM(a.card_round_duration) / (COUNT(a.*) * 60) AS avg_card_round_duration_minutes



FROM second_table a

INNER JOIN cards_analytics.dev.games b ON b.game_id = a.game_id

GROUP BY 1,2

ORDER BY 1
