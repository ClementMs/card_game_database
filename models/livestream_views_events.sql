WITH previous_event_table AS (


SELECT 

user_id,

message_type,

game_id,

timestamp_utc,

message_type_id,

LAG(timestamp_utc) OVER (PARTITION BY user_id, game_id ORDER BY timestamp_utc) AS prev_timestamp,

LAG(message_type_id) OVER (PARTITION BY user_id, game_id ORDER BY timestamp_utc) AS prev_message_type_id_event

// LAG(message_type) OVER (PARTITION BY user_id, game_id ORDER BY timestamp_utc) AS prev_message_type_event

FROM cards_analytics.dev.card_game_events



ORDER BY 1,4


),

time_elapsed_between_viewer_interactions_table AS (


SELECT

    a.*,

    TIMESTAMPDIFF(SECOND, prev_timestamp, timestamp_utc) AS time_elapsed_between_events,

     SUM(CASE WHEN a.prev_message_type_id_event IN (2,3,4,5) THEN 1 ELSE 0 END) OVER (PARTITION BY user_id, game_id ORDER BY timestamp_utc,message_type_id) AS card_round,

     CASE WHEN a.prev_message_type_id_event IN (2,3,4,5) THEN True ELSE FALSE END AS card_round_condition

FROM previous_event_table a


),

livestream_video_view_duration_first_table AS (

SELECT


    *,

    LAG(card_round) OVER (PARTITION BY user_id, game_id ORDER BY timestamp_utc) AS prev_card_round



FROM time_elapsed_between_viewer_interactions_table 


),

SELECT

*

FROM livestream_video_view_duration_second_table


ORDER BY user_id, timestamp_utc, message_type_id

;
