USE ROLE ACCOUNTADMIN;

USE WAREHOUSE NOICE;

CREATE OR REPLACE DATABASE cards_analytics;

CREATE OR REPLACE SCHEMA dev;



//CREATE OR REPLACE STAGE cards_analytics.public.blob_stage
//url = 's3://cardgamesnow/'
//CREDENTIALS = (AWS_KEY_ID = '', AWS_SECRET_KEY = '')
//file_format = (type = csv, SKIP_HEADER = 1);


// LIST @cards_analytics.public.blob_stage/;


CREATE OR REPLACE HYBRID TABLE cards_analytics.dev.games (

    game_id SMALLINT NOT NULL PRIMARY KEY,

    name VARCHAR(16)

);

INSERT INTO cards_analytics.dev.games VALUES

(0, 'fortnite'),

(1, 'apex_legends'),

(2, 'dead_by_daylight'),

(3, 'dota2')

;


CREATE OR REPLACE HYBRID TABLE cards_analytics.dev.game_seasons (


    season_id SMALLINT NOT NULL,

    name VARCHAR(13),

    game_id SMALLINT FOREIGN KEY REFERENCES cards_analytics.dev.games (game_id),

    CONSTRAINT primary_key_game_seasons PRIMARY KEY (season_id, game_id)



) ;

INSERT INTO cards_analytics.dev.game_seasons VALUES

  
(0, 'prequel', 1),

(1, 'test_22', 3),

(2, 'development', 1),

(3, 'season_fall_1', 2),

(4, 'season_beta_1', 0),

(5, 'season_beta_2', 0),

(6, 'season_beta_3', 0),

(7, 'season_beta_4', 0),

(8, 'cb_season_1', 1),

(8, 'cb_season_1', 2),

(9, 'cb_season_2', 1),

(9, 'cb_season_2', 2),

(10, 'cb_season_3', 1),

(10, 'cb_season_3', 2)


;

SELECT * FROM cards_analytics.dev.games;

CREATE STORAGE INTEGRATION s3_int_card_game
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::406942271653:role/mysnowflakerolecard'
  STORAGE_ALLOWED_LOCATIONS = ('*')

;


DESC INTEGRATION s3_int_card_game;


;


CREATE OR REPLACE STAGE cards_analytics.public.new_stage
  STORAGE_INTEGRATION = s3_int_card_game
  url = 's3://cardgamesnow/'
  file_format = (type = csv, SKIP_HEADER = 1);


LIST @cards_analytics.public.new_stage/;

CREATE OR REPLACE HYBRID TABLE cards_analytics.dev.cards (


    card_id SMALLINT NULL PRIMARY KEY, 


    game_id SMALLINT, 

    game_name VARCHAR(16), 

    season_id SMALLINT, 

    season_name VARCHAR(13),
       
    card_index SMALLINT, 
       
    card_level SMALLINT, 
       
    card_type VARCHAR(36), 
       
    match_end_card BOOLEAN,

    CONSTRAINT fk_cards FOREIGN KEY (season_id, game_id) REFERENCES cards_analytics.dev.game_seasons (season_id, game_id)
    


);



COPY INTO cards_analytics.dev.cards
FROM @cards_analytics.public.new_stage/unique_cards_snowflake_mvp
file_format = (TYPE = CSV, FIELD_DELIMITER = ',' SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);



CREATE OR REPLACE HYBRID TABLE cards_analytics.dev.card_game_events (


    event_id INTEGER PRIMARY KEY,

  // check the column order


  //  event_id INTEGER, 
    
    user_id INTEGER NOT NULL,

    message_type VARCHAR(19),

    message_type_id SMALLINT,

    timestamp_utc TIMESTAMP_LTZ, 
    
    hand_shuffled_card_ids VARCHAR(40),
    
    match_end_card_ids VARCHAR(20),

    selected_card_id SMALLINT, 

    failed_card_id SMALLINT, 

 
//    selected_card_id SMALLINT NULL FOREIGN KEY REFERENCES cards_analytics.dev.cards (card_id), 
    
 //   failed_card_id SMALLINT NULL FOREIGN KEY REFERENCES cards_analytics.dev.cards (card_id),

    reason VARCHAR(12),
     
    


    

    
    active_card_failed_card_id_points SMALLINT, 

    succeeded_card_id SMALLINT,

 //   succeeded_card_id SMALLINT NULL FOREIGN KEY REFERENCES cards_analytics.dev.cards (card_id),
    
    
    active_card_id_succeeded_points SMALLINT,
    
    season_id SMALLINT, 
    
    game_id SMALLINT,

    CONSTRAINT fk_events_game_seasons FOREIGN KEY (season_id, game_id) REFERENCES cards_analytics.dev.game_seasons (season_id, game_id)


);


SELECT COUNT(*) FROM cards_analytics.dev.cards;




COPY INTO cards_analytics.dev.card_game_events
FROM @cards_analytics.public.new_stage/card_game_events
file_format = (TYPE = CSV, FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);


CREATE OR REPLACE INDEX idx_game_events_message_type ON cards_analytics.dev.card_game_events (message_type)  ;

CREATE OR REPLACE INDEX idx_game_events_reason ON cards_analytics.dev.card_game_events (reason)  ;

SELECT * FROM cards_analytics.dev.card_game_events LIMIT 5;




CREATE OR REPLACE HYBRID TABLE cards_analytics.dev.card_hands (

    card_position INTEGER,

    card_id INTEGER FOREIGN KEY REFERENCES cards_analytics.dev.cards (card_id),

    event_id INTEGER FOREIGN KEY REFERENCES cards_analytics.dev.card_game_events (event_id),

    match_end_card BOOLEAN,

    CONSTRAINT pk_card_hands PRIMARY KEY (card_position, event_id, card_id)


);





COPY INTO cards_analytics.dev.card_hands
FROM @cards_analytics.public.new_stage/card_game_card_hands
file_format = (type = csv, SKIP_HEADER = 1);

CREATE OR REPLACE INDEX idx_card_hands_card_id ON cards_analytics.dev.card_hands (card_id);



// DELETE FROM cards_analytics.dev.card_hands;


CREATE USER dbt_user_clement PASSWORD = "i_like_go_stop" ;
CREATE ROLE card_game_player;
GRANT USAGE ON WAREHOUSE NOICE TO ROLE card_game_player;
GRANT USAGE ON DATABASE cards_analytics TO ROLE card_game_player;
GRANT USAGE ON SCHEMA dev TO ROLE card_game_player;
GRANT ROLE card_game_player TO USER dbt_user_clement;
GRANT SELECT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cards_analytics.dev TO ROLE card_game_player;
GRANT SELECT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA cards_analytics.dev TO ROLE card_game_player;
GRANT CREATE VIEW ON SCHEMA cards_analytics.dev TO ROLE card_game_player;






