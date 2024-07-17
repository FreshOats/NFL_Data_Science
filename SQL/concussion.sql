-- Create the concussion database and populate with database
CREATE DATABASE concussion; 

CREATE TABLE game_data (
	gamekey INT
	, season_year INT
	, season_type VARCHAR(100)
	, week INT
	, game_date VARCHAR(100)
	, game_day VARCHAR(100)
	, game_site VARCHAR(100)
	, start_time VARCHAR(100)
	, home_team VARCHAR(100)
	, hometeamcode VARCHAR(100)
	, visit_team VARCHAR(100)
	, vistteamcode VARCHAR(100)
	, stadium VARCHAR(100)
	, stadiumtype VARCHAR(100)
	, turf VARCHAR(100)
	, gameweather VARCHAR(100)
	, temperature FLOAT
	, outdoorweather VARCHAR(100)
	, PRIMARY KEY (gamekey)
);

CREATE TABLE play_information (
	season_year INT
	, season_type VARCHAR(100)
	, gamekey INT
	, game_date VARCHAR(100)
	, week INT
	, playid INT
	, game_clock VARCHAR(100)
	, yardline VARCHAR(100)
	, quarter INT
	, play_type VARCHAR(100)
	, poss_team VARCHAR(100)
	, home_team_visit_team VARCHAR(100)
	, score_home_visiting VARCHAR(100)
	, playdescription VARCHAR(1000)
	, FOREIGN KEY (gamekey) REFERENCES game_data (gamekey)
	, PRIMARY KEY (gamekey, playid)
	, UNIQUE (gamekey,playid)
);

CREATE TABLE role_data (
	season_year INT
	, gamekey INT
	, playid INT
	, gsisid INT
	, prole VARCHAR(100)
	, FOREIGN KEY (gamekey) REFERENCES game_data (gamekey)
	, PRIMARY KEY(gamekey, playid, gsisid)
	, UNIQUE(gamekey,playid,gsisid)
);

CREATE TABLE NGS_data (
	season_year INT
	, gamekey INT
	, playid INT
	, gsisid FLOAT -- at least one of the values is in the wrong format
	, g_time VARCHAR(100)
	, x FLOAT
	, y FLOAT
	, dis FLOAT
	, o FLOAT
	, dir FLOAT
	, g_event VARCHAR(100)
);

CREATE TABLE punt_data (
	gsisid INT
	, number VARCHAR(100)
	, position VARCHAR(100)
	, PRIMARY KEY (gsisid, number)
	, UNIQUE (gsisid, number)
);

CREATE TABLE video_review (
    season_year INT
	, gamekey INT
	, playid INT
	, gsisid INT
    , player_activity VARCHAR(12)
    , turnover_related VARCHAR(5)
    , primary_impact_type VARCHAR(25)
    , primary_partner_gsisid VARCHAR(10) --This should be INT, but one value is VARCHAR, will have to clean
    , primary_partner_activity VARCHAR(25)
    , friendly_fire VARCHAR(10)
    , PRIMARY KEY(gsisid,playid,gamekey)
); 


--- Copy the data from the files into the tables. Note, the NGS data will come from 10 different files.

COPY game_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\game_data.csv'
DELIMITER ',' CSV HEADER;

COPY play_information
FROM 'F:\Data\NFL-Punt-Analytics-Competition\play_information.csv'
DELIMITER ',' CSV HEADER;

COPY punt_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\player_punt_data.csv'
DELIMITER ',' CSV HEADER;

COPY role_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\play_player_role_data.csv'
DELIMITER ',' CSV HEADER;

COPY video_review
FROM 'F:\Data\NFL-Punt-Analytics-Competition\video_review.csv'
DELIMITER ',' CSV HEADER;




--- THE 2016 NGS Data ~ 3 minutes
COPY NGS_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\NGS\2016-post.csv'
DELIMITER ',' CSV HEADER;

COPY NGS_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\NGS\2016-pre.csv'
DELIMITER ',' CSV HEADER;

COPY NGS_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\NGS\2016-reg-wk1-6.csv'
DELIMITER ',' CSV HEADER;

COPY NGS_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\NGS\2016-reg-wk7-12.csv'
DELIMITER ',' CSV HEADER;

COPY NGS_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\NGS\2016-reg-wk13-17.csv'
DELIMITER ',' CSV HEADER;



---The 2017 NGS data - all NGS data is upserted into the same table ~ 4 minutes
COPY NGS_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\NGS\2017-post.csv'
DELIMITER ',' CSV HEADER;

COPY NGS_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\NGS\2017-pre.csv'
DELIMITER ',' CSV HEADER;

COPY NGS_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\NGS\2017-reg-wk1-6.csv'
DELIMITER ',' CSV HEADER;

COPY NGS_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\NGS\2017-reg-wk7-12.csv'
DELIMITER ',' CSV HEADER;

COPY NGS_data
FROM 'F:\Data\NFL-Punt-Analytics-Competition\NGS\2017-reg-wk13-17.csv'
DELIMITER ',' CSV HEADER;


--- Upon completion of NGS data upsertion, there should be around 61 million rows