CREATE DATABASE NFL_Concussion;

\c NFL_Concussion

CREATE TABLE IF NOT EXISTS Game_Data (
    GameKey SMALLINT PRIMARY KEY
  , Season_Year SMALLINT
  , Season_Type VARCHAR(10)
  , Week SMALLINT
  , Game_Date TIMESTAMP
  , Game_Day VARCHAR(10)
  , Game_Site VARCHAR(15)
  , Start_Time VARCHAR(10)
  , Home_Team VARCHAR(20)
  , HomeTeamCode VARCHAR(3)
  , Visit_Team VARCHAR(20)
  , VisitTeamCode VARCHAR(3)
  , Stadium VARCHAR(40)
  , StadiumType VARCHAR(30)
  , Turf VARCHAR(30)
  , GameWeather VARCHAR(90)
  , Temperature REAL
  , OutdoorWeather VARCHAR(90)
) WITH (FILLFACTOR = 90);

CREATE INDEX idx_season_year ON Game_Data(Season_Year);
CREATE INDEX idx_game_date ON Game_Data(Game_Date);
CREATE INDEX idx_home_team ON Game_Data(Home_Team);
CREATE INDEX idx_visit_team ON Game_Data(Visit_Team);


CREATE TABLE IF NOT EXISTS Play_Information (
    Season_Year SMALLINT
  , Season_Type VARCHAR(5)
  , GameKey SMALLINT
  , Game_Date DATE
  , Week SMALLINT
  , PlayID SMALLINT
  , Game_Clock VARCHAR(5)
  , YardLine VARCHAR(10)
  , Quarter SMALLINT
  , Play_Type VARCHAR(5)
  , Poss_Team VARCHAR(3)
  , Home_Team_Visit_Team VARCHAR(10)
  , Score_Home_Visiting VARCHAR(10)
  , PlayDescription TEXT
  , PRIMARY KEY (GameKey, PlayID)
) WITH (FILLFACTOR = 90);

CREATE INDEX idx_pi_season_year ON Play_Information(Season_Year);
CREATE INDEX idx_pi_game_date ON Play_Information(Game_Date);
CREATE INDEX idx_pi_play_type ON Play_Information(Play_Type);
CREATE INDEX idx_pi_poss_team ON Play_Information(Poss_Team);


CREATE TABLE IF NOT EXISTS Role_Data (
    Season_Year SMALLINT
  , GameKey SMALLINT
  , PlayID SMALLINT
  , GSISID INTEGER
  , Role VARCHAR(5)
  , PRIMARY KEY (GameKey, PlayID, GSISID)
) WITH (FILLFACTOR = 90);

CREATE INDEX idx_rd_season_year ON Role_Data(Season_Year);
CREATE INDEX idx_rd_gsisid ON Role_Data(GSISID);
CREATE INDEX idx_rd_role ON Role_Data(Role);


CREATE TABLE IF NOT EXISTS NGS_Data (
    Season_Year SMALLINT
  , GameKey SMALLINT
  , PlayID SMALLINT
  , GSISID NUMERIC(10, 1)
  , Time TIMESTAMP
  , x REAL
  , y REAL
  , dis REAL
  , o REAL
  , dir REAL
  , Event VARCHAR(30)
) WITH (FILLFACTOR = 90);

CREATE INDEX idx_ngs_season_year ON NGS_Data(Season_Year);
CREATE INDEX idx_ngs_gamekey ON NGS_Data(GameKey);
CREATE INDEX idx_ngs_playid ON NGS_Data(PlayID);
CREATE INDEX idx_ngs_gsisid ON NGS_Data(GSISID);
CREATE INDEX idx_ngs_time ON NGS_Data(Time);
CREATE INDEX idx_ngs_event ON NGS_Data(Event);



CREATE TABLE IF NOT EXISTS Punt_Data (
    GSISID INTEGER
  , Number VARCHAR(3)
  , Position VARCHAR(3)
) WITH (FILLFACTOR = 90);

CREATE INDEX idx_pd_position ON Punt_Data(Position);


CREATE TABLE IF NOT EXISTS Video_Review (
    Season_Year SMALLINT
  , GameKey SMALLINT
  , PlayID SMALLINT
  , GSISID INTEGER
  , Player_Activity_Derived VARCHAR(10)
  , Turnover_Related VARCHAR(3)
  , Primary_Impact_Type VARCHAR(20)
  , Primary_Partner_GSISID VARCHAR(10)
  , Primary_Partner_Activity_Derived VARCHAR(10)
  , Friendly_Fire VARCHAR(10)
  , PRIMARY KEY (GameKey, PlayID, GSISID)
) WITH (FILLFACTOR = 90);

CREATE INDEX idx_vr_season_year ON Video_Review(Season_Year);
CREATE INDEX idx_vr_gsisid ON Video_Review(GSISID);
CREATE INDEX idx_vr_player_activity ON Video_Review(Player_Activity_Derived);
CREATE INDEX idx_vr_impact_type ON Video_Review(Primary_Impact_Type);
CREATE INDEX idx_vr_partner_gsisid ON Video_Review(Primary_Partner_GSISID);



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
