-- Creating the Database for the Surface/Injury Analytics
CREATE DATABASE NFL; 

CREATE TABLE plays (
	playerkey INT
  	, gameid VARCHAR(50)
	, playkey VARCHAR(50)
	, rosterposition VARCHAR(50)
	, playerday INT
	, playergame INT
	, stadiumtype VARCHAR(50)
	, fieldtype  VARCHAR(50)
	, temperature INT
	, weather VARCHAR(250)
	, playtype VARCHAR(50)
	, playergameplay INT
	, p_position VARCHAR(50)
	, postiongroup VARCHAR(50)
	, PRIMARY KEY (playkey)
);

CREATE TABLE injuries (
    playerkey INT
    , gameid VARCHAR(50)
	, playkey VARCHAR(50)
	, bodypart VARCHAR(50)
	, fieldtype VARCHAR(50)
	, DM_M1 INT
	, DM_M7 INT
	, DM_M28 INT
	, DM_M42 INT
	, FOREIGN KEY (playkey) REFERENCES plays(playkey)			 
);

CREATE TABLE tracking (
	playkey VARCHAR(50)
	, time FLOAT
	, event VARCHAR(50)
	, x FLOAT
	, y FLOAT
	, dir FLOAT
	, dis FLOAT
	, O FLOAT
	, s FLOAT
    , FOREIGN KEY (playkey) REFERENCES plays(playkey)
);


-- Insert the data, the first and second should take a few seconds. The last table took about 19 minutes to populate
COPY plays
FROM 'F:\Data\nfl-playing-surface-analytics\PlayList.csv'
DELIMITER ',' CSV HEADER;

COPY injuries
FROM 'F:\Data\nfl-playing-surface-analytics\InjuryRecord.csv'
DELIMITER ',' CSV HEADER;

COPY tracking
FROM 'F:\Data\nfl-playing-surface-analytics\PlayerTrackData.csv'
DELIMITER ',' CSV HEADER;
