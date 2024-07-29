-- Create the Database for the NFL Surface Injuries for optimized code.

CREATE DATABASE NFL_Surface;

CREATE TABLE IF NOT EXISTS Plays (
    PlayerKey INT
  , GameID VARCHAR(10)
  , PlayKey VARCHAR(20)
  , RosterPosition VARCHAR(20)
  , PlayerDay SMALLINT
  , PlayerGame SMALLINT
  , StadiumType VARCHAR(25)
  , FieldType VARCHAR(10)
  , Temperature SMALLINT
  , Weather VARCHAR(90)
  , PlayType VARCHAR(20)
  , PlayerGamePlay SMALLINT
  , Position VARCHAR(15)
  , PositionGroup VARCHAR(15)
  , PRIMARY KEY (PlayKey)
) WITH (FILLFACTOR = 90);

CREATE INDEX idx_playerkey ON Plays(PlayerKey);
CREATE INDEX idx_gameid ON Plays(GameID);
CREATE INDEX idx_playtype ON Plays(PlayType);
CREATE INDEX idx_position ON Plays(Position);



CREATE TABLE IF NOT EXISTS Injuries (
    PlayerKey INTEGER PRIMARY KEY
  , GameID VARCHAR(10)
  , PlayKey VARCHAR(20)
  , BodyPart VARCHAR(5)
  , Surface VARCHAR(10)
  , DM_M1 SMALLINT
  , DM_M7 SMALLINT
  , DM_M28 SMALLINT
  , DM_M42 SMALLINT
) WITH (FILLFACTOR = 90);

CREATE INDEX idx_playkey ON Injuries(PlayKey);
CREATE INDEX idx_bodypart ON Injuries(BodyPart);
CREATE INDEX idx_surface ON Injuries(Surface);




CREATE TABLE IF NOT EXISTS Tracking (
    PlayKey VARCHAR(20)
  , time REAL
  , event VARCHAR(25)
  , x REAL
  , y REAL
  , dir REAL
  , dis REAL
  , o REAL
  , s REAL
  , PRIMARY KEY (PlayKey, time)
) WITH (FILLFACTOR = 90);

CREATE INDEX idx_tr_playkey ON Tracking(PlayKey);
CREATE INDEX idx_event ON Tracking(event);





-- Insert the data, the first and second should take a few seconds. The last table took about 19 minutes to populate
COPY Plays
FROM 'F:\Data\nfl-playing-surface-analytics\PlayList.csv'
DELIMITER ',' CSV HEADER;

COPY Injuries
FROM 'F:\Data\nfl-playing-surface-analytics\InjuryRecord.csv'
DELIMITER ',' CSV HEADER;

COPY Tracking
FROM 'F:\Data\nfl-playing-surface-analytics\PlayerTrackData.csv'
DELIMITER ',' CSV HEADER;