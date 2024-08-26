CREATE TABLE Tracking_Data (
    PlayKey VARCHAR(20),
    time FLOAT,
    x FLOAT,
    y FLOAT,
    dir FLOAT,
    o FLOAT,
    Angle_Diff FLOAT,
    Displacement FLOAT,
    Speed FLOAT,
    vx FLOAT,
    vy FLOAT,
    omega_dir FLOAT,
    omega_o FLOAT,
    omega_diff FLOAT,
    Position VARCHAR(50),
    Height_m FLOAT,
    Weight_kg FLOAT,
    Chest_rad_m FLOAT,
    px FLOAT,
    py FLOAT,
    moment FLOAT,
    moment_upper FLOAT,
    p_magnitude FLOAT,
    L_dir FLOAT,
    L_diff FLOAT,
    Jx FLOAT,
    Jy FLOAT,
    J_magnitude FLOAT,
    torque FLOAT,
    torque_internal FLOAT,
    InjuryType VARCHAR(50),
    GSISID INT,
    Player_Activity_Derived VARCHAR(50),
    Primary_Impact_Type VARCHAR(50),
    Primary_Partner_GSISID VARCHAR(20),
    Primary_Partner_Activity_Derived VARCHAR(50),
    OpponentKey VARCHAR(20)
);

CREATE TABLE Concussion_Summary (
    PlayKey VARCHAR(20),
    Position VARCHAR(50),
    Role VARCHAR(50),
    Play_Type VARCHAR(50),
    Poss_Team VARCHAR(50),
    Game_Site VARCHAR(50),
    HomeTeamCode VARCHAR(50),
    VisitTeamCode VARCHAR(50),
    StadiumType VARCHAR(50),
    FieldType VARCHAR(50),
    Weather VARCHAR(50),
    Temperature FLOAT,
    Player_Activity_Derived VARCHAR(50),
    Primary_Impact_Type VARCHAR(50),
    Primary_Partner_Activity_Derived VARCHAR(50),
    Primary_Partner_GSISID INT,
    OpponentKey VARCHAR(20),
    IsInjured TINYINT,
    Home_Score TINYINT,
    Visiting_Score TINYINT,
    Score_Difference TINYINT,
    Position_right VARCHAR(50),
    Distance FLOAT,
    Displacement FLOAT,
    Path_Diff FLOAT,
    Max_Angle_Diff FLOAT,
    Mean_Angle_Diff FLOAT,
    Max_Speed FLOAT,
    Mean_Speed FLOAT,
    Max_Impulse FLOAT,
    Mean_Impulse FLOAT,
    Max_Torque FLOAT,
    Mean_Torque FLOAT,
    Max_Int_Torque FLOAT,
    Mean_Int_Torque FLOAT
);

CREATE TABLE Injury_Summary (
    PlayKey VARCHAR(20),
    Position VARCHAR(50),
    StadiumType VARCHAR(50),
    FieldType VARCHAR(50),
    Temperature SMALLINT,
    Weather VARCHAR(50),
    PlayType VARCHAR(50),
    BodyPart VARCHAR(50),
    DM_M1 TINYINT,
    DM_M7 TINYINT,
    DM_M28 TINYINT,
    DM_M42 TINYINT,
    IsInjured TINYINT,
    IsSevere TINYINT,
    Position_right VARCHAR(50),
    Distance FLOAT,
    Displacement FLOAT,
    Path_Diff FLOAT,
    Max_Angle_Diff FLOAT,
    Mean_Angle_Diff FLOAT,
    Max_Speed FLOAT,
    Mean_Speed FLOAT,
    Max_Impulse FLOAT,
    Mean_Impulse FLOAT,
    Max_Torque FLOAT,
    Mean_Torque FLOAT,
    Max_Int_Torque FLOAT,
    Mean_Int_Torque FLOAT
);
