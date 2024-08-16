# NFL Concussion and Lower Body Injury Analysis
***Using Machine Learning and Data Analytics to Identify and Predict High Risk Conditions that Lead to Concussions or Lower Body Injuries During Certain Plays in the NFL***
### *by* Justin R. Papreck

This project is a redevelopment of the "NFL Injury Analysis" project from 2022/2023, looking at better processes, machine learning methods, analysis, and visualizations. Additionally, this project will further develop predictive model for high risk behavior. This project will also go into more details about the biomechanics related to these types of injuries, and offer potential solutions that could help reduce the frequency of incidence, as well as solutions that can reduce the damage done to the body when exposed to such physical trauma. 

---
***Contents***



--- 
# The Data

## Sources
The sources of these data were related to competitions on Kaggle; however the goals of this project are not directly aligned with the goals of the competitions. These competitions were originally non-traditional, just looking for data scientists to come up with models that could help better understand correlations. This project takes it a step further, not only looking at modifying the game and plays, but also finding biomechanical ways to support the players within their specific roles based on their specific risks.  

- [NFL 1st and Future - Analytics](https://www.kaggle.com/competitions/nfl-playing-surface-analytics/data)
- [NFL Punt Analytics Competition](https://www.kaggle.com/competitions/NFL-Punt-Analytics-Competition/data)


Due to the size of the tracking data, I will need to transform the data prior to putting into the database, so I will be performing this as a standard ETL using Polars in python to extract and transform, and then loading into tSQL via Azure for analytics and vizzes. 

## Databases 
The original project used a PostgreSQL, but I will be changing this to tSQL via Azure. 

### Saving the data
The initial processing was done by extracting data from a set of .csv files and transforming them. Due to the high volume of data within the tables, I changed data formats of the numbers and strings to lighter integer values and enumerated values. To maintain the datatype structure, I saved such as parquet files, since .csv cannot save them. 

Since there is no way to connect the data from each datatset - the player IDs are not specified as consistent across the data, and the seasons are also unclear from the first dataset, I will maintain these in two separate databases; though this may change if there is enough of a correlation in Player ID numbers to the GSISID numbers, that may link the two datasets. This would be ideal, because it is possible that some players who sustained one of the injuries may also correlate with the other type. 

The code that establishes the databases and tables are in the following files:
-  SQL/concussion.sql
-  SQL/injuries.sql 

#### Injuries.sql
    The injuries database was straightforward in setup. There are three tables: plays, injuries, and tracking.
    The Playkey is used as the Primary Key and connects the plays table to each of the other tables. The three tables within this database contained qualitative measures on the injuryrecord and playlist tables, and then the quantitative positioning and directional data from the NGS tracking was contained on the playertrackdata table. 

    After adding the data to SQL, the unnecessary columns were removed and the injuryrecord and playlist tables were joined and saved as a new table, Qualitative, since upon EDA it was noted that there would need to be substantial programmatic changes to those tables that would be ideal to do in transformation on Python prior to joining with the tracking data. 

    NOTE:  Body mass distribution; I came across an estimate that 30%-40% of the body mass is waist and below. Since NFL players have a lot more upper body bulk, I will be using the 30% estimate. This will be necessary in the calculation of torque of the upper body with respect to the lower body. 

    

#### Concussion.sql
    The concussion database is made up of 6 tables: game_data, play_information, role_data, NGS_data, punt_data, and video_review. Most of these are connected via a PK combining the GSISID, PlayID, and GameKey. Each of the tables with the exception of the NGS data table, provide overall qualities of the stadium, game, and players. The NGS data imported 10 different files' worth of data, resulting in 61 million rows of data collected every 1/10th second. 

    Issues: 
    NGS_Data - when importing the data, at least one of the INT values was entered as a float, so this will need to be changed back to INT. 
    Video_Review - one of the rows includes "Unknown" in almost every column, making it such that the primary_partner_gsisid is included as VARCHAR instead of INT again.  

    NOTE: there are 1429 records in the ngs data that have no associated gsisid, so these will be dropped, since they're not associated with a known player. 

    NOTE: All of the players that sustained concussions have known positions and gsisid values. I am going to remove any players with unknown positions

    There were 37 injuries catalogued in the video_review
    There were 2442 distinct gsisid across both seasons, so this includes the players who played both or only one season, since there are only about 1700 players per season. 

    However... in the ngs_data, in 2016 there were 2542 unique gsisid
    and in 2017 there were 2890 unique gsisid

    I have no way of accounting for these values regarding their positions and thus their weights... so I'm going to filter these out. 

    **************

    ## Biomechanics
        I really want to look at the moments of impact, or critical moments in the play when the player stops. I will have the velocity in the moments just before and at the stopping time, which will allow me to calculate the force, work and power. I can then use these metrics to compare to known quantities that can result in such injuries, and correlate the amount of force with the duration of injury. 

    - Find a 2016, 2017 rosters to get player weights per position
    - if possible, get heights and shoulder width dimensions. 

    Moment of Inertia of a rotating person as they turn on the field will require the radius from their shoulder to neck and their mass. 

    If I know their impact is hitting the ground, I can calculate the moment of inertia based on their height rotating from 2/3 height CM to the ground. Will need to consult any notes or videos about each play. 

    The most common measure for assessing concussions is Impulse (F*delta_t), which we have. 


# Separating the Data
## Summary Data

The summary data is a collection of the following parameters from both the injury and the concussion datesets. 
- PlayKey
- Distance
- Displacement
- Path_diff
- Max/Mean Angle_diff
- Max/Mean Speed
- Max/Mean Impulse
- Max/Mean Torque
- Max/Mean Torque_int

These datasets will be joined with a union, since the PlayKeys will not be overlapping - NEED TO VERIFY THIS

### Vizzes - all need to be normalized
- Injuries by Weather Condition
- Concussions by playtype
- Concussions by offense/defense
- Injuries by Field Type
- Injuries by Temperatures (5 degree bins)
- Injuries by Position: 
    - Add Is_Severe parameter

### Machine Learning
- Predict severe injuries
- Predict injury type (including concussions)
- Predict if an injury occurs: 
    - Defining high risk behaviors
- Identify high risk GSISIDs
    - Can we assess if particular players are involved in multiple high risk plays? 
    - Do these GSISIDs eventually suffer from an injury?

## Tracking Data (NGS)

The tracking data is collected at 10 Hz. 
The data from concussions and injuries are substantially large, so that doing a union is prohibitive unless sampled. 
The following are the data collected and calcuated in the NGS data, * denotes only in Concussion set

- PlayKey
- DateTime*
- GSISID*
- time
- x
- y
- dir
- dis
- o 
- Angle_diff
- Displacement
- Speed
- Direction
- Vx
- Vy 
- w_dir
- w_o
- w_diff
- position
- height_m
- weight_kg
- chest_rad
- px
- py
- moment
- moment_upper
- p_magnitude
- L_dir
- L_diff
- Jx
- Jy
- J_magnitude
- torque
- torque_int


What these data will be used for are in creating vizzes for each individual injury path on Tableau, for which only plays associated with injuries are needed.
Additionally, I would like to look at mappings of some of the plays where an injury occurred, but utilizing the momentum, impulse, torque, and internal torque 

### Dashboard per injury?
- Torque / time
- Torque_int / time
- J_magnitude / time
- (Distance - Displacement) / time
- Angular Velocities / time
- Angular Momentums / time
- p_magnitude / time

all of these could be attached to a dash where the field is displayed with the x,y coordinate path 

### Concussion analysis
With this, for all but one of the plays the player collided with another player. I would also like to plot the two people's paths and show the differences
in the impulses and torques at the point of contact. What is different between the impulse that caused the concussion and the impulse of the opponent that 
didn't incur the same injury? 


    

    

    