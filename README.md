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


    

    

**********

IDEA: The number of samples out of the box is a massive 29,000 non-injury plays compared to 77 injuries, which will be cut down to about 50 when performing the train-test split. 

Since I want to know what Physical parameters are leading to the injuries, I can look at similar plays by filtering for the playtype and position. Keeping fieldtype as a parameter is fine. 

*****

Post Tableau: 
I started noticing that some of the plays continue long after the injury, especially concussions. Additionally, there is a lot happening in the first few seconds of each play, where players are turning around prior to any translational movement. This can potentially skew all of the data for both the injured and uninjured players. What I believe I will need to do is look at random chunks after the first or so seconds of play for the uninjured playkeys

For those who are injured, I can look at different players during the same timeframes. This is going to be a sampling nightmare. 

It also appears that some of the really long plays just had the clock keep running. 

Since I can look at the plays where an injury occurred, especially with those with concussions + opponent, I can at least filter out the beginning and identify point of contact. 

I can try to do these with the lower body injuries. I think at this point I need to abandon foot injuries, since there isn't a timepoint when we can ID this happening. 

Plan of action
1. Identify points of contact with concussion players - I want to observe the 5 seconds prior to contact of all injury plays.I'll need to record each of the times of contact. I can try to programmatically find the point when the two players are closest together, but that may happen after contact, so I don't think this is ideal 
2. Identify any player stop-points in the non-concussion injuries. Is there a time when the player goes from movement to stop, even if they start up again. If they do start up again, is there a reduction in momentum?
3. Let's look at length of plays. Some of these plays extend way beyond a minute, which is abnormal for an NFL play. Chances are that the clock wasn't stopped. I would like to try to find 5 second chunks within these plays that fall below a certain timeframe. Perhaps I can use the minimum and maximum times of injury from those analyses. 
4. I will have to slice up the plays programmatically somehow. I can probably use a random number to assign a number between 0 and 1 to multiply by the Minimum time recorded to get a start time, and then add 5 seconds to look at the values per play key of each play, prior to encoding for ML.
5. Should I try to adapt to quaternions for angular analysis? I think the mods should be good enough at the moment, but I'll need to redo the analysis after chunking the data into 5 second bits. 


This is looking like it's going to be extremely time-consuming. That being said, I need to make updates to the website ASAP. For the time being, I am going to reduce the available plays to a single play per slide on Tableau. I picked a few for the presentation. Perhaps I can figure out how to animate plays using Python to create gifs, which would be much less data intensive and better for the website. 

Should I just abandon the other injuries and focus on concussions right now? Most of my medical data is related to the concussive and subconcussive injuries. 

I should try to see if I can source new data and come up with a plan to scrape if possible. 





******
# Discussion

In the processing of the tracking information, a lot of information had to be considered. 
To start, there were 7.916 GB of data to be processed for tracking alone in the concussion datasets, already split into 10 different files - 5 per season. The injury tracking data is a single file that is  3.88 GB, which has to be broken up for processing. 

Using Polars, the total processing time of the concussion tracking data was 329 seconds (about 5.5 minute), and the processing of the injury tracking data was 355 seconds (about 6 minutes).  

The important information included in these data are the x and y coordinate positions with respect to the football field, the orientation of the player and the direction they are moving in, and the time that has elapsed since the start of each play, in addition to any of the indexing data to identify the individual plays, games, players, etc. Speed was included, but did not specify how speed was monitored, so this was dropped and calculated using the player's positional changes. 

Data that were not provided: the identity of the players, player weight, size, or height, which were needed for some of the analyses. 

Physical Parameters that need to be considered, and also pose an issue to the expansion of the data: 
velocity - this was obtained by the change in x or y over time (derivative of position with respect to time)
momentum - the mass* of each player multiplied by velocity
impulse - the change in momentum, or in more familiar terms, force multiplied by time.
angular velocity - calculated from the change in direction of the player's body or orientation
angular momentum - calcuated using the moment of inertia of each player, a multiple of the player's mass and assuming cylindrical rotation, 1/2 MR^2 where the radius is from center of rotation at crown to end of shoulder
torque - the change in angular momentum over time, think of this as rotational force
torque_internal - the change in the angular momentum of the difference between the direction of movement and orientation, which also uses a smaller moment of inertia, only observing mass of the upper 2/3 of the body.
Displacement - how the distance an object is relative to its starting place. So when I took a break or 3 to think through how to fix the trigonometry fringe issues, when I returned to my desk, I had a displacement of 0.  
Distance - the total of displacements from each step, which makes it look a lot better when I can say that I actually walked 2 or so miles, despite having no net displacement. This is going to be important when trying to find a memory-efficient way of determining linearity or alinearity of a path taken. When displacement = distance, the path is linear. When the distance is very large compared to the displacement, the body has made directional changes, increasing the path length in getting to the final destination. 


Most of these data are maintained in x and y vector formats, but in order to actually look at the weight the parameters such as momentum and impulse have on the body, I need to calculate the magnitude of the vector components, which applies the distance formula

If we think about velocity and angular velocity as the derivative of position, and then impulse as related to (not exactly) the derivative of velocity, we can gather these parameters by using the differences between previous rows, splitting the data by PlayKey. 
This means two things: 
1. Increasing the size of the data
2. Adding 2 rows of nonzero null values to each unique playkey that have to be dealt with. 


For the qualtitative measures maintained in the tracking data, we had limited options. For example, the number of positions or types of injury or impact are relatively small, so the data can be kept as categorical instead of string.
With the quantitative measures, values could be maintained as float 32 or int 32 (the integer values just barely exceeded int 16), so using parquet files and enabling string cache maintained the smaller data structure during processing.

One additional consideration with the pipeline was removing the unnecessary data following use. Since the tracking data is primarily used to create visualizations, keeping the vector components for impulse and momentum aren't necessary once they're used to calculate the magnitude. 
Likewise, the weights and measurements of the players aren't needed after determining the moments of inertia, so these parameters can all be dropped following the processing steps to reduce size and improve efficiency of data handling. 

A final thing in tracking information was that the data for visuals did not need all of the plays that weren't associated with injuries. The only non-injured PlayKeys kept were those players who were involved in the play with the injured player, as the player who made contact with the injured. 


For purposes of machine learning, however, it is extremely important to keep the data from the non-injured players and uneventful plays, to investigate the parameters leading to high risk plays. 
In these, we will be adding a lot of the qualitative data, but the paths are not as important. Aggregation of these data to look at the maxima and mean values were calculated to produce summary data that can be used with machine learning. 


