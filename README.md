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

## Databases 
The original project used a PostgreSQL database, which is what I will continue to use for this iteration of the project. 

Since there is no way to connect the data from each datatset - the player IDs are not specified as consistent across the data, and the seasons are also unclear from the first dataset, I will maintain these in two separate databases; though this may change if there is enough of a correlation in Player ID numbers to the GSISID numbers, that may link the two datasets. This would be ideal, because it is possible that some players who sustained one of the injuries may also correlate with the other type. 

The code that establishes the databases and tables are in the following files:
-  SQL/concussion.sql
-  /SQL/injuries.sql 

#### Injuries.sql
    The injuries database was straightforward in setup. There are three tables: plays, injuries, and tracking.
    The Playkey is used as the Primary Key and connects the plays table to each of the other tables. The three tables within this database contained qualitative measures on the injuryrecord and playlist tables, and then the quantitative positioning and directional data from the NGS tracking was contained on the playertrackdata table. 

    After adding the data to SQL, the unnecessary columns were removed and the injuryrecord and playlist tables were joined and saved as a new table, Qualitative, since upon EDA it was noted that there would need to be substantial programmatic changes to those tables that would be ideal to do in transformation on Python prior to joining with the tracking data. 

    

#### Concussion.sql
    The concussion database is made up of 6 tables: game_data, play_information, role_data, NGS_data, punt_data, and video_review. Most of these are connected via a PK combining the GSISID, PlayID, and GameKey. Each of the tables with the exception of the NGS data table, provide overall qualities of the stadium, game, and players. The NGS data imported 10 different files' worth of data, resulting in 61 million rows of data collected every 1/10th second. 

    Issues: 
    NGS_Data - when importing the data, at least one of the INT values was entered as a float, so this will need to be changed back to INT. 
    Video_Review - one of the rows includes "Unknown" in almost every column, making it such that the primary_partner_gsisid is included as VARCHAR instead of INT again.  


    **************

    ## Biomechanics
        I really want to look at the moments of impact, or critical moments in the play when the player stops. I will have the velocity in the moments just before and at the stopping time, which will allow me to calculate the force, work and power. I can then use these metrics to compare to known quantities that can result in such injuries, and correlate the amount of force with the duration of injury. 

    - Find a 2016, 2017 rosters to get player weights per position
    - if possible, get heights and shoulder width dimensions. 

    Moment of Inertia of a rotating person as they turn on the field will require the radius from their shoulder to neck and their mass. 

    If I know their impact is hitting the ground, I can calculate the moment of inertia based on their height rotating from 2/3 height CM to the ground. Will need to consult any notes or videos about each play. 

    

    ### Beautiful Soup
    I'm going to have to scrape

    https://www.espn.com/nfl/players

    to get the heights and weights of players from this season to calculate averages. Or I can just trust the averages already present, though not for the 2016 and 2017 seasons. Whatever. 
    