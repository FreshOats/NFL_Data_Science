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