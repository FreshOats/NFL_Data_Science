# Cleaning Functions
# This will set up the code for all cleaning functions. Currently, this cleans only the injury data for surface injuries. 
# TO RUN: 
    # from CleaningFunctions import *
    # clean_injuries('quals')


##### Primary Cleaning Functions #####
def clean_injuries():
    from DataHandler import data_writer
    database = "nfl_surface"
    quals = table_joiner() 
    quals = column_capitalizer(quals, df_name='quals')
    quals = stadium_cleaner(quals, df_name='quals')
    quals = weather_cleaner(quals)
    quals = injury_cleaner(quals)
    data_writer(quals, database, "qualitative")
    print("Play and Injury Data has been cleaned and uploaded as qualitative")
    del quals


def clean_concussions():
    from DataHandler import data_loader, data_writer
    import polars as pl
    database = 'nfl_concussion'

    df = data_loader(database='nfl_concussion', dataset='concussion')
    df = column_capitalizer(df, 'concussion')
    df = stadium_cleaner(df, 'concussion')
    df = weather_cleaner(df)
    df = turf_cleaner(df)
    df = df.filter(pl.col("Game_Date").is_not_null())
    df = score_splitter(df)
    data_writer(df, database, "clean_data")
    print("Concussion Data has been cleaned and uploaded as clean_data")
    # return df



######################################################################################
# Extracts and joins the necessary columns from the Injuries and Plays tables
def table_joiner():
    import polars as pl
    from DataHandler import data_loader

    plays = data_loader('plays', 'nfl_surface')
    injuries = data_loader('injuries', 'nfl_surface')

    quals = (
        plays.join(injuries, on="playkey", how='left')
        .select([
            pl.col("playkey")
            , pl.col("position")
            , pl.col("stadiumtype")
            , pl.col("fieldtype")
            , pl.col("temperature")
            , pl.col("weather")
            , pl.col("playtype")
            , pl.col("bodypart")
            , pl.col("dm_m1")
            , pl.col("dm_m7")
            , pl.col("dm_m28")
            , pl.col("dm_m42")

        ])
    )
    return quals


# Changes the all lower-case to Capitalized PascalCase column headers 
def column_capitalizer(df, df_name):          
    if df_name == 'quals':
        columns = {
        'playkey': "PlayKey"
        , 'position': 'Position'
        , 'stadiumtype': 'Stadium_Type'
        , 'fieldtype': 'Field_Type'
        , 'temperature': 'Temperature'
        , 'weather': 'Weather'
        , 'playtype': 'Play_Type'
        , 'bodypart': 'Body_Part'
        , 'dm_m1': 'DM_1'
        , 'dm_m7': 'DM_7'
        , 'dm_m28': 'DM_28'
        , 'dm_m42': 'DM_42'
        }

    elif df_name == 'concussion':
        columns = {
        'playkey': 'PlayKey'
        , 'position': 'Position'
        , 'role': 'Role'
        , 'game_date': 'Game_Date'
        , 'yardline': 'Yardline'
        , 'quarter': 'Quarter'
        , 'play_type': 'Play_Type'
        , 'poss_team': 'Poss_Team'
        , 'score_home_visiting': 'Score_Home_Visiting'
        , 'game_site': 'Game_Site'
        , 'start_time': 'Start_Time'
        , 'hometeamcode': 'Home_Team_Code'
        , 'visitteamcode': 'Visit_Team_Code'
        , 'stadiumtype': 'Stadium_Type'
        , 'turf': 'Field_Type'
        , 'gameweather': 'Weather'
        , 'temperature': 'Temperature'
        , 'player_activity_derived': 'Player_Activity_Derived'
        , 'primary_impact_type': 'Primary_Impact_Type'
        , 'primary_partner_activity_derived': 'Primary_Partner_Activity_Derived'
        , 'primary_partner_gsisid': 'Primary_Partner_Gsisid'
        }


    df = df.rename(columns)
    return df

# This changes stadiums to either Indoor or Outdoor per game records - some of the dome stadiums have a roof that can open, if open the game is considered outdoor.
def stadium_cleaner(df, df_name):
    import polars as pl 

    if df_name == 'quals':       
        stadium_dict = {
            'Outdoor': 'Outdoor'
            , 'Indoors': 'Indoor'
            , 'Oudoor': 'Outdoor'
            , 'Outdoors': 'Outdoor'
            , 'Open': 'Outdoor'
            , 'Closed Dome': 'Indoor'
            , 'Domed, closed': 'Indoor'
            , 'Dome': 'Indoor'
            , 'Indoor': 'Indoor'
            , 'Domed': 'Indoor'
            , 'Retr. Roof-Closed': 'Indoor'
            , 'Outdoor Retr Roof-Open': 'Outdoor'
            , 'Retractable Roof': 'Indoor'
            , 'Ourdoor': 'Outdoor'
            , 'Indoor, Roof Closed': 'Indoor'
            , 'Retr. Roof - Closed': 'Indoor'
            , 'Bowl': 'Outdoor'
            , 'Outddors': 'Outdoor'
            , 'Retr. Roof-Open': 'Outdoor'
            , 'Dome, closed': 'Indoor'
            , 'Indoor, Open Roof': 'Outdoor'
            , 'Domed, Open': 'Outdoor'
            , 'Domed, open': 'Outdoor'
            , 'Heinz Field': 'Outdoor'
            , 'Cloudy': 'Outdoor'
            , 'Retr. Roof - Open': 'Outdoor'
            , 'Retr. Roof Closed': 'Indoor'
            , 'Outdor': 'Outdoor'
            , 'Outside': 'Outdoor'
        }


        df = df.with_columns(pl.col("Stadium_Type").fill_null("Outdoor")) # Since most stadiums are outdoor and the percentage of games played indoor is already met by the known indoor games those seasons, all unknown games were set to outdoor


    elif df_name == 'concussion':
        stadium_dict = {
            'Outdoor': 'Outdoor'
            , 'outdoor': 'Outdoor'
            , 'Indoors': 'Indoor'
            , 'Indoors (Domed)': 'Indoor'
            , 'Oudoor': 'Outdoor'
            , 'Outdoors': 'Outdoor'
            , 'Outdoors ': 'Outdoor'
            , 'Open': 'Outdoor'
            , 'Closed Dome': 'Indoor'
            , 'Domed, closed': 'Indoor'
            , 'Dome': 'Indoor'
            , 'Indoor': 'Indoor'
            , 'Domed': 'Indoor'
            , 'Retr. Roof-Closed': 'Indoor'
            , 'Outdoor Retr Roof-Open': 'Outdoor'
            , 'Retractable Roof': 'Indoor'
            , 'Ourdoor': 'Outdoor'
            , 'Indoor, Roof Closed': 'Indoor'
            , 'Retr. Roof - Closed': 'Indoor'
            , 'Bowl': 'Outdoor'
            , 'Outddors': 'Outdoor'
            , 'Retr. Roof-Open': 'Outdoor'
            , 'Dome, closed': 'Indoor'
            , 'Indoor, Open Roof': 'Outdoor'
            , 'Domed, Open': 'Outdoor'
            , 'Domed, open': 'Outdoor'
            , 'Heinz Field': 'Outdoor'
            , 'Cloudy': 'Outdoor'
            , 'Retr. Roof - Open': 'Outdoor'
            , 'Retr. Roof Closed': 'Indoor'
            , 'Outdor': 'Outdoor'
            , 'Outside': 'Outdoor'
            , 'Indoor, non-retractable roof': 'Indoor'
            , 'Retr. roof - closed': 'Indoor'
            , 'Indoor, fixed roof ': 'Indoor'
            , 'Indoor, Non-Retractable Dome': 'Indoor'
            , 'Indoor, Fixed Roof': 'Indoor'
            , 'Indoor, fixed roof': 'Indoor'
            , None: 'Outdoor'  # It was verified that all fields with null values are indeed outdoor
        }


    df = df.with_columns(pl.col("Stadium_Type").replace(stadium_dict)) # This uses the dict to assign naming conventions

    return df

# Cleans up the weather data from having a lot of different but similar to a few categories
def weather_cleaner(df):
     import polars as pl
     
     weather_dict = {
            'Clear and warm': 'Clear'
            , 'Mostly Cloudy': 'Cloudy'
            , 'Sunny': 'Clear'
            , 'Clear': 'Clear'
            , 'Cloudy': 'Cloudy'
            , 'Cloudy, fog started developing in 2nd quarter': 'Hazy/Fog'
            , 'Rain': 'Rain'
            , 'Partly Cloudy': 'Cloudy'
            , 'Mostly cloudy': 'Cloudy'
            , 'Cloudy and cold': 'Cloudy'
            , 'Cloudy and Cool': 'Cloudy'
            , 'Rain Chance 40%': 'Rain'
            , 'Controlled Climate': 'Indoor'
            , 'Sunny and warm': 'Clear'
            , 'Partly cloudy': 'Cloudy'
            , 'Clear and Cool': 'Cloudy'
            , 'Clear and cold': 'Cloudy'
            , 'Sunny and cold': 'Clear'
            , 'Indoor': 'Indoor'
            , 'Partly Sunny': 'Clear'
            , 'N/A (Indoors)': 'Indoor'
            , 'Mostly Sunny': 'Clear'
            , 'Indoors': 'Indoor'
            , 'Clear Skies': 'Clear'
            , 'Partly sunny': 'Clear'
            , 'Showers': 'Rain'
            , 'N/A Indoor': 'Indoor'
            , 'Sunny and clear': 'Clear'
            , 'Snow': 'Snow'
            , 'Scattered Showers': 'Rain'
            , 'Party Cloudy': 'Cloudy'
            , 'Clear skies': 'Clear'
            , 'Rain likely, temps in low 40s.': 'Rain'
            , 'Hazy': 'Hazy/Fog'
            , 'Partly Clouidy': 'Cloudy'
            , 'Sunny Skies': 'Clear'
            , 'Overcast': 'Cloudy'
            , 'Cloudy, 50% change of rain': 'Cloudy'
            , 'Fair': 'Clear'
            , 'Light Rain': 'Rain'
            , 'Partly clear': 'Clear'
            , 'Mostly Coudy': 'Cloudy'
            , '10% Chance of Rain': 'Cloudy'
            , 'Cloudy, chance of rain': 'Cloudy'
            , 'Heat Index 95': 'Clear'
            , 'Sunny, highs to upper 80s': 'Clear'
            , 'Sun & clouds': 'Cloudy'
            , 'Heavy lake effect snow': 'Snow'
            , 'Mostly sunny': 'Clear'
            , 'Cloudy, Rain': 'Rain'
            , 'Sunny, Windy': 'Windy'
            , 'Mostly Sunny Skies': 'Clear'
            , 'Rainy': 'Rain'
            , '30% Chance of Rain': 'Rain'
            , 'Cloudy, light snow accumulating 1-3"': 'Snow'
            , 'cloudy': 'Cloudy'
            , 'Clear and Sunny': 'Clear'
            , 'Coudy': 'Cloudy'
            , 'Clear and sunny': 'Clear'
            , 'Clear to Partly Cloudy': 'Clear'
            , 'Cloudy with periods of rain, thunder possible. Winds shifting to WNW, 10-20 mph.': 'Windy'
            , 'Rain shower': 'Rain'
            , 'Cold': 'Clear'
            , 'Partly cloudy, lows to upper 50s.': 'Cloudy'
            , 'Scattered thunderstorms': 'Rain'
            , 'CLEAR': 'Clear'
            , 'Partly CLoudy': 'Cloudy'
            , 'Chance of Showers': 'Rain'
            , 'Snow showers': 'Snow'
            , 'Clear and Cold': 'Clear'
            , 'Cloudy with rain': 'Rain'
            , 'Sunny intervals': 'Clear'
            , 'Clear and cool': 'Clear'
            , 'Cloudy, Humid, Chance of Rain': 'Rain'
            , 'Cloudy and Cold': 'Cloudy'
            , 'Cloudy with patches of fog': 'Hazy/Fog'
            , 'Controlled': 'Indoor'
            , 'Sunny and Clear': 'Clear'
            , 'Cloudy with Possible Stray Showers/Thundershowers': 'Rain'
            , 'Suny': 'Clear'
            , 'T-Storms': 'Rain'
            , 'Sunny and cool': 'Clear'
            , 'Cloudy, steady temps': 'Cloudy'
            , 'Hazy, hot and humid': 'Hazy/Fog'
            , 'Sunny Intervals': 'Clear'
            , 'Partly Cloudy, Chance of Rain 80%': 'Rain'
            , 'Mostly Clear. Gusting ot 14.': 'Windy'
            , 'Mostly CLoudy': 'Cloudy'
            , 'Snow Showers, 3 to 5 inches expected.': 'Snow'
            }





     df = df.with_columns(pl.col("Weather").replace(weather_dict)) # Standardizes the weather to a few main types

     df = df.with_columns(             # Null handling - all null weather conditions for indoor stadiums are filled "indoor"
                pl.when(pl.col("Stadium_Type") == "Indoor")
                .then(pl.col("Weather").fill_null("Indoor"))
                .otherwise(pl.col("Weather"))
                .alias("Weather")
                )
     
     # For the non-indoor games with null values for weather, to maintain the percentage of games that were clear/cloudy, temperature was used as a divider, above and below 70 degrees
     df = df.with_columns(
                pl.when(pl.col("Temperature") > 70)
                .then(pl.col("Weather").fill_null("Clear"))
                .otherwise(pl.col("Weather"))
                .alias("Weather")
                )
     df = df.with_columns(pl.col("Weather").fill_null("Cloudy"))

     return df


# This fixes the issues with introduced nulls following the joins 
def injury_cleaner(quals):
    import polars as pl
    quals = quals.filter(pl.col('Play_Type').is_not_null()) # 0.14% of rows did not have a play type, and ALL of these were non-injury plays, so they were removed

    quals = quals.with_columns(pl.col("Body_Part").fill_null("No_Injury")) # This fills all null from the join with No Injury

    quals = quals.with_columns(
    pl.col(["DM_1", "DM_7", "DM_28", "DM_42"]).fill_null(0)) # This fills the nulls from the Join with 0s, since there were no injuries.

    return quals


# This will standardize the types of Turf for the FieldType to either natural or synthetic

def turf_cleaner(df):
    ''' 
    Changes the many different types of turf listed into either natural or synthetic
    '''
    import polars as pl

    turf_dict = {
        'Grass': 'Natural'
        , 'Field Turf': 'Synthetic'
        , 'Natural Grass': 'Natural'
        , 'grass': 'Natural'
        , 'Artificial': 'Synthetic'
        , 'FieldTurf': 'Synthetic'
        , 'DD GrassMaster': 'Synthetic'
        , 'A-Turf Titan': 'Synthetic'
        , 'UBU Sports Speed S5-M': 'Synthetic'
        , 'UBU Speed Series S5-M': 'Synthetic'
        , 'Artifical': 'Synthetic'
        , 'UBU Speed Series-S5-M': 'Synthetic'
        , 'FieldTurf 360': 'Synthetic'
        , 'Natural grass': 'Natural'
        , 'Field turf': 'Synthetic'
        , 'Natural': 'Natural'
        , 'Natrual Grass': 'Natural'
        , 'Synthetic': 'Synthetic'
        , 'Natural Grass ': 'Natural'
        , 'Naturall Grass': 'Natural'
        , 'FieldTurf360': 'Synthetic'
        , None: 'Natural'  # The only field with null values is Miami Gardens, which has Natural
        }

    
    df = df.with_columns(pl.col("Field_Type").replace(turf_dict))
    return df

def score_splitter(df):
    ''' 
    Splits the string column from Score_Home_Visiting into two numeric columns for each of the scores. It also creates a column that calculates the difference. 
    '''
    import polars as pl

    df = df.with_columns([
        pl.col("Score_Home_Visiting").str.extract(r"(\d+)\s*-\s*(\d+)", 1).cast(pl.Int16).alias("Home_Score")
        , pl.col("Score_Home_Visiting").str.extract(r"(\d+)\s*-\s*(\d+)", 2).cast(pl.Int16).alias("Visiting_Score") # Find difference between scores
        ])

    df = df.with_columns([
        (pl.col("Home_Score") - pl.col("Visiting_Score")).cast(pl.Int16).alias("Score_Difference")
        ])
    
    df = df.drop("Score_Home_Visiting")
    
    return df


# def weight_gainer(df):
#     '''
#     Adds body parameters weight, height, and chest radius per player position
#     '''
#     import polars as pl

#     body_data = {
#     "Position_Code": ["QB", "RB", "FB", "WR", "TE", "T", "G", "C", "DE", "DT", "NT", "LB", "OLB", "MLB", "CB", "S", "K", "P", "SS", "ILB", "FS", "LS", "DB"]
#     , "Position_Name": ["Quarterback", "Running Back", "Fullback", "Wide Receiver", "Tight End", "Tackle", "Guard", "Center", "Defensive End", "Defensive Tackle", "Nose Tackle", "Linebacker", "Outside Linebacker", "Middle Linebacker", "Cornerback", "Safety", "Kicker", "Punter", "Strong Safety", "Inside Linebacker", "Free Safety", "Long Snapper", "Defensive Back"]
#     , "Height_m": [1.91, 1.79, 1.85, 1.88, 1.96, 1.97, 1.90, 1.87, 1.97, 1.92, 1.88, 1.90, 1.90, 1.87, 1.82, 1.84, 1.83, 1.88, 1.84, 1.90, 1.84, 1.88, 1.82]
#     , "Weight_kg": [102.1, 95.3, 111.1, 90.7, 114.6, 140.6, 141.8, 136.1, 120.2, 141.8, 152.0, 110.0, 108.9, 113.4, 87.4, 95.9, 92.08, 97.52, 95.9, 110.0, 95.9, 108.86, 87.4]
#     , "Chest_rad_m": [0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191]
#     }

    