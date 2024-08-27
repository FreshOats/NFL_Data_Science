# Joins and Cleans the Qualitative Data from the Injury and Concussion Data

##### Primary Injury Cleaning Function #####
def clean_injury_qual():
    """
    Applies data cleaning to surface injury data and writes to 'qualitative_injuries' as a csv file 
    """
    from DataHandler import parquet_writer, data_shrinker
    import os 

    analysis = "injury"
    injury_qual_path = "F:/Data/Processing_data/QualInjuries.parquet"

    df = table_joiner(analysis) 
    df = injury_interpolator(df, analysis)
    df = stadium_cleaner(df)
    df = play_cleaner(df)
    df = weather_cleaner(df)
    df, schema = data_shrinker(df)
    df.write_parquet(injury_qual_path)

    print('Injuries have been cleaned and dressed.')
    # return df


##### Primary Concussion Cleaning Function #####
def clean_concussions(): 
    """
    Applies data cleaning to surface injury data and writes to 'qualitative_injuries' as a csv file 
    """
    from DataHandler import parquet_writer, data_shrinker

    analysis = "concussion"
    concussion_qual_path = "F:/Data/Processing_data/QualitativeConcussions.parquet"
    
    df = table_joiner(analysis)
    df = injury_interpolator(df, analysis)
    df = stadium_cleaner(df)
    df = weather_cleaner(df)
    df = turf_cleaner(df)
    df = cancellation_cleaner(df)
    df = score_splitter(df)
    df, schema = data_shrinker(df)
    df.write_parquet(concussion_qual_path)
    del df

    print('Concussions have been assessed and cleared for play.')
    # return df


#############################################################################

def table_joiner(analysis):
    """
    Joins the two non-ngs tables in the injury data, and joins 5 non-ngs tables from the concussion dataset. 
    """
    import polars as pl # type: ignore
    from DataHandler import data_loader

    valid_analyses = ['injury', 'concussion']
    if analysis not in valid_analyses: 
        raise ValueError(f"Invalid dataset name '{analysis}'. Valid options are: {valid_analyses}")


    try: 
        #Injury Data Loader
        if analysis == 'injury':
            plays = data_loader('plays')
            injuries = data_loader('injuries')

            df = (
                plays.join(injuries, on="PlayKey", how='left')
                .select([
                    pl.col("PlayKey").cast(pl.Utf8)
                    , pl.col("Position").cast(pl.Utf8)
                    , pl.col("StadiumType").cast(pl.Utf8)
                    , pl.col("FieldType").cast(pl.Utf8)
                    , pl.col("Temperature").cast(pl.Int16)
                    , pl.col("Weather").cast(pl.Utf8)
                    , pl.col("PlayType").cast(pl.Utf8)
                    , pl.col("BodyPart").cast(pl.Utf8)
                    , pl.col("DM_M1").cast(pl.Int8)
                    , pl.col("DM_M7").cast(pl.Int8)
                    , pl.col("DM_M28").cast(pl.Int8)
                    , pl.col("DM_M42").cast(pl.Int8)
                ])
            )

        # Concussion Data Loader
        elif analysis == 'concussion':
            role_data = data_loader('role_data')
            punt_data = data_loader('punt_data')
            play_information = data_loader('play_information')
            game_data = data_loader('game_data')
            video_review = data_loader('video_review')


            df = (
                role_data
                .join(
                    punt_data
                    , left_on="GSISID"
                    , right_on="GSISID"
                    , how="left"
                    , suffix="_punt"
                )
                .join(
                    play_information
                    , left_on=["GameKey", "PlayID"]
                    , right_on=["GameKey", "PlayID"]
                    , how="left"
                    , suffix="_play"
                )
                .join(
                    game_data
                    , left_on="GameKey"
                    , right_on="GameKey"
                    , how="left"
                    , suffix="_game"
                )
                .join(
                    video_review
                    , left_on=["GameKey", "PlayID", "GSISID"]
                    , right_on=["GameKey", "PlayID", "GSISID"]
                    , how="left"
                    , suffix="_video"
                )
                .with_columns([
                    pl.concat_str([
                        pl.col("GSISID").cast(pl.Utf8)
                        , pl.lit("-")
                        , pl.col("GameKey").cast(pl.Utf8)
                        , pl.lit("-")
                        , pl.col("PlayID").cast(pl.Utf8)
                    ]).alias("PlayKey"),
                    pl.concat_str([
                        pl.col("Primary_Partner_GSISID").cast(pl.Utf8)
                        , pl.lit("-")
                        , pl.col("GameKey").cast(pl.Utf8)
                        , pl.lit("-")
                        , pl.col("PlayID").cast(pl.Utf8)
                    ]).alias("OpponentKey")
                    , pl.when(pl.col("Primary_Partner_GSISID") == "Unclear")
                        .then(pl.lit("00000"))
                        .otherwise(pl.col("Primary_Partner_GSISID"))
                        .cast(pl.Int64)
                        .alias("Primary_Partner_GSISID")
                ])
                .select([
                    "PlayKey"
                    , "GSISID"
                    , "GameKey"
                    , "PlayID"
                    , "Position"
                    , 'Number'
                    , "Role"
                    , "Game_Date"
                    , "YardLine"
                    , "Quarter"
                    , "Play_Type"
                    , "Poss_Team"
                    , "Score_Home_Visiting"
                    , "Game_Site"
                    , "Start_Time"
                    , "HomeTeamCode"
                    , "VisitTeamCode"
                    , "StadiumType"
                    , "Turf"
                    , "GameWeather"
                    , "Temperature"
                    , "Player_Activity_Derived"
                    , "Primary_Impact_Type"
                    , "Primary_Partner_Activity_Derived"
                    , "Primary_Partner_GSISID"
                    , "OpponentKey"
                ])
                .unique()
            )



        print(f"Tables are holding hands. How cute.")
        return df
    
    except Exception as e: 
        print(f"An error occurred while processing the '{analysis}' analysis: {e}.")
        return None
    

def injury_interpolator(df, analysis): 
    """
    Creates two new columns, IsInjured and IsSevere, where any injury sets 
    IsInjured to 1, and any injury over 28 days provides a 1 in IsSevere
    """
    import polars as pl # type: ignore

    if analysis == 'injury':
        df = df.with_columns([ 
            pl.when(pl.col("DM_M1") == 1).then(1).otherwise(0).cast(pl.Int8).alias("IsInjured")
            , pl.when(pl.col("DM_M28") == 1).then(1).otherwise(0).cast(pl.Int8).alias("IsSevere")
            ])
        
        df = df.filter(pl.col('PlayType').is_not_null()) # 0.14% of rows did not have a play type, and ALL of these were non-injury plays, so they were removed
        df = df.with_columns([ 
                pl.col("BodyPart").fill_null("No_Injury")
                , pl.col(["DM_M1", "DM_M7", "DM_M28", "DM_M42"]).fill_null(0)
                ])

    elif analysis == 'concussion':
        df = df.with_columns([ 
            pl.when(pl.col("Primary_Impact_Type").is_not_null()).then(1).otherwise(0).alias("IsInjured")
            , pl.col("Player_Activity_Derived").fill_null("No_Injury")
            , pl.col("Primary_Impact_Type").fill_null("No_Injury")
            , pl.col("Primary_Partner_Activity_Derived").fill_null("No_Injury")
            , pl.col("Primary_Partner_GSISID").fill_null(00000)
            , pl.col("OpponentKey").fill_null("None")
            ])
        
    print("Injury columns have been added.")
    return df


def stadium_cleaner(df):
    """
    Noramlizes all stadium types to be either indoor or outdoor per game records. Some of the dome 
    stadiums were listed as open or closed for different games, and these were accounted for.
    All games with dates were checked to ensure null values were indeed outdoor games. 
    """
    import polars as pl  # type: ignore

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
        , 'outdoor': 'Outdoor'
        , 'Outdoors ': 'Outdoor'
        , 'Indoor, non-retractable roof': 'Indoor'
        , 'Retr. roof - closed': 'Indoor'
        , 'Indoor, fixed roof ': 'Indoor'
        , 'Indoor, Non-Retractable Dome': 'Indoor'
        , 'Indoor, Fixed Roof': 'Indoor'
        , 'Indoor, fixed roof': 'Indoor'
        , 'Indoors (Domed)': 'Indoor'
        , None: 'Outdoor'
        }


    df = df.with_columns(pl.col("StadiumType").replace(stadium_dict)) # This uses the dict to assign naming conventions

    print(f"Someone managed to clean up those stadiums!")
    return df


def play_cleaner(df):
    """
    Reduces the number of play types listed as Kickoff or Punt plays to just those two types. 
    """
    import polars as pl  # type: ignore

    play_dict = {
        'Kickoff Not Returned': 'Kickoff'
        , 'Kickoff Returned': 'Kickoff'
        , 'Punt Not Returned': 'Punt'
        , 'Punt Returned': 'Punt'
        , '0': 'Unknown'
        }

    df = df.with_columns(pl.col("PlayType").replace(play_dict)) # This uses the dict to assign naming conventions

    print(f"Plays have been set!")
    return df


def weather_cleaner(df):
     """
     Uses mapping to limit the number of different weather groupings. 
     """
     import polars as pl # type: ignore

     # If using the concussion dataset, rename the GameWeather column to Weather
     if "GameWeather" in df.columns:
       df = df.rename({"GameWeather": "Weather"})

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
                pl.when(pl.col("StadiumType") == "Indoor")
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

     print(f"Looks like the weather has been cleared up.")
     return df


def turf_cleaner(df):
    import polars as pl # type: ignore

    # df = df.rename({"Turf": "FieldType"})

    turf_dict = {
        'Grass': 'Natural',
        'Field Turf': 'Synthetic', 
        'Natural Grass': 'Natural',
        'grass': 'Natural',
        'Artificial': 'Synthetic',
        'FieldTurf': 'Synthetic',
        'DD GrassMaster': 'Synthetic',
        'A-Turf Titan': 'Synthetic',
        'UBU Sports Speed S5-M': 'Synthetic',
        'UBU Speed Series S5-M': 'Synthetic',
        'Artifical': 'Synthetic',
        'UBU Speed Series-S5-M': 'Synthetic',
        'FieldTurf 360': 'Synthetic',
        'Natural grass': 'Natural',
        'Field turf': 'Synthetic',
        'Natural': 'Natural',
        'Natrual Grass': 'Natural',
        'Synthetic': 'Synthetic',
        'Natural Grass ': 'Natural',
        'Naturall Grass': 'Natural',
        'FieldTurf360': 'Synthetic',
        None: 'Natural' # The only field with null values is Miami Gardens, which has Natural
        }
    
    df = df.with_columns(pl.col("FieldType").replace(turf_dict))
    return df


def cancellation_cleaner(df):
    """
    There are 44 rows that have no Game_Date, which correlate with games that were canceled. 
    This was verified by looking at the lineup of hometeam and visit team during those seasons.

    Additionally, there are 10 rows that lack positions, numbers, or even an identifier to which 
    team the players were on, totalling 4 undocumented players. These rows will be removed as well.   
    """
    import polars as pl # type: ignore

    df = df.filter(pl.col("Game_Date").is_not_null())
    df = df.filter(pl.col("Position").is_not_null()) # Removes 4 players and 10 rows where the position and player number were not recorded, none associated with injuries

    return df
    

def score_splitter(df):
    ''' 
    Splits the string column from Score_Home_Visiting into two numeric columns for each of the scores. It also creates a column that calculates the difference. 
    '''
    import polars as pl # type: ignore

    df = df.with_columns([
        pl.col("Score_Home_Visiting").str.extract(r"(\d+)\s*-\s*(\d+)", 1).cast(pl.Int16).alias("Home_Score")
        , pl.col("Score_Home_Visiting").str.extract(r"(\d+)\s*-\s*(\d+)", 2).cast(pl.Int16).alias("Visiting_Score") # Find difference between scores
        ])

    df = df.with_columns([
        (pl.col("Home_Score") - pl.col("Visiting_Score")).cast(pl.Int16).alias("Score_Difference")
        ])
    
    df = df.drop("Score_Home_Visiting")
    
    print(f"The scores have been fixed. Just not how Pete Rose would fix them.")
    return df