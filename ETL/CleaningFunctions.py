# Cleaning Functions
# This will set up the code for all cleaning functions. Currently, this cleans only the injury data for surface injuries. 
# TO RUN: 
    # from CleaningFunctions import *
    # quals = clean_injuries('quals')


##### Primary Cleaning Functions #####
def clean_injuries(dataset='quals'):
    quals = data_loader(dataset)
    quals = column_capitalizer(quals)
    quals = stadium_cleaner(quals)
    quals = weather_cleaner(quals)
    quals = injury_cleaner(quals)

    return quals




def data_loader(dataset):
    import polars as pl
    import sqlalchemy as db
    from sqlalchemy.orm import Session
    from sqlalchemy import create_engine
    import psycopg2

    # Make connection to the database
    from config import db_password
    uri = f"postgresql://postgres:{db_password}@127.0.0.1:5432/nfl"
    del db_password
    
    if dataset == 'quals':
        # Select the qualitative table from SQL 
        query = "SELECT * FROM qualitative"
    

    df = pl.read_database_uri(query=query, uri=uri)
    return df



######################################################################################
# Changes the all lower-case to Capitalized PascalCase column headers 
def column_capitalizer(quals):          
    columns = {
        'playkey': "PlayKey"
        , 'rosterposition': 'Position'
        , 'stadiumtype': 'StadiumType'
        , 'fieldtype': 'FieldType'
        , 'temperature': 'Temperature'
        , 'weather': 'Weather'
        , 'playtype': 'PlayType'
        , 'bodypart': 'BodyPart'
        , 'dm_m1': 'DM_1'
        , 'dm_m7': 'DM_7'
        , 'dm_m28': 'DM_28'
        , 'dm_m42': 'DM_42'
    } 
    quals = quals.rename(columns)
    return quals

# This changes stadiums to either Indoor or Outdoor per game records - some of the dome stadiums have a roof that can open, if open the game is considered outdoor.
def stadium_cleaner(quals):
    import polars as pl        
    stadium_dict = {'Outdoor': 'Outdoor',
        'Indoors': 'Indoor',
        'Oudoor': 'Outdoor',
        'Outdoors': 'Outdoor',
        'Open': 'Outdoor',
        'Closed Dome': 'Indoor',
        'Domed, closed': 'Indoor',
        'Dome': 'Indoor',
        'Indoor': 'Indoor',
        'Domed': 'Indoor',
        'Retr. Roof-Closed': 'Indoor',
        'Outdoor Retr Roof-Open': 'Outdoor',
        'Retractable Roof': 'Indoor',
        'Ourdoor': 'Outdoor',
        'Indoor, Roof Closed': 'Indoor',
        'Retr. Roof - Closed': 'Indoor',
        'Bowl': 'Outdoor',
        'Outddors': 'Outdoor',
        'Retr. Roof-Open': 'Outdoor',
        'Dome, closed': 'Indoor',
        'Indoor, Open Roof': 'Outdoor',
        'Domed, Open': 'Outdoor',
        'Domed, open': 'Outdoor',
        'Heinz Field': 'Outdoor',
        'Cloudy': 'Outdoor',
        'Retr. Roof - Open': 'Outdoor',
        'Retr. Roof Closed': 'Indoor',
        'Outdor': 'Outdoor',
        'Outside': 'Outdoor'}
    
    quals = quals.with_columns(pl.col("StadiumType").fill_null("Outdoor")) # Since most stadiums are outdoor and the percentage of games played indoor is already met by the known indoor games those seasons, all unknown games were set to outdoor
    quals = quals.with_columns(pl.col("StadiumType").replace(stadium_dict)) # This uses the dict to assign naming conventions

    return quals

# Cleans up the weather data from having a lot of different but similar to a few categories
def weather_cleaner(quals):
     import polars as pl
     weather_dict = {'Clear and warm': 'Clear',
                'Mostly Cloudy': 'Cloudy',
                'Sunny': 'Clear',
                'Clear': 'Clear',
                'Cloudy': 'Cloudy',
                'Cloudy, fog started developing in 2nd quarter': 'Hazy/Fog',
                'Rain': 'Rain',
                'Partly Cloudy': 'Cloudy',
                'Mostly cloudy': 'Cloudy',
                'Cloudy and cold': 'Cloudy',
                'Cloudy and Cool': 'Cloudy',
                'Rain Chance 40%': 'Rain',
                'Controlled Climate': 'Indoor',
                'Sunny and warm': 'Clear',
                'Partly cloudy': 'Cloudy',
                'Clear and Cool': 'Cloudy',
                'Clear and cold': 'Cloudy',
                'Sunny and cold': 'Clear',
                'Indoor': 'Indoor',
                'Partly Sunny': 'Clear',
                'N/A (Indoors)': 'Indoor',
                'Mostly Sunny': 'Clear',
                'Indoors': 'Indoor',
                'Clear Skies': 'Clear',
                'Partly sunny': 'Clear',
                'Showers': 'Rain',
                'N/A Indoor': 'Indoor',
                'Sunny and clear': 'Clear',
                'Snow': 'Snow',
                'Scattered Showers': 'Rain',
                'Party Cloudy': 'Cloudy',
                'Clear skies': 'Clear',
                'Rain likely, temps in low 40s.': 'Rain',
                'Hazy': 'Hazy/Fog',
                'Partly Clouidy': 'Cloudy',
                'Sunny Skies': 'Clear',
                'Overcast': 'Cloudy',
                'Cloudy, 50% change of rain': 'Cloudy',
                'Fair': 'Clear',
                'Light Rain': 'Rain',
                'Partly clear': 'Clear',
                'Mostly Coudy': 'Cloudy',
                '10% Chance of Rain': 'Cloudy',
                'Cloudy, chance of rain': 'Cloudy',
                'Heat Index 95': 'Clear',
                'Sunny, highs to upper 80s': 'Clear',
                'Sun & clouds': 'Cloudy',
                'Heavy lake effect snow': 'Snow',
                'Mostly sunny': 'Clear',
                'Cloudy, Rain': 'Rain',
                'Sunny, Windy': 'Windy',
                'Mostly Sunny Skies': 'Clear',
                'Rainy': 'Rain',
                '30% Chance of Rain': 'Rain',
                'Cloudy, light snow accumulating 1-3"': 'Snow',
                'cloudy': 'Cloudy',
                'Clear and Sunny': 'Clear',
                'Coudy': 'Cloudy',
                'Clear and sunny': 'Clear',
                'Clear to Partly Cloudy': 'Clear',
                'Cloudy with periods of rain, thunder possible. Winds shifting to WNW, 10-20 mph.': 'Windy',
                'Rain shower': 'Rain',
                'Cold': 'Clear'}
     
     quals = quals.with_columns(pl.col("Weather").replace(weather_dict)) # Standardizes the weather to a few main types

     quals = quals.with_columns(             # Null handling - all null weather conditions for indoor stadiums are filled "indoor"
                pl.when(pl.col("StadiumType") == "Indoor")
                .then(pl.col("Weather").fill_null("Indoor"))
                .otherwise(pl.col("Weather"))
                .alias("Weather")
                )
     
     # For the non-indoor games with null values for weather, to maintain the percentage of games that were clear/cloudy, temperature was used as a divider, above and below 70 degrees
     quals = quals.with_columns(
                pl.when(pl.col("Temperature") > 70)
                .then(pl.col("Weather").fill_null("Clear"))
                .otherwise(pl.col("Weather"))
                .alias("Weather")
                )
     quals = quals.with_columns(pl.col("Weather").fill_null("Cloudy"))

     return quals


# This fixes the issues with introduced nulls following the joins 
def injury_cleaner(quals):
    import polars as pl
    quals = quals.with_columns(pl.col('PlayType').is_not_null()) # 0.14% of rows did not have a play type, and ALL of these were non-injury plays, so they were removed

    quals = quals.with_columns(pl.col("BodyPart").fill_null("NoInjury")) # This fills all null from the join with No Injury

    quals = quals.with_columns(
    pl.col(["DM_1", "DM_7", "DM_28", "DM_42"]).fill_null(0)) # This fills the nulls from the Join with 0s, since there were no injuries.

    return quals