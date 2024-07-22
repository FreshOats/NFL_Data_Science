# Cleaning Functions
# This will set up the code for all cleaning functions. Currently, this cleans only the injury data for surface injuries. 
# TO RUN: 
    # from CleaningFunctions import *
    # clean_injuries('quals')


##### Primary Cleaning Functions #####
def clean_injuries(dataset='quals'):
    quals = data_loader(dataset)
    quals = column_capitalizer(quals)
    quals = stadium_cleaner(quals)
    quals = weather_cleaner(quals)
    quals = injury_cleaner(quals)
    data_writer(quals)
    del quals



def data_loader(dataset='clean_quals'): # Read in raw data from SQL database
    #Options are 'quals', 'clean_quals', 
    import polars as pl
    import numpy as np
    import sqlalchemy as db
    from sqlalchemy.orm import Session
    from sqlalchemy import create_engine
    import psycopg2

    # Make connection to the database
    from config import db_password
    uri = f"postgresql://postgres:{db_password}@127.0.0.1:5432/nfl"
    del db_password
    
    valid_datasets = ['clean_quals', 'quals', 'tracking']
    if dataset not in valid_datasets:
        raise ValueError(f"Invalid dataset name '{dataset}'. Valid options are: {valid_datasets}")

    try:
        if dataset == 'clean_quals':
            query = "SELECT * FROM qualitative_clean" 
            df = pl.read_database_uri(query=query, uri=uri)
        elif dataset == 'quals':
            query = "SELECT * FROM qualitative" 
            df = pl.read_database_uri(query=query, uri=uri)
        elif dataset == 'tracking':
            scan = pl.scan_csv("F:/Data/nfl-playing-surface-analytics/PlayerTrackData.csv")
            df = scan.collect(streaming=True, infer_schema_length=10000)
        return df
    except Exception as e: 
        print(f"An error occurred while loading the dataset '{dataset}': {e}")
        return None



def data_writer(quals):
    # Write the Cleaned data back to the SQL server, where this database will be retrieved for future analyses
    import sqlalchemy as db
    import pandas as pd
    from sqlalchemy.orm import Session
    from sqlalchemy import create_engine
    import psycopg2
    from config import db_password
    uri = f"postgresql://postgres:{db_password}@127.0.0.1:5432/nfl"
    del db_password

    # Having an issue pushing the polars to the DB, so I need to switch to Pandas to use SQLalchemy
    quals_p = quals.to_pandas()

    # Write table to database
    engine = create_engine(uri)
    quals_p.to_sql("qualitative_clean", engine, if_exists='replace', index=False)
    

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
    quals = quals.filter(pl.col('PlayType').is_not_null()) # 0.14% of rows did not have a play type, and ALL of these were non-injury plays, so they were removed

    quals = quals.with_columns(pl.col("BodyPart").fill_null("NoInjury")) # This fills all null from the join with No Injury

    quals = quals.with_columns(
    pl.col(["DM_1", "DM_7", "DM_28", "DM_42"]).fill_null(0)) # This fills the nulls from the Join with 0s, since there were no injuries.

    return quals

# Data Shrinker reduces the memory required by reducing the sizes required in each of the columns based on data types
def data_shrinker(df, verbose=True):
    """
    Optimize memory usage of a Polars dataframe for both categorical and numeric data.
    """
    import polars as pl
    import numpy as np
    start_mem = df.estimated_size("mb")
    if verbose:
        print(f'Memory usage of dataframe is {start_mem:.2f} MB')
    
    for col in df.columns:
        col_type = df[col].dtype
        
        if col_type in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
            c_min = df[col].min()
            c_max = df[col].max()
            
            if col_type.is_integer():
                if c_min >= np.iinfo(np.int8).min and c_max <= np.iinfo(np.int8).max:
                    df = df.with_columns(pl.col(col).cast(pl.Int8))
                elif c_min >= np.iinfo(np.int16).min and c_max <= np.iinfo(np.int16).max:
                    df = df.with_columns(pl.col(col).cast(pl.Int16))
                elif c_min >= np.iinfo(np.int32).min and c_max <= np.iinfo(np.int32).max:
                    df = df.with_columns(pl.col(col).cast(pl.Int32))
                else:
                    df = df.with_columns(pl.col(col).cast(pl.Int64))
            else:
                if c_min >= np.finfo(np.float32).min and c_max <= np.finfo(np.float32).max:
                    df = df.with_columns(pl.col(col).cast(pl.Float32))
                else:
                    df = df.with_columns(pl.col(col).cast(pl.Float64))
        
        elif col_type == pl.Utf8:
            if df[col].n_unique() / len(df) < 0.5:  # If less than 50% unique values
                df = df.with_columns(pl.col(col).cast(pl.Categorical))

    end_mem = df.estimated_size("mb")
    if verbose:
        print(f'Memory usage after optimization is: {end_mem:.2f} MB')
        print(f'Decreased by {100 * (start_mem - end_mem) / start_mem:.1f}%')
    
    return df