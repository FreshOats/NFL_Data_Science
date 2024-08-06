# Data Handler
# This will load data or write data back to the SQL database

def data_loader(dataset, database='nfl_surface'): # Read in raw data from SQL database
    #Options are 'quals', 'clean_quals', 
    import polars as pl
    import numpy as np
    import sqlalchemy as db
    from sqlalchemy.orm import Session
    from sqlalchemy import create_engine
    import psycopg2

    # Make connection to the database
    from config import db_password
    uri = f"postgresql://postgres:{db_password}@127.0.0.1:5432/{database}"
    del db_password
    pl.set_random_seed(666)
    

    
    valid_datasets = ['clean_quals', 'qualitative', 'tracking', 'injuries', 'plays', 'play_positions', 'ngs_data', 'concussion', 'positions']
    if dataset not in valid_datasets:
        raise ValueError(f"Invalid dataset name '{dataset}'. Valid options are: {valid_datasets}")

    valid_databases = ['nfl_surface', 'nfl_concussion']
    if database not in valid_databases:
        raise ValueError(f"Invalid database name '{database}'. Valid options are: {valid_databases}")

    try:
        if dataset == 'qualitative':
            query = "SELECT * FROM qualitative" 
            df = pl.read_database_uri(query=query, uri=uri)
        elif dataset == 'plays':
            query = "SELECT playkey, position, stadiumtype, fieldtype, temperature, weather, playtype FROM plays"
            df = pl.read_database_uri(query=query, uri=uri)
        elif dataset == 'play_positions':
            query = "SELECT playkey, position FROM plays"
            df = pl.read_database_uri(query=query, uri=uri)
        elif dataset == 'injuries':
            query = "SELECT playkey, bodypart, DM_M1, DM_M7, DM_M28, DM_M42 FROM injuries"
            df = pl.read_database_uri(query=query, uri=uri)
        elif dataset == 'tracking':
            scan = pl.scan_csv("F:/Data/nfl-playing-surface-analytics/PlayerTrackData.csv")
            df = scan.head(10000).collect()
            # df = scan.collect(streaming=True, infer_schema_length=10000)
        
        elif dataset == 'concussion':
            query = "SELECT * FROM descriptive_data"
            df = pl.read_database_uri(query=query, uri=uri)
        elif dataset == 'clean_quals':
            query = "SELECT * FROM clean_data"
            df = pl.read_database_uri(query=query, uri=uri)
        elif dataset == 'ngs_data':
            query = """ SELECT gamekey, playid, gsisid, time, x, y, dis, o, dir, event 
                        FROM ngs_data 
                        WHERE gsisid IS NOT NULL 
                        ORDER BY gamekey
                        LIMIT 10000
                    """
            df = pl.read_database_uri(query=query, uri=uri)
        elif dataset == 'positions':
            query = "SELECT gsisid, position FROM punt_data"
            df = pl.read_database_uri(query=query, uri=uri)
        return df
    
    
    except Exception as e: 
        print(f"An error occurred while loading the dataset '{dataset}': {e}")
        return None



def data_writer(df, database, new_table_name):
    # Write the Cleaned data back to the SQL server, where this database will be retrieved for future analyses
    import sqlalchemy as db
    import pandas as pd
    from sqlalchemy.orm import Session
    from sqlalchemy import create_engine
    import psycopg2
    from config import db_password
    uri = f"postgresql://postgres:{db_password}@127.0.0.1:5432/{database}"
    del db_password

    # Having an issue pushing the polars to the DB, so I need to switch to Pandas to use SQLalchemy
    df_p = df.to_pandas()

    # Write table to database
    engine = create_engine(uri)
    df_p.to_sql(new_table_name, engine, if_exists='replace', index=False)



def data_shrinker(df, verbose=True):
    import polars as pl
    import numpy as np
    """
    Optimize memory usage of a Polars dataframe for both categorical and numeric data.
    """
    start_mem = df.estimated_size("mb")
    if verbose:
        print(f'Memory usage of dataframe is {start_mem:.2f} MB')

    # Create the event enum
    event_enum = create_event_enum()

    for col in df.columns:
        col_type = df[col].dtype

        if col_type in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
            # Handle missing values
            if df[col].null_count() > 0:
                c_min = df[col].min() if df[col].min() is not None else float('nan')
                c_max = df[col].max() if df[col].max() is not None else float('nan')
            else:
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
            if col == "event":  # Check if the column is 'event'
                # Clean the event column by stripping whitespace
                df = df.with_columns(pl.col(col).str.strip_chars().cast(event_enum))  # Cast to Enum
            elif col != "PlayKey" and df[col].n_unique() / len(df) < 0.5:  # If less than 50% unique values
                df = df.with_columns(pl.col(col).cast(pl.Categorical))

    end_mem = df.estimated_size("mb")
    if verbose:
        print(f'Memory usage after optimization is: {end_mem:.2f} MB')
        print(f'Decreased by {100 * (start_mem - end_mem) / start_mem:.1f}%')

    return df



  # This creates an event enum for the Data Shrinker 

def create_event_enum():
    import polars as pl
    """
    Create an Enum for known events.
    """
    return pl.Enum([
        "ball_snap"
      ,  "drop_kick"
      ,  "end_path"
      ,  "extra_point"
      ,  "extra_point_attempt"
      ,  "extra_point_blocked"
      ,  "extra_point_fake"
      ,  "extra_point_missed"
      ,  "fair_catch"
      ,  "field_goal"
      ,  "field_goal_attempt"
      ,  "field_goal_blocked"
      ,  "field_goal_fake"
      ,  "field_goal_missed"
      ,  "field_goal_play"
      ,  "first_contact"
      ,  "free_kick"
      ,  "free_kick_play"
      ,  "fumble"
      ,  "fumble_defense_recovered"
      ,  "fumble_offense_recovered"
      ,  "handoff"
      ,  "huddle_break_offense"
      ,  "huddle_start_offense"
      ,  "kick_received"
      ,  "kick_recovered"
      ,  "kickoff"
      ,  "kickoff_land"
      ,  "kickoff_play"
      ,  "lateral"
      ,  "line_set"
      ,  "man_in_motion"
      ,  "onside_kick"
      ,  "out_of_bounds"
      ,  "pass_arrived"
      ,  "pass_forward"
      ,  "pass_lateral"
      ,  "pass_outcome_caught"
      ,  "pass_outcome_incomplete"
      ,  "pass_outcome_interception"
      ,  "pass_outcome_touchdown"
      ,  "pass_shovel"
      ,  "pass_tipped"
      ,  "penalty_accepted"
      ,  "penalty_declined"
      ,  "penalty_flag"
      ,  "play_action"
      ,  "play_submit"
      ,  "punt"
      ,  "punt_blocked"
      ,  "punt_downed"
      ,  "punt_fake"
      ,  "punt_land"
      ,  "punt_muffed"
      ,  "punt_play"
      ,  "punt_received"
      ,  "qb_kneel"
      ,  "qb_sack"
      ,  "qb_spike"
      ,  "qb_strip_sack"
      ,  "run"
      ,  "run_pass_option"
      ,  "safety"
      ,  "shift"
      ,  "snap_direct"
      ,  "tackle"
      ,  "timeout"
      ,  "timeout_away"
      ,  "timeout_booth_review"
      ,  "timeout_halftime"
      ,  "timeout_home"
      ,  "timeout_injury"
      ,  "timeout_quarter"
      ,  "timeout_tv"
      ,  "touchback"
      ,  "touchdown"
      ,  "two_minute_warning"
      ,  "two_point_conversion"
      ,  "two_point_play"
      ,  "xp_fake"
    ])