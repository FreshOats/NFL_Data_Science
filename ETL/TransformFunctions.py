# Transform Functions
# This imports the clean data from the database and reduces the memory usage prior to processing the data. 




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

    #### I want to introduce a calculation for the velocity vectors using parametric equations for X and Y along with the time. Doing this, I can ignore the angles provided for the direction,  