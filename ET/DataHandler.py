# Data Handling for Injury and Concussion Data
def data_loader(dataset): 
    """
    Accepts the desired dataset string and opens the file as either a polars dataframe
    or a lazyframe. Lazyloading is used for the larger tracking datasets. 
    """
    import polars as pl  # type: ignore
    import numpy as np # type: ignore

    valid_datasets = ['plays', 'injuries', 'role_data', 'punt_data', 'play_information', 'game_data', 'video_review', 'qualitative_injuries', 'qualitative_concussions']
    if dataset not in valid_datasets: 
        raise ValueError(f"Invalid dataset name '{dataset}'. Valid options are: {valid_datasets}")

    try:
        # Injury Datasets
        if dataset == 'plays':
            PlayList_path = "F:/Data/nfl-playing-surface-analytics/PlayList.csv"
            df = pl.read_csv(PlayList_path)
        elif dataset == 'injuries':
            InjuryRecord_path = "F:/Data/nfl-playing-surface-analytics/InjuryRecord.csv"
            df = pl.read_csv(InjuryRecord_path)

        # Concussion Datasets
        elif dataset == 'role_data':
            play_player_role_data_path = "F:/Data/NFL-Punt-Analytics-Competition/play_player_role_data.csv"
            df = pl.read_csv(play_player_role_data_path)
        elif dataset == 'punt_data':
            player_punt_data_path = "F:/Data/NFL-Punt-Analytics-Competition/player_punt_data.csv"
            df = pl.read_csv(player_punt_data_path)
        elif dataset == 'play_information':
            play_information_path = "F:/Data/NFL-Punt-Analytics-Competition/play_information.csv"
            df = pl.read_csv(play_information_path)
        elif dataset == 'game_data':
            game_data_path = "F:/Data/NFL-Punt-Analytics-Competition/game_data.csv"
            df = pl.read_csv(game_data_path)
        elif dataset == 'video_review':
            video_review_path = "F:/Data/NFL-Punt-Analytics-Competition/video_review.csv"
            df = pl.read_csv(video_review_path)

        # Qualitative Datasets
        elif dataset == 'qualitative_injuries':
            qi_path = "F:/Data/Clean_Data/qualitative_injuries.parquet"
            df = pl.read_parquet(qi_path)
        elif dataset == 'qualitative_concussions':
            qc_path = "F:/Data/Clean_Data/qualitative_concussions.parquet"
            df = pl.read_parquet(qc_path)

        # Tracking Datasets
        elif dataset == 'tracking':
            tracking_path = "F:/Data/nfl-playing-surface-analytics/PlayerTrackData.csv"
            df = pl.scan_csv(tracking_path)
        elif dataset == 'ngs':
            df

        return df
    
    except Exception as e: 
        print(f"An error occurred while loading the dataset '{dataset}': {e}")
        return None
    

def csv_writer(df, new_file_name):
    """
    Write table to local file as temporary until all cleaning and transformation is done.
    """
    import polars as pl # type: ignore
    import os
       
    path = 'F:/Data/Clean_Data'
    full_path = f"{path}/{new_file_name}.csv"
    
    # Check if file exists
    if os.path.exists(full_path):
        os.remove(full_path)
    
    # Write new file
    df.write_csv(full_path)
    print(f"New file has been written to {full_path}")

def parquet_writer(df, new_file_name):
    """
    Write table to local file as Parquet format until all cleaning and transformation is done.
    """
    import polars as pl # type: ignore
    import os
       
    path = 'F:/Data/Clean_Data'
    full_path = f"{path}/{new_file_name}.parquet"
    
    # Check if file exists
    if os.path.exists(full_path):
        os.remove(full_path)
    
    # Write new file
    df.write_parquet(full_path, compression="snappy")
    print(f"New Parquet file has been written to {full_path}")


def data_shrinker(df, verbose=True):
    """
    Optimize memory usage of a Polars dataframe for both categorical and numeric data.
    """
    import polars as pl # type: ignore
    import numpy as np # type: ignore

    # Enable string cache to ensure consistent encoding
    pl.enable_string_cache()

    start_mem = df.estimated_size("mb")
    if verbose:
        print(f'Memory usage of dataframe is {start_mem:.2f} MB')

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
            if col != "PlayKey" and df[col].n_unique() / len(df) < 0.5:  # If less than 50% unique values
                # Create an Enum type for the column
                enum_type = pl.Enum(df[col].unique())
                df = df.with_columns(pl.col(col).cast(enum_type))

    end_mem = df.estimated_size("mb")
    if verbose:
        print(f'Memory usage after optimization is: {end_mem:.2f} MB')
        print(f'Decreased by {100 * (start_mem - end_mem) / start_mem:.1f}%')

    return df
