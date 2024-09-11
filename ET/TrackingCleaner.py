# Tracking Functions
####################
# Concatenate All Tracking Data

def track_all_quant(injury_record_path, injury_tracking_path, concussion_tracking_path, output_path):
    """ 
    Concatenates the two tracking dataframes for Viz creation with all injuries. 
    This additionally adds an InjuryType column for concussions and other. 
    """
    import polars as pl #type: ignore
    pl.enable_string_cache()

    import time

    #Write 
    # output_path = "F:/Data/Processing_data/All_Tracking.parquet"

    start_time = time.time()    
    body_part = pl.read_csv(injury_record_path).select(['PlayKey', 'BodyPart']).filter(pl.col("PlayKey").is_not_null())
    concussions = pl.read_parquet(concussion_tracking_path)
    injuries = pl.read_parquet(injury_tracking_path)


    concussions = concussions.with_columns(
        pl.when(pl.col("InjuryKey") == pl.col("PlayKey"))
            .then(pl.lit("Concussion"))
            .otherwise(pl.lit("No Injury"))
            .alias("InjuryType")
            ).drop("GSISID")

    injuries = injuries.join(
        body_part
        , on='PlayKey'
        , how='left' 
        ).rename({"BodyPart": "InjuryType"}
        ).with_columns(
            pl.col("PlayKey").alias('InjuryKey')
        )
    

    common_columns = ["PlayKey"
                    , "time"
                    , "x"
                    , "y"
                    , "dir"
                    , "o"
                    , "Angle_Diff"
                    , "Displacement"
                    , "Speed"
                    # , "vx"
                    # , "vy"
                    , "omega_dir"
                    , "omega_o"
                    , "omega_diff"
                    , "Position"
                    # , "Height_m"
                    # , "Weight_kg"
                    # , "Chest_rad_m"
                    # , "px"
                    # , "py"
                    # , "moment"
                    # , "moment_upper"
                    , "p_magnitude"
                    , "L_dir"
                    , "L_diff"
                    # , "Jx"
                    # , "Jy"
                    , "J_magnitude"
                    , "torque"
                    , "torque_internal"
                    , "InjuryType"
                    , "InjuryKey"
                    ]

    additional_columns = ["PlayerActivity"
                        , "ImpactType"
                        , "OpponentKey"
                        ]

    concussions = concussions.select(common_columns + additional_columns)

    for col in additional_columns:
        if col not in injuries.columns:
            injuries = injuries.with_columns(pl.lit(None).alias(col))

    injuries = injuries.select(common_columns + additional_columns)

    combined_df = pl.concat([concussions, injuries])
    combined_df = combined_df.sort(["PlayKey", "time"])



    combined_df.write_parquet(output_path)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Concatenated all Injury and Concussion tracking data to {output_path} \n Execution time: {execution_time} seconds.")


#############################################################################
#### Concussion Processing #######


def process_ngs_files(source_dir, output_dir):
    """
    Opens each of the NGS files and applies the concussion transformation funcions, appending
    the results to a single combined dataframe
    """
    import polars as pl # type: ignore
    import os
    import time

    start_time = time.time()
    # List all NGS files in the directory
    ngs_files = [f for f in os.listdir(source_dir) if f.startswith('NGS-')]

    
    # Process each file and store the results
    processed_dfs = []
    for file in ngs_files:
        file_path = os.path.join(source_dir, file)
        processed_df = transform_concussion_tracking(file_path)
        processed_dfs.append(processed_df)

    # Concatenate all processed DataFrames
    combined_df = pl.concat(processed_dfs)

    # Save the combined DataFrame to a CSV file
    output_path = os.path.join(output_dir, 'TrackingConcussions.parquet')
    combined_df.write_parquet(output_path)

    end_time = time.time()
    execution_time = (end_time - start_time)/60 
    print(f"Combined processed data saved to: {output_path} \n Execution time: {execution_time} minutes.")
    return combined_df



def transform_concussion_tracking(file_path):
    """
    Main Transformation function for the concussion data. 
    """
    import polars as pl # type: ignore
    pl.enable_string_cache()

    # Read and filter for injured pairs
    df = pl.read_csv(file_path, truncate_ragged_lines=True, ignore_errors=True)
    df = column_corrector(df)
    df = create_opponent_plays(df, 'tracking')
    df = reduce_float_precision(df)

    # Mechanics Processing
    df = (df
          .pipe(angle_corrector)
          .pipe(velocity_calculator)
          .pipe(body_builder_conc)
          .pipe(impulse_calculator)
          )
    
    return df


def transform_concussion_summary_files(file_path):
    """
    Main Transformation function for the concussion data. 
    """
    import polars as pl # type: ignore
    pl.enable_string_cache()

    # Read and filter for injured pairs
    df = pl.read_csv(file_path, truncate_ragged_lines=True, ignore_errors=True)
    df = column_corrector(df)
    df = create_opponent_plays(df, 'summary')
    df = reduce_float_precision(df)

    # Mechanics Processing
    df = (df
          .pipe(angle_corrector)
          .pipe(velocity_calculator)
          .pipe(body_builder_conc)
          .pipe(impulse_calculator)
          .pipe(summary_calculator)
          )
    
    return df



def collect_concussion_summaries(group_dir):
    import polars as pl #type: ignore
    import os

    # Initialize an empty list for the dataframes
    summary_dfs = []

    # Iterate through files in the directory
    for file in os.listdir(group_dir):
        if file.startswith("NGS-"):
            file_path = os.path.join(group_dir, file)
            
            # Read the Parquet file
            df = pl.read_parquet(file_path)
            
            # Append to the list of summary dataframes
            summary_dfs.append(df)

    # Concatenate all summary dataframes
    summary_df = pl.concat(summary_dfs)  

    return summary_df


def process_concussion_summary():
    """
    Extract and Transform the tracking data for the concussion dataset per file.  
    These datasets will then be used to create the summary data and then filter 
    for the injuries using the video review information to get only the concussion plays for
    the player and their opponent, which will be a different playkey based on GSISID, but the latter
    portion will match. 
    """
    import os
    import time
    import polars as pl # type: ignore
    pl.enable_string_cache()
    
    start_time = time.time()
    

    source_dir = "F:/Data/NFL-Punt-Analytics-Competition/"
    concussion_group_dir = "F:/Data/Clean_Data/concussion_output/"
    os.makedirs(concussion_group_dir, exist_ok=True)


    for file in os.listdir(source_dir):
        if file.startswith("NGS-"):
            file_path = os.path.join(source_dir, file)
            
            df = transform_concussion_summary_files(file_path)
            
            output_file_path = os.path.join(concussion_group_dir, file.replace(".csv", ".parquet"))
            df.write_parquet(output_file_path)

            print(f"Processed and saved: {output_file_path}")

    end_time = time.time()
    execution_time = (end_time - start_time)/60
    print(f"For fuck's sake that took {execution_time} minutes. Finally done processing and saving the concussion files.")
    print(f"Continuing to concatenate the summaries. This shouldn't take as long")

    review_df = collect_concussion_summaries(concussion_group_dir)

    return review_df


################################################################
#### Injury Processing #####

def transform_and_save_injury_data(injury_tracking_path, injury_output_dir, main_dir):
    """
    Full transform process for the surface injury data. 
    """
    import time
    start_time = time.time()
    optimized_df = optimize_lazyframe(injury_tracking_path)
    process_file(optimized_df, injury_output_dir, group_size=15000)
    tracking_injuries(injury_output_dir, main_dir)
    end_time = time.time()
    execution_time = (end_time - start_time)/60
    print(f"Execution time: {execution_time} minutes.")
    


def create_initial_lazyframe(injury_tracking_path):
    import polars as pl # type: ignore
    return pl.scan_csv(injury_tracking_path
                       , truncate_ragged_lines=True
                       , infer_schema_length=10000
                       , ignore_errors=True).drop(['event', 's', 'dis'])


def optimize_lazyframe(injury_tracking_path): # Changed name to optimize_injury_data()
    """
    This function opens the original tracking csv, reduces size by casting to less expensive data types, 
    and then saves the optimized tracking dataset to file. It does not return a dataframe, so any use will 
    be expected to call from the saved optimized file. 
    """
    import polars as pl # type: ignore
    from DataHandler import data_shrinker

    df = create_initial_lazyframe(injury_tracking_path).collect(streaming=True)
    optimized_df, optimized_schema = data_shrinker(df)
    
    # Cast the DataFrame columns to the types specified in optimized_schema
    for column, dtype in optimized_schema.items():
        optimized_df = optimized_df.with_columns(pl.col(column).cast(dtype))

    return optimized_df


def process_file(optimized_df, output_dir, group_size):
    """"
    Breaks up the file into smaller chunks, making sure not to break up PlayKeys
    since the times for max and mean must be maintainted per PlayKey
    """
    import math
    import polars as pl # type: ignore
    
    # Get unique PlayKey values
    unique_playkeys = optimized_df["PlayKey"].unique().to_list()
    
    # Calculate the number of groups
    num_groups = math.ceil(len(unique_playkeys) / group_size)
    
    # Process each group of PlayKeys
    for i in range(num_groups):
        start_idx = i * group_size
        end_idx = min((i + 1) * group_size, len(unique_playkeys))
        playkey_group = unique_playkeys[start_idx:end_idx]
        process_and_save_playkey_group(optimized_df, playkey_group, output_dir, i + 1)

    print("Processing complete.")


def process_and_save_playkey_group(lazy_df, playkeys, output_dir, group_number):
    """
    Performs the transformation to add mechanics data to the tracking data
    This is applied to each individual group, saving as a parquet file. 
    """
    import os
    import polars as pl # type: ignore
    # Filter the lazy DataFrame for the specific PlayKeys
    group_df = lazy_df.filter(pl.col("PlayKey").is_in(playkeys))
    
    # Processing
    group_df = (group_df
                .pipe(angle_corrector)
                .pipe(velocity_calculator)
                .pipe(body_builder_inj)
                .pipe(impulse_calculator))

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save the DataFrame for this group as a Parquet file
    output_file = os.path.join(output_dir, f"group_{group_number}.parquet")
    group_df.write_parquet(output_file)
    print(f"Saved data for PlayKey group: {group_number}")



def tracking_injuries(group_dir, main_dir):
    """
    Main transformation function for the injury data. 
    """
    import os
    import time
    import polars as pl # type: ignore
    pl.enable_string_cache()

    # Read in the PlayKeys from the injury file to isolate PlayKeys associated with injury paths
    InjuryRecord_path = "F:/Data/nfl-playing-surface-analytics/InjuryRecord.csv"
    injuryPlayKeys = pl.read_csv(InjuryRecord_path)
    PlayKeys = injuryPlayKeys.select("PlayKey").unique().with_columns([
            pl.col("PlayKey").cast(pl.Utf8)
            ])

    # Initialize an empty list to store dataframes
    filtered_dfs = []

    # Iterate through files in the directory
    for file in os.listdir(group_dir):
        if file.startswith("group_"):
            file_path = os.path.join(group_dir, file)
            
            # Read the Parquet file
            df = pl.read_parquet(file_path)
            
            # Ensure PlayKey is of type Utf8
            df = df.with_columns(pl.col("PlayKey").cast(pl.Utf8))
            
            # Inner join with unique_gsisid to filter rows
            filtered_df = df.join(PlayKeys, on="PlayKey", how="inner")
            
            # Append to the list of filtered dataframes
            filtered_dfs.append(filtered_df)

    # Concatenate all filtered dataframes
    final_df = pl.concat(filtered_dfs)

    # Save the concatenated dataframe
    output_path = os.path.join(main_dir, "TrackingInjuries.parquet")
    final_df.write_parquet(output_path)

    print(f"Combined processed data saved to: {output_path}.")
    


def collect_injury_summaries(group_dir):

    import polars as pl # type: ignore
    import os

    # Initialize an empty list to store dataframes
    summary_dfs = []

    # Iterate through files in the directory
    for file in os.listdir(group_dir):
        if file.startswith("group_"):
            file_path = os.path.join(group_dir, file)
            
            # Read the Parquet file
            df = pl.read_parquet(file_path)
            
            # Apply the summary_calculator function
            temp_df = summary_calculator(df)
            
            # Append to the list of summary dataframes
            summary_dfs.append(temp_df)

    # Concatenate all summary dataframes
    summary_df = pl.concat(summary_dfs)


    return summary_df


###############################################################




def create_time_numeric(df):
    import polars as pl #type: ignore
    # Sort the dataframe by PlayKey and Time
    df = df.sort(['PlayKey', 'Time'])
    
    # Create a new column with incrementing values for each PlayKey
    df = df.with_columns([
        pl.arange(0, pl.len()).over('PlayKey').alias('index')
    ])
    
    # Calculate Time_numeric
    df = df.with_columns([
        (pl.col('index') * 0.1).alias('Time_numeric')
    ])
    
    # Drop the temporary index column
    df = df.drop('index')
    
    return df


def reduce_float_precision(df):
    import polars as pl # type: ignore
    for col in df.columns:
        if df[col].dtype == pl.Float64:
            df = df.with_columns(pl.col(col).cast(pl.Float32))
        elif df[col].dtype == pl.Int64:
            df = df.with_columns(pl.col(col).cast(pl.Int32))
    return df


def create_opponent_plays(df, method='tracking'): 
    import polars as pl # type: ignore
    pl.enable_string_cache()

    if method == 'summary':
        join_type = 'left'
    else: 
        join_type = 'inner'

    
    # Establish the column header order 
    column_order = [
        'PlayKey'
        , 'time'
        , 'x'
        , 'y'
        , 'o'
        , 'dir'
        , 'GSISID'
        , 'PlayerActivity'
        , 'ImpactType'
        , 'OpponentKey'
        , 'InjuryKey'
    ]

    review = clean_review()
    # First join: review.InjuryKey = df.PlayKey
    df_joined_injury = df.join(
        review
        , on='PlayKey'
        , how=join_type
        ).drop(['Primary_Partner_GSISID'
                , 'Primary_Partner_Activity_Derived'
                ]
        ).rename({'Player_Activity_Derived': 'PlayerActivity', 'Primary_Impact_Type': 'ImpactType'}
        ).select(column_order)


    # Second join: review.OpponentKey = df.PlayKey
    df_joined_opponent = df.join(
        review
        , left_on='PlayKey'
        , right_on='OpponentKey'
        , how=join_type
        ).drop(['PlayKey_right'
            , 'Player_Activity_Derived'
            , 'Primary_Partner_GSISID']
        ).rename({'Primary_Partner_Activity_Derived': 'PlayerActivity', 'Primary_Impact_Type': 'ImpactType'})

    df_joined_opponent = df_joined_opponent.with_columns([
        pl.lit(None).cast(pl.Utf8).alias('OpponentKey')
        ]).select(column_order)


    # Combine the results
    df_final = pl.concat([df_joined_injury, df_joined_opponent])

    return df_final


def clean_review():
    import polars as pl # type: ignore
    pl.enable_string_cache()
    from DataHandler import data_shrinker

    review = pl.read_csv("F:/Data/NFL-Punt-Analytics-Competition/video_review.csv")
    review, schema = data_shrinker(review)
    
    review = review.with_columns([
        pl.concat_str([
            pl.col('GSISID').cast(pl.Int32).cast(pl.Utf8)
            , pl.lit('-')
            , pl.col('GameKey').cast(pl.Utf8)
            , pl.lit('-')
            , pl.col('PlayID').cast(pl.Utf8)
        ]).alias('PlayKey')
        , pl.concat_str([
            pl.col('Primary_Partner_GSISID').cast(pl.Utf8)
            , pl.lit('-')
            , pl.col('GameKey').cast(pl.Utf8)
            , pl.lit('-')
            , pl.col('PlayID').cast(pl.Utf8)
        ]).alias('OpponentKey')
    ])

    review = review.with_columns([
        pl.col('PlayKey').alias('InjuryKey')
        ]).drop([
        'Season_Year'
        , 'GameKey'
        , 'PlayID'
        , 'GSISID'
        , 'Turnover_Related'
        , 'Friendly_Fire'
        ])
    
    return review


def column_corrector(df):
    import polars as pl # type: ignore
    """
    Add a Play_Time column that acts like the 'time' column did in the injury dataset. 
    Each PlayKey will start at 0.0 and increase by 0.1 for each subsequent record.
    """
    # Filter out rows with GSISID values that are too large for Int32
    df = df.filter(pl.col('GSISID') <= 1000000)  # Maximum value for Int32

    df = df.with_columns([
        pl.concat_str([
            pl.col('GSISID').cast(pl.Int32).cast(pl.Utf8)
            , pl.lit('-')
            , pl.col('GameKey').cast(pl.Utf8)
            , pl.lit('-')
            , pl.col('PlayID').cast(pl.Utf8)
        ]).alias('PlayKey')
    ])
     
    df = df.sort(['PlayKey', 'Time'])

    df = df.with_columns([
        pl.col('Time').str.strptime(
            pl.Datetime
            , format="%Y-%m-%d %H:%M:%S.%3f"
            , strict=False
        ).alias('Time')
    ])
    
    df = create_time_numeric(df)

    df = df.select([
        'PlayKey'
        , 'Time_numeric'
        , 'x'
        , 'y'
        , 'o'
        , 'dir'
        , 'GSISID'
        ]).rename({"Time_numeric":"time"})
    
    return df


###############################################################################
#### Transformation Functions
def calculate_angle_difference(angle1, angle2):
    """
    Calculate the smallest angle difference between two angles 
    using trigonometric functions, accounting for edge cases.
    """
    import numpy as np # type: ignore

    sin_diff = np.sin(np.radians(angle2 - angle1))
    cos_diff = np.cos(np.radians(angle2 - angle1))
    return np.degrees(np.arctan2(sin_diff, cos_diff))

def angle_corrector(df):
    """
    Make corrections to angles to reduce fringe errors at 360. 
    Due to the stupidity of the convention, where the direction was set for 
    0 degrees along the y axis, I will need to add 90 to all degree angles
    before doing any trig on them. This way when I calculate using vectors, they will 
    be on the same plane as the collected data for direction the player's 
    body is facing, the direction the player is oriented, and the direction the 
    body is physically moving in. Direction and Dir should match. 
    """
    import polars as pl # type: ignore

    try: 
        df = df.with_columns([
            ((pl.col("dir") + 90) % 360 - 180).alias("dir").abs()
            , ((pl.col("o") + 90) % 360 - 180).alias("o").abs()
        ]).with_columns(
            (calculate_angle_difference(pl.col("dir"), pl.col("o"))).abs().round(2).alias("Angle_Diff")
            )
        
        return df
    
    except Exception as e: 
        print(f"An error occurred during calculate_angle_difference: {e}")
        return None
    

def velocity_calculator(df):
    """
    Using the (X,Y) and time columns, perform calculations based on the 
    difference between two rows to find displacement, speed, direction 
    of motion, velocity in x and y components, and the angular velocities 
    of the direction of motion and orientations 
    """
    import numpy as np # type: ignore
    import polars as pl # type: ignore

    try:
        return df.with_columns([
            # Convert 'o' and 'dir' to radians
            (pl.col("o") * np.pi / 180).alias("o_rad"),
            (pl.col("dir") * np.pi / 180).alias("dir_rad")
        ]).with_columns([
            # Pre-calculate shifted values
            pl.col("x").shift(1).over("PlayKey").alias("prev_x")
            , pl.col("y").shift(1).over("PlayKey").alias("prev_y")
            # , pl.col("time").shift(1).over("PlayKey").alias("prev_time")
            , pl.col("dir_rad").shift(1).over("PlayKey").alias("prev_dir")
            , pl.col("o_rad").shift(1).over("PlayKey").alias("prev_o")
        ]).with_columns([
            # Calculate the component displacements 
            (pl.col("x") - pl.col("prev_x")).alias("dx")
            , (pl.col("y") - pl.col("prev_y")).alias("dy")
        ]).with_columns([
            # Calculate displacement
            ((pl.col("dx")**2 + pl.col("dy")**2)**0.5).alias("Displacement")
        ]).with_columns([
            # Calculate speed
            (pl.col("Displacement") / 0.1).alias("Speed")
            # Calculate velocity components
            , (pl.col("dx") / 0.1).alias("vx")
            , (pl.col("dy") / 0.1).alias("vy")
            # Calculate angular velocities
            , ((pl.col("dir_rad") - pl.col("prev_dir")) / 0.1).alias("omega_dir")
            , ((pl.col("o_rad") - pl.col("prev_o")) / 0.1).alias("omega_o")
        ]).with_columns([
            ((pl.col("omega_dir") - pl.col("omega_o")).abs()).alias("omega_diff")
        ]).drop([
            "prev_x", "prev_y", "prev_dir", "prev_o", "dx", "dy", "o_rad", "dir_rad"
        ])
    
    except Exception as e: 
        print(f"An error occurred during velocity_calculator: {e}")
        return None
    

def body_builder_inj(df):
    """
    This uses averages collected for height, weight, and chest radius for each position. This information
    is used to determine the momentum and impulse rather than just looking at velocities in the analysis. Chest
    radius is needed for angular moment of inertia as a rotating cylinder.
    The data here are cast as f32 to reduce the size of these columns as well as in all future calculations, where the f64 
    gets exponentially larger with application. 
    """
    import polars as pl # type: ignore

    # Enable global string cache
    pl.enable_string_cache()
    
    try:
        body_data = pl.DataFrame({
            "Position": ["QB", "RB", "FB", "WR", "TE", "T", "G", "C", "DE", "DT", "NT", "LB", "OLB", "MLB", "CB", "S", "K", "P", "SS", "ILB", "FS", "LS", "DB"]
            , "Height_m": [1.91, 1.79, 1.85, 1.88, 1.96, 1.97, 1.90, 1.87, 1.97, 1.92, 1.88, 1.90, 1.90, 1.87, 1.82, 1.84, 1.83, 1.88, 1.84, 1.90, 1.84, 1.88, 1.82]
            , "Weight_kg": [102.1, 95.3, 111.1, 90.7, 114.6, 140.6, 141.8, 136.1, 120.2, 141.8, 152.0, 110.0, 108.9, 113.4, 87.4, 95.9, 92.08, 97.52, 95.9, 110.0, 95.9, 108.86, 87.4]
            , "Chest_rad_m": [0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191]
        }).with_columns([
            pl.col("Height_m").cast(pl.Float32)
            , pl.col("Weight_kg").cast(pl.Float32)
            , pl.col("Chest_rad_m").cast(pl.Float32)
            , pl.col("Position").cast(pl.Categorical)
        ])

        PlayList_path = "F:/Data/nfl-playing-surface-analytics/PlayList.csv"
        position = pl.read_csv(PlayList_path).select(["PlayKey", "Position"])
        position = position.with_columns([
            pl.col("PlayKey").cast(pl.Utf8)
            , pl.col("Position").cast(pl.Categorical)
        ])

        position = position.join(
            body_data
            , on='Position'
            , how='left'
        )

        df = df.with_columns([
            pl.col("PlayKey").cast(pl.Utf8)
        ])

        df = df.join(
            position
            , on='PlayKey'
            , how='left'
        )    

        return df.filter(pl.col('Position').is_not_null())    
        
    except Exception as e: 
        print(f"An error occurred during body_builder: {e}")
        return None


def body_builder_conc(df):
    """
    This uses averages collected for height, weight, and chest radius for each position. This information
    is used to determine the momentum and impulse rather than just looking at velocities in the analysis. Chest
    radius is needed for angular moment of inertia as a rotating cylinder.
    The data here are cast as f32 to reduce the size of these columns as well as in all future calculations, where the f64 
    gets exponentially larger with application. 
    """
    import polars as pl # type: ignore

    # Enable global string cache
    pl.enable_string_cache()


    try:
        body_data = pl.DataFrame({
            "Position": ["QB", "RB", "FB", "WR", "TE", "T", "G", "C", "DE", "DT", "NT", "LB", "OLB", "MLB", "CB", "S", "K", "P", "SS", "ILB", "FS", "LS", "DB"]
            , "Height_m": [1.91, 1.79, 1.85, 1.88, 1.96, 1.97, 1.90, 1.87, 1.97, 1.92, 1.88, 1.90, 1.90, 1.87, 1.82, 1.84, 1.83, 1.88, 1.84, 1.90, 1.84, 1.88, 1.82]
            , "Weight_kg": [102.1, 95.3, 111.1, 90.7, 114.6, 140.6, 141.8, 136.1, 120.2, 141.8, 152.0, 110.0, 108.9, 113.4, 87.4, 95.9, 92.08, 97.52, 95.9, 110.0, 95.9, 108.86, 87.4]
            , "Chest_rad_m": [0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191]
        }).with_columns([
            pl.col("Height_m").cast(pl.Float32)
            , pl.col("Weight_kg").cast(pl.Float32)
            , pl.col("Chest_rad_m").cast(pl.Float32)
            , pl.col("Position").cast(pl.Categorical)
        ])

        
        Player_path = "F:/Data/NFL-Punt-Analytics-Competition/player_punt_data.csv"
        position = pl.read_csv(Player_path).select(["GSISID", "Position"])
        position = position.with_columns([
            pl.col("GSISID").cast(pl.Int32)
            , pl.col("Position").cast(pl.Categorical)
        ])

        position = position.join(
            body_data
            , on='Position'
            , how='left'
        )

        df = df.with_columns([
            pl.col("GSISID").cast(pl.Int32)
        ])

        df = df.join(
            position
            , on='GSISID'
            , how='left'
        )    

        return df.filter(pl.col('Position').is_not_null())    
        
    except Exception as e: 
        print(f"Something broke the body_builder: {e}")
        return None


def impulse_calculator(df):
    """
    Using the (X,Y) and time columns, perform calculations based on the velocities and changes 
    in velocites along with player mass to get the momentum and impulse, a measure that can 
    be assessed along with medical data related to concussions and injuries
    """
    import numpy as np # type: ignore
    import polars as pl # type: ignore
    

    try: 
        return df.with_columns([
            # Calculate the linear momentum for each instant
            (pl.col('vx') * pl.col('Weight_kg')).alias('px')
            , (pl.col('vy') * pl.col('Weight_kg')).alias('py')

            # Calculate the moment of inertia of a rotating upright body (1/12 mr^2)
            , (1/2 * pl.col('Weight_kg') * (pl.col('Chest_rad_m')**2)).alias('moment')
            
            # Calculate the moment of inertia of the upper body turning upright with respect to waist (70% mass)
            , (1/2 * (pl.col('Weight_kg')*0.7) * (pl.col('Chest_rad_m')**2)).alias('moment_upper')
        
        ]).with_columns([
            # Calculate the magnitude of linear momentum
            ((pl.col("px")**2 + pl.col("py")**2)**0.5).alias("p_magnitude")
            
            # Calculate the angular momentum for the direction
            , (pl.col('omega_dir')*pl.col('moment')).alias('L_dir')

            # Calculate the angular momentum of the upper body with respect to lower
            , (pl.col('omega_diff')*pl.col('moment_upper')).alias('L_diff')


        ]).with_columns([
            # Pre-calculate shifted values for linear and angular momenta
            pl.col("px").shift(1).over("PlayKey").alias("prev_px")
            , pl.col("py").shift(1).over("PlayKey").alias("prev_py")
            , pl.col("L_dir").shift(1).over("PlayKey").alias("prev_L_dir")
            , pl.col("L_diff").shift(1).over("PlayKey").alias("prev_L_diff")
            
        ]).with_columns([
            # Calculate impulse, J, which is the change in linear momentum 
            ((pl.col("px") - pl.col("prev_px"))).alias("Jx")
            , ((pl.col("py") - pl.col("prev_py"))).alias("Jy")
            
        ]).with_columns([
            # Calculate the magnitude of linear momentum
            ((pl.col("Jx")**2 + pl.col("Jy")**2)**0.5).alias("J_magnitude")

            # Calculate torque as the change in angular momentum L over the change in time
            , (((pl.col("L_dir") - pl.col("prev_L_dir"))) / 0.1).alias("torque")
            , (((pl.col("L_diff") - pl.col("prev_L_diff"))) / 0.1).alias("torque_internal")

        ]).drop([
            "prev_L_dir", "prev_px", "prev_py", "prev_L_diff"
        ])
    
    except Exception as e: 
        print(f"Something got fucked up in the impulse_calculator, which surprises no one.")
        return None
    


########################################

# Summary Calculations
def summary_calculator(df):
    """
    Collects dispalcement and distance, means and maxima for the for each of the parameters collected
    and outputs to a quantitative summary table that can be joined to the qualitative table for machine learning.  
    """
    import polars as pl # type: ignore

    result = df.select([
        "PlayKey"
        , pl.col("Position")
        , pl.col("Displacement").sum().over("PlayKey").alias("Distance")
        , pl.col("x").first().over("PlayKey").alias("start_x")
        , pl.col("y").first().over("PlayKey").alias("start_y")
        , pl.col("x").last().over("PlayKey").alias("end_x")
        , pl.col("y").last().over("PlayKey").alias("end_y")
        , pl.col("Angle_Diff").max().over("PlayKey").alias("Max_Angle_Diff")
        , pl.col("Angle_Diff").mean().over("PlayKey").alias("Mean_Angle_Diff")
        , pl.col("Speed").max().over("PlayKey").alias("Max_Speed")
        , pl.col("Speed").mean().over("PlayKey").alias("Mean_Speed")
        , pl.col("J_magnitude").max().over("PlayKey").alias("Max_Impulse")
        , pl.col("J_magnitude").mean().over("PlayKey").alias("Mean_Impulse")
        , pl.col("torque").max().over("PlayKey").alias("Max_Torque")
        , pl.col("torque").mean().over("PlayKey").alias("Mean_Torque")
        , pl.col("torque_internal").max().over("PlayKey").alias("Max_Int_Torque")
        , pl.col("torque_internal").mean().over("PlayKey").alias("Mean_Int_Torque")

        ]).unique(subset=["PlayKey"])


    # Calculate the displacement
    result = result.with_columns([
        (((pl.col("end_x") - pl.col("start_x"))**2 + 
          (pl.col("end_y") - pl.col("start_y"))**2)**0.5)
        .alias("Displacement")
        ]).with_columns([
            (pl.col("Distance") - pl.col("Displacement")).alias("Path_Diff")
        ])

     
    # Select only the required columns
    result = result.select([
        'PlayKey'
        , 'Position'
        , 'Distance'
        , 'Displacement'
        , 'Path_Diff'
        , 'Max_Angle_Diff'
        , 'Mean_Angle_Diff'
        , 'Max_Speed'
        , 'Mean_Speed'
        , 'Max_Impulse'
        , 'Mean_Impulse'
        , 'Max_Torque'
        , 'Mean_Torque'
        , 'Max_Int_Torque'
        , 'Mean_Int_Torque'
      
    ]).sort("PlayKey")


    return result