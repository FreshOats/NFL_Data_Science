# Cleans and Calculates Quantitative Values from Tracking Data

#### Primary Injury Tranformation Functions ####
def optimize_injury_data():
    """
    This function opens the original tracking csv, reduces size by casting to less expensive data types, 
    and then saves the optimized tracking dataset to file. It does not return a dataframe, so any use will 
    be expected to call from the saved optimized file. 
    """
    import polars as pl # type: ignore
    from DataHandler import data_shrinker

    injury_tracking_path = "F:/Data/nfl-playing-surface-analytics/PlayerTrackData.csv"
    optimized_path = "F:/Data/Processing_data/OptimizedTrackData.parquet"

    df = create_initial_lazyframe(injury_tracking_path).collect(streaming=True)
    optimized_df, optimized_schema = data_shrinker(df)
    
    # Cast the DataFrame columns to the types specified in optimized_schema
    for column, dtype in optimized_schema.items():
        optimized_df = optimized_df.with_columns(pl.col(column).cast(dtype))

    # Write the DataFrame to a Parquet file
    optimized_df.write_parquet(optimized_path)

    print(f"Saved optimized data to {optimized_path}. Now go get some ice.")



def clean_injury_quant():
    """
    Cleans and transforms quantitative data from the tracking 
    data collected. Makes a number of calculations for physical 
    analysis and inserts data collected from external sources. 
    """

    # Read Path
    optimized_path = "F:/Data/Processing_data/OptimizedTrackData.parquet"

    # Write paths
    group_dir = "F:/Data/Processing_data/injury_output"
    main_dir = "F:/Data/Processing_data/"


    # Transform     
    process_file(optimized_path, group_dir) # WORKING
    injury_summary_maker(group_dir)   # This is currently broken because there is no "F:/Data/Processing_data/QualitativeInjuries.parquet"
            # I will need to fix the qualitative data functions and use the outputs from those to run this. 
    tracking_injuries(group_dir, main_dir) # WORKING



###############################################################################
#### Primary Concussion Tranformation Functions ####

def clean_concussion_quant():
    """
    Cycles through the different datasets cleaning the concussion data. 
    After processing, the data are filtered and concatenated into two different datasets: summary, and tracking
    """

    analysis = 'concussion'
    source_dir = "F:/Data/NFL-Punt-Analytics-Competition/"

    











###############################################################################
#### Processing Subroutines
def create_initial_lazyframe(injury_tracking_path):
    import polars as pl # type: ignore 
    return pl.scan_csv(injury_tracking_path, truncate_ragged_lines=True, infer_schema_length=10000, ignore_errors=True).drop(['event', 's', 'dis'])


# def transform_injury_data(optimized_path, group_dir):
#     """
#     Full transform process for the surface injury data.
#     Output options are for returning a summary df to the database or the full tracking with 
#     additional columns added
#     """

#     if __name__ == "__main__":
#         process_file(optimized_path, group_dir)


def process_and_save_playkey_group(df, playkeys, group_dir, group_number):
    import polars as pl # type: ignore
    import os
    # Filter the lazy DataFrame for the specific PlayKeys
    group_df = df.filter(pl.col("PlayKey").is_in(playkeys))
    
    # Processing
    group_df = (group_df
                .pipe(angle_corrector)
                .pipe(velocity_calculator)
                .pipe(body_builder_inj)
                .pipe(impulse_calculator))

    # Create the output directory if it doesn't exist
    os.makedirs(group_dir, exist_ok=True)
    
    # Save the DataFrame for this group as a Parquet file
    output_file = os.path.join(group_dir, f"group_{group_number}.parquet")
    group_df.write_parquet(output_file)
    print(f"Saved data for PlayKey group: {group_number}")


def process_file(optimized_path, group_dir, group_size=20000):
    """
    Full transform process for the surface injury data.
    Output options are for returning a summary df to the database or the full tracking with 
    additional columns added
    """
    import math
    import polars as pl # type: ignore

    # Read the Parquet file as a DataFrame
    df = pl.read_parquet(optimized_path)
    
    # Get unique PlayKey values
    unique_playkeys = df["PlayKey"].unique().to_list()
    
    # Calculate the number of groups
    num_groups = math.ceil(len(unique_playkeys) / group_size)
    
    # Process each group of PlayKeys
    for i in range(num_groups):
        start_idx = i * group_size
        end_idx = min((i + 1) * group_size, len(unique_playkeys))
        playkey_group = unique_playkeys[start_idx:end_idx]
        process_and_save_playkey_group(df, playkey_group, group_dir, i + 1)

    print("Processing complete.")


def process_and_save_concussion_data():
    """
    Extract and Transform the tracking data for the concussion dataset per file.  
    These datasets will then be used to create the summary data and then filter 
    for the injuries using the video review information to get only the concussion plays for
    the player and their opponent, which will be a different playkey based on GSISID, but the latter
    portion will match. 
    """
    import polars as pl # type: ignore
    import os
    from DataHandler import data_shrinker

    source_dir = "F:/Data/NFL-Punt-Analytics-Competition/"
    output_dir = "F:/Data/Processing_data/concussion_output/"
    os.makedirs(output_dir, exist_ok=True)


    for file in os.listdir(source_dir):
        if file.startswith("NGS-"):
            file_path = os.path.join(source_dir, file)
            

            # Read the CSV into polars DF
            df = pl.read_csv(file_path, truncate_ragged_lines=True, ignore_errors=True)
            df, schema = data_shrinker(df)
            df = (df
                .pipe(column_corrector)
                    .pipe(angle_corrector)
                    .pipe(body_builder_conc)
                    .pipe(velocity_calculator)                
                    .pipe(impulse_calculator))
            
            output_file_path = os.path.join(output_dir, file.replace(".csv", ".parquet"))

            df.write_parquet(output_file_path)

            print(f"Processed and saved: {output_file_path}")

    print("For fuck's sake that took a while. Finally done processing and saving the concussion files.")



def create_concussion_review_df(review):
    """
    Create a new DataFrame from multiple Parquet files, including only rows where the PlayKey matches those in the review DataFrame.
    """
    import polars as pl # type: ignore
    import os
    pl.enable_string_cache()

    ngs_dir = "F:/Data/Processing_data/concussion_output/"
    output_file_path = "F:/Data/Processing_data/OpponentPlays.parquet"


    # Add the OpponentKey column
    review = review.with_columns(
        (pl.col("Primary_Partner_GSISID") + pl.col("PlayKey").str.slice(5)).alias("OpponentKey")
        )
    # Filter OpponentKey values that are longer than 12 characters
    
    # Extract PlayKey and OpponentKey values into lists
    playkey_list = review["PlayKey"].to_list()
    opponentkey_list = review["OpponentKey"].to_list()

    # Remove any "Unknown" GSISID opponents from the list, since it will be a nonsense PlayKey
    opponentkey_list = [key for key in opponentkey_list if key is not None and len(key) <= 12]

    # Combine both lists
    combined_keys = playkey_list + opponentkey_list


    # Initialize a list to store dataframes from each table
    dataframes = []

    # Iterate through the parquet files in the directory
    for file in os.listdir(ngs_dir):
        if file.startswith("NGS-"):
            file_path = os.path.join(ngs_dir, file)

            # Read into df
            df = pl.read_parquet(file_path)

            # Filter based on matching PlayKey values
            filtered_df = df.filter(pl.col('PlayKey').is_in(combined_keys))

            # Append to the dataframes
            dataframes.append(filtered_df)

    combined_df = pl.concat(dataframes)

    combined_df = combined_df.join(
        review
        , on='PlayKey'
        , how = 'left'
    )

    combined_df.write_parquet(output_file_path)

    print(f"Processed and saved: {output_file_path}")


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
            ((pl.col("dir") + 90) % 360 - 180).alias("dir")
            , ((pl.col("o") + 90) % 360 - 180).alias("o")
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
        print(f"An error occurred during body_builder: {e}")
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
            , (1/12 * pl.col('Weight_kg') * (pl.col('Chest_rad_m')**2)).alias('moment')
            
            # Calculate the moment of inertia of the upper body turning upright with respect to waist (70% mass)
            , (1/12 * (pl.col('Weight_kg')*0.7) * (pl.col('Chest_rad_m')**2)).alias('moment_upper')
        
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
        print(f"An error occurred during the impulse_calculator, which surprises no one.")
        return None
    

def column_corrector(df):
    import polars as pl # type: ignore
    """
    Add a Play_Time column that acts like the 'time' column did in the injury dataset. 
    Each PlayKey will start at 0.0 and increase by 0.1 for each subsequent record.
    """
    df = df.with_columns([
        pl.concat_str([
            pl.col('GSISID').cast(pl.Int32).cast(pl.Utf8)
            , pl.lit('-')
            , pl.col('GameKey').cast(pl.Utf8)
            , pl.lit('-')
            , pl.col('PlayID').cast(pl.Utf8)
        ]).alias('PlayKey')
    ])
     
    
    df = df.select([
        'PlayKey'
        , 'Time'
        , 'x'
        , 'y'
        , 'o'
        , 'dir'
        , 'GSISID'
        ]).rename({"Time":"DateTime"})

    df = df.sort(['PlayKey', 'DateTime'])

    df = df.with_columns(
        (pl.arange(0, pl.len()) * 0.1).over("PlayKey").cast(pl.Float32).alias("time")
        ).with_columns([pl.col('GSISID').cast(pl.Int32)])  
    
    df = df.drop(['DateTime'])
    
    return df


def clean_review():
    import polars as pl #type: ignore
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
        ]).drop(['Season_Year', 'GameKey', 'PlayID', 'GSISID', 'Turnover_Related', 'Friendly_Fire'])
    
    return review


def add_review_data(df, review):

    df = df.join(
        review
        , on="PlayKey"
        , how="inner"    
        )
    
    return df

###############################################################################
#### Summary Output
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


def collect_summaries(group_dir):

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

    # Save the concatenated dataframe
    # summary_df.write_parquet(os.path.join(group_dir, "summary_df.parquet"))

    # print("Processing complete. Summary dataframe saved as 'summary_df.parquet'")

    return summary_df


def collect_concussion_summaries(group_dir="F:/Data/Processing_data/concussion_output"):
    import polars as pl
    import os

    # Initialize an empty list for the dataframes
    summary_dfs = []

    # Iterate through files in the directory
    for file in os.listdir(group_dir):
        if file.startswith("NGS-"):
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


def injury_summary_maker(group_dir):
    """
    Joins the qualitative and quantitative summary data
    """
    import polars as pl # type: ignore
    # Read
    qual_path = "F:/Data/Processing_data/QualitativeInjuries.parquet"
    
    #Write    
    qual_quant_path = "F:/Data/Processing_data/Full_Summary_Injuries.parquet"
    

    quant = collect_summaries(group_dir)
    quals = pl.read_parquet(qual_path)
    qual_quant = quals.join(quant, on="PlayKey", how="inner")

    qual_quant.write_parquet(qual_quant_path)
    print(f"Saved the full summary with qualitative and quantitative features at {qual_quant_path}")


def concussion_summary_maker(group_dir="F:/Data/Processing_data/concussion_output"): 
    """
    Joins the qualitative and quantitative summary data from the concussion sets
    """
    import polars as pl # type: ignore
    pl.enable_string_cache()

    # Read
    qual_path = "F:/Data/Processing_data/QualitativeConcussions.parquet"
    
    #Write    
    qual_quant_path = "F:/Data/Processing_data/Full_Summary_Concussions.parquet"

    quant = collect_concussion_summaries(group_dir)
    quals = pl.read_parquet(qual_path).drop(['GSISID', 'GameKey', 'PlayID', 'Number', 'Game_Date', 'YardLine', 'Quarter', 'Start_Time'])

    qual_quant = quals.join(quant, on="PlayKey", how="inner")

    qual_quant.write_parquet(qual_quant_path)
    print(f"Saved the full summary with qualitative and quantitative features at {qual_quant_path}")  


###############################################################################
#### Tracking Output
def tracking_injuries(group_dir, main_dir):
    from DataHandler import data_loader
    import os
    import polars as pl # type: ignore
    pl.enable_string_cache()

    # Read in the PlayKeys from the injury file to isolate PlayKeys associated with injury paths
    injuryPlayKeys = data_loader('injuries')
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
    final_df.write_parquet(os.path.join(main_dir, "TrackingInjuries.parquet"))

    print("Processing complete. Filtered summary dataframe saved as 'TrackingInjuries.parquet'")