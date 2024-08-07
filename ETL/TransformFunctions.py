""" 
Transform Functions
This imports the clean data from the database and reduces the memory usage prior to processing the data
"""

def transform_injury_data(analysis_type='tracking'):
    """
    Full transform process for the surface injury data.
    Output options are for returning a summary df to the database or the full tracking with 
    additional columns added
    """
    if analysis_type == 'tracking':
        if __name__ == "__main__":
            input_file = "F:/Data/Processing_data/tracking.csv"
            output_dir = "F:/Data/Processing_data/output"
    
            process_csv('tracking', input_file, output_dir)

    elif analysis_type =='summary':
        if __name__ == "__main__":
            input_file = "F:/Data/Processing_data/tracking.csv"
            output_dir = "F:/Data/Processing_data/output"
            
            process_csv('summary', input_file, output_dir)


def process_and_save_playkey_group(lazy_df, playkeys, output_dir, group_number):
    from DataHandler import data_shrinker
    import os
    import polars as pl # type: ignore

    # Filter the lazy DataFrame for the specific PlayKeys
    group_df = lazy_df.filter(pl.col("PlayKey").is_in(playkeys))
    
    # Processing
    group_df = angle_corrector(group_df)
    group_df = velocity_calculator(group_df)
    group_df = body_builder(group_df)
    group_df = impulse_calculator(group_df)


    # Collect the data
    processed_df = group_df.collect()
    processed_df = data_shrinker(processed_df)
 

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    

    # Save the DataFrame for this group as a CSV file
    output_file = os.path.join(output_dir, f"group_{group_number}.csv")
    processed_df.write_csv(output_file)
    print(f"Saved data for PlayKey group: {group_number}")


def process_and_save_summary(lazy_df, playkeys, output_dir, group_number):
    # Filter the lazy DataFrame for the specific PlayKeys
    group_df = lazy_df.filter(pl.col("PlayKey").is_in(playkeys))
    
    # Processing
    group_df = angle_corrector(group_df)
    group_df = velocity_calculator(group_df)
    group_df = body_builder(group_df)
    group_df = impulse_calculator(group_df)

    # Calculate the path summary quant data
    summary_df = path_calculator(group_df)

    # Collect the data
    summary_df = summary_df.collect()
    summary_df = data_shrinker(summary_df)


    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    

    # Save the summary data for this group in a separate csv file
    summary_file = os.path.join(output_dir, f"summary_group_{group_number}.csv")
    summary_df.write_csv(summary_file)
    
    print(f"Saved data for PlayKey group: {group_number}")


def process_csv(analysis_type, input_file, output_dir, group_size=10000):
    # Scan the CSV file
    lazy_df = pl.scan_csv(input_file, truncate_ragged_lines=True, infer_schema_length=10000, ignore_errors=True).drop(['event', 's'])
    
    # Get unique PlayKey values
    unique_playkeys = lazy_df.select(pl.col("PlayKey").unique()).collect()["PlayKey"].to_list()
    
    # Calculate the number of groups
    num_groups = math.ceil(len(unique_playkeys) / group_size)
    

    if analysis_type == "tracking":
        # Process each group of PlayKeys
        for i in range(num_groups):
            start_idx = i * group_size
            end_idx = min((i + 1) * group_size, len(unique_playkeys))
            playkey_group = unique_playkeys[start_idx:end_idx]
            process_and_save_playkey_group(lazy_df, playkey_group, output_dir, i + 1)

    elif analysis_type == "summary":
        # Process each group of Summary
        for i in range(num_groups):
            start_idx = i * group_size
            end_idx = min((i + 1) * group_size, len(unique_playkeys))
            playkey_group = unique_playkeys[start_idx:end_idx]
            process_and_save_summary(lazy_df, playkey_group, output_dir, i + 1)

    print("Processing complete.")


    # from DataHandler import data_loader, data_shrinker, data_writer
    # import polars as pl # type: ignore

    # quant = pl.scan_csv("F:/Data/nfl-playing-surface-analytics/PlayerTrackData.csv").drop(['event', 's'])

    # valid_outputs = ['tracking', 'summary']
    # if output not in valid_outputs:
    #     raise ValueError(f"Invalid ouptut selection: '{output}'. Valid options are: '{valid_outputs}'")

    # try: 
    #     # Transform the tracking data
    #     quant = angle_corrector(quant)
    #     print("angles corrected")
    #     quant = body_builder(quant, 'tracking')
    #     print("weights added")
    #     quant = velocity_calculator(quant)
    #     print("velocity calculated")
    #     quant = impulse_calculator(quant)
    #     print("impulse calculated, starting path calculations.")
    
    #     if output == 'summary':
    #         summary = path_calculator(quant)
    #         print("path calculated, opening data loader")
    #         del quant # remove the large table from memory
    #         # Open and merge the qualitative data
    #         quals = data_loader('qualitative', 'nfl_surface')
    #         print("qualitative loaded")
    #         qual_quant = qual_quant_merger(quals, summary)
            
    #         print("Writing all quantitative and qualitative summary data to the database as summary_data. Wait.")
    #         data_writer(qual_quant, 'nfl_surface', 'summary_data')
    #         print("Data has been uploaded to the database. Probably.")        

    #     elif output == 'tracking':
    #         # upload the physical data to the database for machine learning
    #         print("Writing the transformed table with physical parameters to the database as quantitative")
    #         data_writer(quant, 'nfl_surface', 'quantitative')
    #         print("Data has been uploaded to the database. Go celebrate!")
        
    # except Exception as e:
    #     print(f"An error occurred with your selection, '{output}': {e}")
    #     return None

######################################################################################
       
def transform_concussion_data(input_file, output_dir):
    """
    Full transform process for the concussion data.
    Output options are for returning a summary df to the database or the full tracking with 
    additional columns added
    """
    import os 
    import polars as pl 

    # Extract the base name of the input file
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    
    # Read the CSV file
    lazy_df = pl.scan_csv(input_file, truncate_ragged_lines=True, ignore_errors=True).drop(['Event', 'dis', 'Season_Year'])
    
    # Apply the processing steps
    lazy_df = column_corrector(lazy_df)
    lazy_df = angle_corrector(lazy_df)
    lazy_df = body_builder(lazy_df, 'ngs_data')
    lazy_df = velocity_calculator(lazy_df)
    lazy_df = impulse_calculator(lazy_df)
    
    # Calculate the path summary
    lazy_summary = path_calculator(lazy_df)
    
    # Create output filenames
    processed_output = os.path.join(output_dir, f"{base_name}_processed.csv")
    summary_output = os.path.join(output_dir, f"{base_name}_summary.csv")
    
    # Save the processed data and summary
    lazy_df.collect().write_csv(processed_output)
    lazy_summary.collect().write_csv(summary_output)
    
    print(f"Processed and saved data for file: {input_file}")


    # Main Concussion execution
    if __name__ == "__main__":
        input_dir = "F:/Data/Processing_data/NGS"
        output_dir = "F:/Data/Processing_data/output"
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Process all NGS-*.csv files in the input directory
        for filename in os.listdir(input_dir):
            if filename.startswith("NGS") and filename.endswith(".csv"):
                input_file = os.path.join(input_dir, filename)
                process_ngs_file(input_file, output_dir)
        
        print("All files processed.")


######################################################################################
def column_corrector(df):
    """
    Add a Play_Time column that acts like the 'time' column did in the injury dataset. 
    Each PlayKey will start at 0.0 and increase by 0.1 for each subsequent record.
    """
    import polars as pl # type: ignore
    
    # First, let's identify any problematic gsisid values
    problematic_gsisid = df.filter(pl.col('GSISID').cast(pl.Int32).is_null()).select('GSISID').unique()
    
    # Collect the problematic gsisid values (this will execute the lazy computation)
    problematic_gsisid_count = problematic_gsisid.collect().height
    
    if problematic_gsisid_count > 0:
        print(f"Found {problematic_gsisid_count} problematic gsisid values:")
        print(problematic_gsisid.collect())
    
    df = df.with_columns([
        pl.concat_str([
            pl.when(pl.col('GSISID').cast(pl.Int32).is_null())
              .then(pl.col('GSISID').cast(pl.Float64).round(0).cast(pl.Int32).cast(pl.Utf8))
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
        ]).rename({"Time":"Datetime"})

    df = df.sort(['PlayKey', 'Datetime'])

    df = df.with_columns(
        (pl.arange(0, pl.len()) * 0.1).over("PlayKey").alias("time")
        ).with_columns([
            pl.when(pl.col('GSISID').cast(pl.Int32).is_null())
              .then(pl.col('GSISID').cast(pl.Float64).round(0).cast(pl.Int32))
              .otherwise(pl.col('GSISID').cast(pl.Int32))
              .alias('GSISID')
        ])  
    
    return df



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
    Make corrections to angles to reduce fringe errors at 360
    """
    import polars as pl # type: ignore

    df = df.with_columns([
        ((pl.col("dir") + 180) % 360 - 180).alias("dir")
        , ((pl.col("o") + 180) % 360 - 180).alias("o")
    ]).with_columns(
        (calculate_angle_difference(pl.col("dir"), pl.col("o"))).abs().round(2).alias("Angle_Diff")
        )
    
    return df


def body_builder(df, df_name):
    """
    This uses averages collected for height, weight, and chest radius for each position. This information
    is used to determine the momentum and impulse rather than just looking at velocities in the analysis. Chest
    radius is needed for angular moment of inertia as a rotating cyliner.
    """
    import polars as pl # type: ignore
    from DataHandler import data_loader

    body_data = pl.LazyFrame({
        "position": ["QB", "RB", "FB", "WR", "TE", "T", "G", "C", "DE", "DT", "NT", "LB", "OLB", "MLB", "CB", "S", "K", "P", "SS", "ILB", "FS", "LS", "DB"]
        # , "Position_Name": ["Quarterback", "Running Back", "Fullback", "Wide Receiver", "Tight End", "Tackle", "Guard", "Center", "Defensive End", "Defensive Tackle", "Nose Tackle", "Linebacker", "Outside Linebacker", "Middle Linebacker", "Cornerback", "Safety", "Kicker", "Punter", "Strong Safety", "Inside Linebacker", "Free Safety", "Long Snapper", "Defensive Back"]
        , "Height_m": [1.91, 1.79, 1.85, 1.88, 1.96, 1.97, 1.90, 1.87, 1.97, 1.92, 1.88, 1.90, 1.90, 1.87, 1.82, 1.84, 1.83, 1.88, 1.84, 1.90, 1.84, 1.88, 1.82]
        , "Weight_kg": [102.1, 95.3, 111.1, 90.7, 114.6, 140.6, 141.8, 136.1, 120.2, 141.8, 152.0, 110.0, 108.9, 113.4, 87.4, 95.9, 92.08, 97.52, 95.9, 110.0, 95.9, 108.86, 87.4]
        , "Chest_rad_m": [0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191, 0.191]
        })

    valid_df_names = ['ngs_data', 'tracking']
    if df_name not in valid_df_names:
        raise ValueError(f"Invalid dataframe name '{df_name}'. Valid options are: {valid_df_names}")

    try:    
        if df_name == 'tracking':
            position = data_loader(dataset='play_positions', database='nfl_surface')
            position = position.lazy().join(
                body_data
                , on='position'
                , how='left'
                )

            df = df.join(
                position
                , left_on='PlayKey'
                , right_on='playkey'
                , how='left'
            )    
            

        elif df_name == 'ngs_data':
            position = data_loader(dataset='positions', database='nfl_concussion')
            position = position.lazy().join(
                body_data
                , left_on='position'
                , right_on='position'
                , how='left'
                )
            
            df = df.join(
                position
                , left_on='GSISID'
                , right_on='gsisid'
                , how='left'
                )
        

        return df.filter(pl.col('position').is_not_null())    
    
    except Exception as e: 
        print(f"An error occurred while loading the dataframe '{df_name}': {e}")
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
        # Calculate direction
        , (np.degrees(np.arctan2(pl.col("dx"), pl.col("dy")))).alias("Direction")
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


def impulse_calculator(df):
    """
    Using the (X,Y) and time columns, perform calculations based on the velocities and changes 
    in velocites along with player mass to get the momentum and impulse, a measure that can 
    be assessed along with medical data related to concussions and injuries
    """
    import numpy as np # type: ignore
    import polars as pl # type: ignore
    
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


def path_calculator(df):
    """
    Collects dispalcement and distance, means and maxima for the for each of the parameters collected
    and outputs to a quantitative summary table that can be joined to the qualitative table for machine learning.  
    """
    import polars as pl # type: ignore

    result = df.select([
        "PlayKey"
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

# Join the Qualitative with the Quantitative to create Summary Table
def qual_quant_merger(quals, quant):
    """
    Joins the qualitative and quantitative summary data
    """

    from DataHandler import data_shrinker

    qual_quant = quals.join(quant, on="PlayKey", how="left")
    qual_quant = data_shrinker(qual_quant)

    return qual_quant