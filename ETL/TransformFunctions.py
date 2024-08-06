# Transform Functions
# This imports the clean data from the database and reduces the memory usage prior to processing the data
# This will take about 3 minutes to run


def transform_injury_data(output):
    from DataHandler import data_loader, data_shrinker, data_writer
    import polars as pl # type: ignore

    valid_outputs = ['tracking', 'summary']
    if output not in valid_outputs:
        raise ValueError(f"Invalid ouptut selection: '{output}'. Valid options are: '{valid_outputs}'")

    try: 
        # Transform the tracking data
        quant = data_loader(dataset='tracking', database='nfl_surface')
        quant = data_shrinker(quant)
        quant = angle_corrector(quant)
        quant = body_builder(quant, 'tracking')
        quant = velocity_calculator(quant)
        quant = impulse_calculator(quant)
    
        if output == 'summary':
            summary = path_calculator(quant)
            del quant # remove the large table from memory
            # Open and merge the qualitative data
            quals = data_loader('qualitative', 'nfl_surface')
            qual_quant = qual_quant_merger(quals, summary)
            
            print("Writing all quantitative and qualitative summary data to the database as summary_data. Wait.")
            data_writer(qual_quant, 'nfl_surface', 'summary_data')
            print("Data has been uploaded to the database. Probably.")        

        elif output == 'tracking':
            # upload the physical data to the database for machine learning
            print("Writing the transformed table with physical parameters to the database as quantitative")
            data_writer(quant, 'nfl_surface', 'quantitative')
            print("Data has been uploaded to the database. Go celebrate!")
        
    except Exception as e:
        print(f"An error occurred with your selection, '{output}': {e}")
        return None


       
def transform_concussion_data(output):
    from DataHandler import data_loader, data_shrinker, data_writer
    import polars as pl # type: ignore

    valid_outputs = ['tracking', 'summary']
    if output not in valid_outputs:
        raise ValueError(f"Invalid ouptut selection: '{output}'. Valid options are: '{valid_outputs}'")

    try: 
        track = data_loader(dataset='ngs_data', database='nfl_concussion')
        track = data_shrinker(track)
        track = column_corrector(track)
        track = angle_corrector(track)
        track = body_builder(track, 'ngs_data')
        track = velocity_calculator(track)
        track = impulse_calculator(track)
    
        if output == 'summary':
            summary =  path_calculator(track)
            del track # remove the large table from memory
            # Open and merge the qualitative data
            quals = data_loader('clean_quals', 'nfl_concussion')
            qual_quant = qual_quant_merger(quals, summary)
            
            print("Writing all quantitative and qualitative summary data to the database as summary_data. Wait.")
            data_writer(qual_quant, 'nfl_concussion', 'summary_data')
            print("Data has been uploaded to the database. Probably.")        

        elif output == 'tracking':
            # upload the physical data to the database for machine learning
            print("Writing the transformed table with physical parameters to the database as quantitative")
            data_writer(track, 'nfl_concussion', 'quantitative')
            print("Data has been uploaded to the database. Good for you!")
        
    except Exception as e:
        print(f"An error occurred with your selection, '{output}': {e}")
        return None




#############################################
def column_corrector(df):
    import polars as pl # type: ignore
    """
    Add a Play_Time column that acts like the 'time' column did in the injury dataset. 
    Each PlayKey will start at 0.0 and increase by 0.1 for each subsequent record.
    """
    df = df.with_columns([
        pl.concat_str([
            pl.col('gsisid').cast(pl.Int32).cast(pl.Utf8)
            , pl.lit('-')
            , pl.col('gamekey').cast(pl.Utf8)
            , pl.lit('-')
            , pl.col('playid').cast(pl.Utf8)
        ]).alias('PlayKey')
    ])
     
    
    df = df.select([
        'PlayKey'
        , 'time'
        , 'x'
        , 'y'
        , 'o'
        , 'dir'
        , 'gsisid'
        ]).rename({"time":"datetime"})

    df = df.sort(['PlayKey', 'datetime'])

    df = df.with_columns(
        (pl.arange(0, pl.len()) * 0.1).over("PlayKey").alias("time")
        ).with_columns([pl.col('gsisid').cast(pl.Int32)])  
    
    return df



def calculate_angle_difference(angle1, angle2):
    import numpy as np # type: ignore
    """
    Calculate the smallest angle difference between two angles 
    using trigonometric functions, accounting for edge cases.
    """
    sin_diff = np.sin(np.radians(angle2 - angle1))
    cos_diff = np.cos(np.radians(angle2 - angle1))
    return np.degrees(np.arctan2(sin_diff, cos_diff))

def angle_corrector(df):
    import polars as pl # type: ignore
    """
    Make corrections to angles to reduce fringe errors at 360
    """
    df = df.with_columns([
        ((pl.col("dir") + 180) % 360 - 180).alias("dir")
        , ((pl.col("o") + 180) % 360 - 180).alias("o")
    ]).with_columns(
        (calculate_angle_difference(pl.col("dir"), pl.col("o"))).abs().round(2).alias("Angle_Diff")
        )
    
    return df


def body_builder(df, df_name):
    import polars as pl # type: ignore
    from DataHandler import data_loader

    body_data = pl.DataFrame({
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
        if df_name == 'ngs_data':
            position = data_loader(dataset='positions', database='nfl_concussion')
            position = position.join(
                body_data
                , left_on='position'
                , right_on='position'
                , how='left'
                )
            
            df = df.join(
                position
                , on='gsisid'
                , how='left'
                ).drop_nulls(subset=['position'])
            

        elif df_name == 'tracking':
            position = data_loader(dataset='play_positions', database='nfl_surface')
            position = position.join(
                body_data
                , left_on='position'
                , right_on='position'
                , how='left'
                )

            df = df.join(
                position
                , left_on='PlayKey'
                , right_on='playkey'
                , how='left'
            ).drop_nulls(subset=['position']).drop(['event'])

            

        return df    
    
    except Exception as e: 
        print(f"An error occurred while loading the dataframe '{df_name}': {e}")
        return None




def velocity_calculator(df):
    import numpy as np # type: ignore
    import polars as pl # type: ignore
    """
    Using the (X,Y) and time columns, perform calculations based on the 
    difference between two rows to find displacement, speed, direction 
    of motion, velocity in x and y components, and the angular velocities 
    of the direction of motion and orientations 
    """
    
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
    import numpy as np # type: ignore
    import polars as pl # type: ignore
    """
    Using the (X,Y) and time columns, perform calculations based on the velocities and changes 
    in velocites along with player mass to get the momentum and impulse, a measure that can 
    be assessed along with medical data related to concussions and injuries
    """
    
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
    import polars as pl # type: ignore
    # This provides a summary table that can be integrated with the qualitative data

    # Calculate total distance and displacement for each PlayKey
    # Calculate total distance and displacement for each PlayKey
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
    from DataHandler import data_shrinker

    qual_quant = quals.join(quant, on="PlayKey", how="left")
    qual_quant = data_shrinker(qual_quant)

    return qual_quant