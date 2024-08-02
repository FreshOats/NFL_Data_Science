# Transform Functions
# This imports the clean data from the database and reduces the memory usage prior to processing the data
# This will take about 3 minutes to run


def transform_injury_data():
    from DataHandler import data_loader, data_shrinker, data_writer

    # Transform the tracking data
    quant = data_loader('tracking')
    quant = data_shrinker(quant)
    quant = angle_corrector(quant)
    quant = velocity_calculator(quant)
    quant = acceleration_calculator(quant)
    quant = path_calculator(quant) # Needs to be updated with acceleration data

    # Open and merge the qualitative data
    quals = data_loader('qualitative')
    qual_quant = qual_quant_merger(quals, quant)
    print("Writing all quantitative and qualitative summary data to the database as summary_data")
    data_writer(qual_quant, 'nfl_surface', 'summary_data')
    
    del quant
    del quals



def transform_concussion_data():
    from DataHandler import data_loader, data_shrinker, data_writer

    # transform the ngs_data







#############################################
def calculate_angle_difference(angle1, angle2):
    import numpy as np
    """
    Calculate the smallest angle difference between two angles 
    using trigonometric functions, accounting for edge cases.
    """
    sin_diff = np.sin(np.radians(angle2 - angle1))
    cos_diff = np.cos(np.radians(angle2 - angle1))
    return np.degrees(np.arctan2(sin_diff, cos_diff))

def angle_corrector(df):
    import polars as pl
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


def velocity_calculator(df):
    import numpy as np
    import polars as pl
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

def acceleration_calculator(df): 
    '''
    This will require annother process as was done with the velocity_calculator, 
    only this time, I will be using the changes in velocity along with the changes in time 
    to find the linear and angular accelerations. 
    '''
    def acceleration_calculator(df):
        import numpy as np
        import polars as pl
        """
        Using the (X,Y) and time columns, perform calculations based on the 
        difference between two rows to find displacement, speed, direction 
        of motion, velocity in x and y components, and the angular velocities 
        of the direction of motion and orientations 
        """
        
        return df.with_columns([
            # Pre-calculate shifted values for linear and angular velocities
            pl.col("vx").shift(1).over("PlayKey").alias("prev_vx")
            , pl.col("vy").shift(1).over("PlayKey").alias("prev_vy")
            , pl.col("omega_dir").shift(1).over("PlayKey").alias("prev_omega_dir")
            , pl.col("omega_o").shift(1).over("PlayKey").alias("prev_omega_o")
        
        ]).with_columns([
            # Calculate ax and ay from velocity differences over time
            ((pl.col("vx") - pl.col("prev_vx")) / 0.1).alias("ax")
            , ((pl.col("vy") - pl.col("prev_vy")) / 0.1).alias("ay")
            # Calculate angular accelerations
            , ((pl.col("omega_dir") - pl.col("prev_omega_dir")) / 0.1).alias("alpha_dir")
            , ((pl.col("omega_o") - pl.col("prev_omega_o")) / 0.1).alias("alpha_o")
        ]).drop([
            "prev_omega_dir", "prev_omega_o", "prev_vx", "prev_vy"
        ])

def force_calculator(df):
    '''
    This will analyze the masses, heights, and shoulder widths of the players per position.
    Using this information, we can calculate the moment of inertia for any angular data.
    I would like to show the changes in force as a function of time and see how the balance
    of the angular and linear forces impact the injury
    ''' 


def path_calculator(df):
    import polars as pl
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
        , pl.col("a_magnitude").max().over("PlayKey").alias("Max_Accel")
        , pl.col("a_magnitude").mean().over("PlayKey").alias("Mean_Accel")
        , pl.col("omega_dir").max().over("PlayKey").alias("Max_omega_dir")
        , pl.col("omega_dir").mean().over("PlayKey").alias("Mean_omega_dir")
        , pl.col("omega_o").max().over("PlayKey").alias("Max_omega_o")
        , pl.col("omega_o").mean().over("PlayKey").alias("Mean_omega_o")
        , pl.col("omega_diff").max().over("PlayKey").alias("Max_d_omega")
        , pl.col("omega_diff").mean().over("PlayKey").alias("Mean_d_omega")
        , pl.col("alpha_dir").max().over("PlayKey").alias("Max_alpha_dir")
        , pl.col("alpha_dir").mean().over("PlayKey").alias("Mean_alpha_dir")
        , pl.col("alpha_o").max().over("PlayKey").alias("Max_alpha_o")
        , pl.col("alpha_o").mean().over("PlayKey").alias("Mean_alpha_o")
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
        , 'Max_omega_dir'
        , 'Mean_omega_dir'
        , 'Max_omega_o'
        , 'Mean_omega_o'
        , 'Max_d_omega'
        , 'Mean_d_omega'
    ]).sort("PlayKey")


    return result

# Join the Qualitative with the Quantitative to create Summary Table
def qual_quant_merger(quals, quant):
    from DataHandler import data_shrinker
    qual_quant = quals.join(quant, on="PlayKey", how="left")
    qual_quant = data_shrinker(qual_quant)

    return qual_quant