# Transform Functions
# This imports the clean data from the database and reduces the memory usage prior to processing the data
# This will take about 3 minutes to run


def transform_data():
    from DataHandler import data_loader, data_shrinker, data_writer

    # Transform the tracking data
    quant = data_loader('tracking')
    quant = data_shrinker(quant)
    quant = angle_corrector(quant)
    quant = dynamics_calculator(quant)
    quant = path_calculator(quant)

    # Open and merge the qualitative data
    quals = data_loader('qualitative')
    qual_quant = qual_quant_merger(quals, quant)
    print("Writing all quantitative and qualitative summary data to the database as summary_data")
    data_writer(qual_quant, 'nfl_surface', 'summary_data')
    
    del quant
    del quals





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
        ((pl.col("dir") + 180) % 360 - 180).alias("dir"),
        ((pl.col("o") + 180) % 360 - 180).alias("o")
    ]).with_columns(
        (calculate_angle_difference(pl.col("dir"), pl.col("o"))).abs().round(2).alias("angle_diff")
        )
    
    return df


def dynamics_calculator(df):
    import numpy as np
    import polars as pl
    """
    Using the (X,Y) and time columns, perform calculations based on the 
    difference between two rows to find displacement, speed, direction 
    of motion, velocity in x and y components, and the angular velocities 
    of the direction of motion and orientations 
    """

    df = df.with_columns([
        # Pre-calculate shifted values
        pl.col("x").shift(1).over("PlayKey").alias("prev_x"),
        pl.col("y").shift(1).over("PlayKey").alias("prev_y"),
        pl.col("time").shift(1).over("PlayKey").alias("prev_time"),
        pl.col("dir").shift(1).over("PlayKey").alias("prev_dir"),
        pl.col("o").shift(1).over("PlayKey").alias("prev_o")
    ]).with_columns([
        # Calculate time difference
        (pl.col("time") - pl.col("prev_time")).alias("dt"),
        # Calculate x and y differences
        (pl.col("x") - pl.col("prev_x")).alias("dx"),
        (pl.col("y") - pl.col("prev_y")).alias("dy")
    ]).with_columns([
        # Calculate displacement
        ((pl.col("dx")**2 + pl.col("dy")**2)**0.5).alias("dist")
    ]).with_columns([
        # Calculate speed
        (pl.col("dist") / pl.col("dt")).alias("speed"),
        # Calculate direction
        (np.degrees(np.arctan2(pl.col("dx"), pl.col("dy")))).alias("direction"),
        # Calculate velocity components
        (pl.col("dx") / pl.col("dt")).alias("vx"),
        (pl.col("dy") / pl.col("dt")).alias("vy"),
        # Calculate angular velocities
        ((pl.col("dir") - pl.col("prev_dir")) / pl.col("dt")).alias("omega_dir"),
        ((pl.col("o") - pl.col("prev_o")) / pl.col("dt")).alias("omega_o")
    ]).with_columns([
        ((pl.col("omega_dir") - pl.col("omega_o")).abs()).alias("d_omega")
    ]).drop([
        "prev_x", "prev_y", "prev_time", "prev_dir", "prev_o", "dt", "dx", "dy"
    ]).drop_nulls()


    return df


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
        ((pl.col("dir") + 180) % 360 - 180).alias("dir"),
        ((pl.col("o") + 180) % 360 - 180).alias("o")
    ]).with_columns(
        (calculate_angle_difference(pl.col("dir"), pl.col("o"))).abs().round(2).alias("angle_diff")
        )
    
    return df


def path_calculator(df):
    import polars as pl
    # Calculate total distance and displacement for each PlayKey
    result = df.select([
        "PlayKey",
        pl.col("dist").sum().over("PlayKey").alias("distance"),
        pl.col("x").first().over("PlayKey").alias("start_x"),
        pl.col("y").first().over("PlayKey").alias("start_y"),
        pl.col("x").last().over("PlayKey").alias("end_x"),
        pl.col("y").last().over("PlayKey").alias("end_y"), 
        pl.col("angle_diff").max().over("PlayKey").alias("max_angle_diff"),
        pl.col("angle_diff").mean().over("PlayKey").alias("mean_angle_diff"), 
        pl.col("speed").max().over("PlayKey").alias("max_speed"),
        pl.col("speed").mean().over("PlayKey").alias("mean_speed"),
        pl.col("omega_dir").max().over("PlayKey").alias("max_omega_dir"),
        pl.col("omega_dir").mean().over("PlayKey").alias("mean_omega_dir"),
        pl.col("omega_o").max().over("PlayKey").alias("max_omega_o"),
        pl.col("omega_o").mean().over("PlayKey").alias("mean_omega_o"), 
        pl.col("d_omega").max().over("PlayKey").alias("max_d_omega"),
        pl.col("d_omega").mean().over("PlayKey").alias("mean_d_omega") 
    ]).unique(subset=["PlayKey"])

    # Calculate the displacement
    result = result.with_columns([
        (((pl.col("end_x") - pl.col("start_x"))**2 + 
          (pl.col("end_y") - pl.col("start_y"))**2)**0.5)
        .alias("displacement")
    ]).with_columns([
        (pl.col("distance") - pl.col("displacement")).alias("path_diff")
    ])

     
    # Select only the required columns
    result = result.select(['PlayKey',
        'distance',
        'displacement',
        'path_diff',
        'max_angle_diff',
        'mean_angle_diff',
        'max_speed',
        'mean_speed',
        'max_omega_dir',
        'mean_omega_dir',
        'max_omega_o',
        'mean_omega_o',
        'max_d_omega',
        'mean_d_omega']).sort("PlayKey")

    return result

# Join the Qualitative with the Quantitative to create Summary Table
def qual_quant_merger(quals, quant):
    from DataHandler import data_shrinker
    qual_quant = quals.join(quant, on="PlayKey", how="left")
    qual_quant = data_shrinker(qual_quant)

    return qual_quant