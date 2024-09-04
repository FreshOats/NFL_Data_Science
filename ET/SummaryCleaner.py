# Summary Cleaner\
from QualitativeCleaner import *
injury_qual = clean_injury_qual()
group_dir = "F:/Data/Clean_Data/injury_output/"
output_dir = "F:/Data/Clean_Data/"


############################
def injury_summary_maker(group_dir, injury_qual, output_dir):
    """
    Joins the qualitative and quantitative summary data
    """
    import polars as pl # type: ignore
    pl.enable_string_cache()
    import os
    
    #Write    
    summary_file = "Summary_Injuries.parquet"
    summary_path = os.path.join(output_dir, summary_file)

    quant = collect_summaries(group_dir)
    # quals = pl.read_parquet(qual_path)
    summary_df = injury_qual.join(quant, on="PlayKey", how="inner")

    summary_df.write_parquet(summary_path)
    print(f"Saved the full summary with qualitative and quantitative features at {summary_path}")
    # return summary_df


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

    return summary_df


def summary_calculator(df):
    """
    The df input to this function is the tracking data chunk already formatted as a polars dataframe. 
    
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


    # Calculate the displacement and the difference between the distance and displacement
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


######
# Concussion Summary Code

def process_and_save_concussion_data(source_dir, concussion_output_dir):
    from TrackingCleaner import column_corrector, angle_corrector, body_builder_conc, velocity_calculator, impulse_calculator
    from DataHandler import data_shrinker
    import os
    import polars as pl #type: ignore
    import time 

    start_time = time.time()
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
            
            output_file_path = os.path.join(concussion_output_dir, file.replace(".csv", ".parquet"))

            df.write_parquet(output_file_path)

            print(f"Processed and saved: {output_file_path}")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"For fuck's sake that took a {execution_time} seconds. Finally done processing and saving the concussion files.")