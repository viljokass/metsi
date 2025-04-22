'''
convert_to_opt.py

This script converts the data.xda file into alternatives.csv and alternatives_key.csv.
Basically just what the R script did, but in Python.

Therefore, it is assumed that the "data.xda" file is inside the target directory.
'''

import os
import polars as pl
import operator

# column names for our DataFrame
data_names = [
    "identifier",
    "area",
    "npv_1_percent",
    "npv_2_percent",
    "npv_3_percent",
    "npv_4_percent",
    "npv_5_percent",
    
    "stock_0",
    "stock_5",
    "stock_10",
    "stock_20",
    "stock_25",
    
    "harvest_5",
    "harvest_10",
    "harvest_20",
    "harvest_25",
    
    "harvest_value_5",
    "harvest_value_10",
    "harvest_value_20",
    "harvest_value_25",
    
    "harvest_value_first_5",
    "harvest_value_below_5",
    "harvest_value_above_5",
    "harvest_value_even_5",
    "harvest_value_clearcut_5",
    
    "harvest_value_first_10",
    "harvest_value_below_10",
    "harvest_value_above_10",
    "harvest_value_even_10",
    "harvest_value_clearcut_10",
    
    "harvest_value_first_20",
    "harvest_value_below_20",
    "harvest_value_above_20",
    "harvest_value_even_20",
    "harvest_value_clearcut_20",
    
    "harvest_value_first_25",
    "harvest_value_below_25",
    "harvest_value_above_25",
    "harvest_value_even_25",
    "harvest_value_clearcut_25",
    
    "stock_1_0",
    "stock_1_5",
    "stock_1_10",
    "stock_1_20",
    "stock_1_25",
    
    "stock_2_0",
    "stock_2_5",
    "stock_2_10",
    "stock_2_20",
    "stock_2_25",
    
    "stock_3_0",
    "stock_3_5",
    "stock_3_10",
    "stock_3_20",
    "stock_3_25",
    
    "stock_4_0",
    "stock_4_5",
    "stock_4_10",
    "stock_4_20",
    "stock_4_25",
    
    "stock_5_0",
    "stock_5_5",
    "stock_5_10",
    "stock_5_20",
    "stock_5_25",
    
    "stock_6_0",
    "stock_6_5",
    "stock_6_10",
    "stock_6_20",
    "stock_6_25",
    
    "stock_7_0",
    "stock_7_5",
    "stock_7_10",
    "stock_7_20",
    "stock_7_25",
    
    "stock_8_0",
    "stock_8_5",
    "stock_8_10",
    "stock_8_20",
    "stock_8_25",
    
    "stock_9_0",
    "stock_9_5",
    "stock_9_10",
    "stock_9_20",
    "stock_9_25",
    
    "stock_10_0",
    "stock_10_5",
    "stock_10_10",
    "stock_10_20",
    "stock_10_25",
    
    "stock_11_0",
    "stock_11_5",
    "stock_11_10",
    "stock_11_20",
    "stock_11_25",
    
    "stock_12_0",
    "stock_12_5",
    "stock_12_10",
    "stock_12_20",
    "stock_12_25",
    
    "stock_13_0",
    "stock_13_5",
    "stock_13_10",
    "stock_13_20",
    "stock_13_25",
    
    "stock_14_0",
    "stock_14_5",
    "stock_14_10",
    "stock_14_20",
    "stock_14_25",
    
    "stock_15_0",
    "stock_15_5",
    "stock_15_10",
    "stock_15_20",
    "stock_15_25",
    
    "stock_16_0",
    "stock_16_5",
    "stock_16_10",
    "stock_16_20",
    "stock_16_25",
    
    "stock_17_0",
    "stock_17_5",
    "stock_17_10",
    "stock_17_20",
    "stock_17_25",
    
    "stock_18_0",
    "stock_18_5",
    "stock_18_10",
    "stock_18_20",
    "stock_18_25",
    
    "stock_19_0",
    "stock_19_5",
    "stock_19_10",
    "stock_19_20",
    "stock_19_25",
    
    "stock_20_0",
    "stock_20_5",
    "stock_20_10",
    "stock_20_20",
    "stock_20_25",
    
    "stock_21_0",
    "stock_21_5",
    "stock_21_10",
    "stock_21_20",
    "stock_21_25",
    
    "stock_22_0",
    "stock_22_5",
    "stock_22_10",
    "stock_22_20",
    "stock_22_25",
    
    "stock_23_0",
    "stock_23_5",
    "stock_23_10",
    "stock_23_20",
    "stock_23_25",
    
    "stock_24_0",
    "stock_24_5",
    "stock_24_10",
    "stock_24_20",
    "stock_24_25",
    
    "stock_25_0",
    "stock_25_5",
    "stock_25_10",
    "stock_25_20",
    "stock_25_25",
    
    "stock_26_0",
    "stock_26_5",
    "stock_26_10",
    "stock_26_20",
    "stock_26_25",
    
    "stock_27_0",
    "stock_27_5",
    "stock_27_10",
    "stock_27_20",
    "stock_27_25",
    
    "stock_28_0",
    "stock_28_5",
    "stock_28_10",
    "stock_28_20",
    "stock_28_25",
    
    "stock_29_0",
    "stock_29_5",
    "stock_29_10",
    "stock_29_20",
    "stock_29_25",
    
    "stock_30_0",
    "stock_30_5",
    "stock_30_10",
    "stock_30_20",
    "stock_30_25",
    
    "stock_31_0",
    "stock_31_5",
    "stock_31_10",
    "stock_31_20",
    "stock_31_25",
    
    "stock_32_0",
    "stock_32_5",
    "stock_32_10",
    "stock_32_20",
    "stock_32_25",
    
    "stock_33_0",
    "stock_33_5",
    "stock_33_10",
    "stock_33_20",
    "stock_33_25",
    
    "stock_34_0",
    "stock_34_5",
    "stock_34_10",
    "stock_34_20",
    "stock_34_25",
    
    "stock_35_0",
    "stock_35_5",
    "stock_35_10",
    "stock_35_20",
    "stock_35_25",
    
    "stock_36_0",
    "stock_36_5",
    "stock_36_10",
    "stock_36_20",
    "stock_36_25",
    
    "stock_37_0",
    "stock_37_5",
    "stock_37_10",
    "stock_37_20",
    "stock_37_25",
    
    "stock_38_0",
    "stock_38_5",
    "stock_38_10",
    "stock_38_20",
    "stock_38_25",
]

# Parameters for adjusting the data read.
w = 230  # the width of the DataFrame
harvest_value_start = 14
harvest_value_end = 20
harvest_start = 20  # where harvests that do something start
harvest_end = 40    # where harvests that do something end


class ConversionException(Exception):
    '''Exception for when conversion fails'''

# Export this function out
def convert_to_opt(data_dir: str, usernum: int):

    # Make sure the necessary data exists
    if not os.path.exists(f"{data_dir}/data.xda"):
        raise ConversionException(f"There's no \"data.xda\" in {data_dir}.")
    # Read the CSV in
    df = pl.read_csv(
        f"{data_dir}/data.xda",
        separator='\t',
        has_header=False,
        infer_schema=False
    ).with_columns(
        # First cast all columns from 1 to 183 to pl Float
        pl.nth(range(1, w)).cast(
            pl.Float64,
            strict=True),
        # Then cast the first one to Int64
        pl.nth(0).cast(
            pl.Int64,
            strict=True))

    # Multiply columns 2 to w with the area column.
    df = df.with_columns((pl.nth(range(2, w)) * pl.nth(1)))

    # Name the columns
    df = df.rename(
        lambda column_name: data_names[int(column_name.split("_")[1])-1])

    # Init the dataframe for filtering out the zero-profit treatments besides the first one
    df_filtered = df.schema.to_frame()

    # Define column schema for the alternatives_key DataFrame
    alt_key_schema = pl.Schema({
        "holding": pl.Int64,
        "unit": pl.Int64,
        "schedule": pl.Int64,
        "treatment": pl.String
    })

    # Init the dataframes that we write out into CSVs
    alt_key_csv = alt_key_schema.to_frame()

    # Initiate the schedule Series, to which we store the schedule numbers in order
    schedules = pl.Series("schedule", [], dtype=pl.Int64)

    # For each unique identifier in the data:
    for identifier in df["identifier"].unique(maintain_order=True):
        # Get the data for the id
        rows_per_id = df.filter(pl.col("identifier") == identifier)

        # Remove rows, whose treatment sums are 0, except for the first one.
        # (Remove all, whose treatment sums are 0, and add the first one back in)
        rows_per_id = pl.concat([
            # The first row of the DataFrame
            rows_per_id.head(1),
            # The rest of the DataFrame with the zeroes removed
            rows_per_id.remove(pl.fold(
                acc=pl.lit(0),
                function=operator.add,
                # index 20 (R) is where harvests that do something start
                # index 40 (R) is where harvests that do something end
                exprs=pl.nth(range(harvest_start, harvest_end))
                # TODO: check this in a more "floating point" manner with epsilons etc.
            ) == 0.0)
        ])

        # Concantenate the filtered DataFrame to the larger dataframe
        df_filtered = pl.concat([
            df_filtered,
            rows_per_id
        ])

        # Add the "donothing" treatment to the alternatives_key DataFrame
        alt_key_csv = pl.concat([
            alt_key_csv,
            pl.DataFrame({
                "holding": [usernum],
                "unit": [identifier],
                "schedule": [0],
                "treatment": ["donothing"]
            }, schema=alt_key_schema)
        ])

        # Initialize the schedule list and a running schedule number
        schedule_list = [0]
        schedule_num = 0
        # For each row under the identifier:
        for row in rows_per_id.iter_rows():
            if schedule_num == 0:
                schedule_num += 1
                continue
            treatments = row[harvest_start:harvest_end]

            # Initialize treatment key and treatment counter
            treatment_key = ""
            treatment_count = 0
            # For each treatment in treatments
            for treatment in treatments:
                if treatment > 0:
                    treatment_name = data_names[harvest_start + treatment_count].split('_')
                    treatment_key += treatment_name[2] + \
                        "_" + treatment_name[3] + " + "
                treatment_count += 1

            # Add the new treatment to the DataFrame
            alt_key_csv = pl.concat([
                alt_key_csv,
                pl.DataFrame({
                    "holding": [usernum],
                    "unit":	[identifier],
                    "schedule":	[schedule_num],
                    "treatment": [treatment_key[:-3]]
                }, schema=alt_key_schema)
            ])

            # Append the schedule number to the schedule list
            schedule_list.append(schedule_num)

            schedule_num += 1

        # Append the schedules of this stand to the schedule series
        schedules = schedules.append(
            pl.Series("schedule", schedule_list, dtype=pl.Int64))

    schedules = schedules.rechunk()

    # Kinda silly for this to be here, but for completeness, initiate a Series that contains just the holding number
    holdings = pl.Series(
        "holding",
        map(lambda x: usernum, range(schedules.shape[0])),
        dtype=pl.Int64
    )

    # Maybe not necessary to rechunk but might improve writing speeds?
    alt_key_csv = alt_key_csv.rechunk()
    # Output
    alt_key_csv.write_csv(f"{data_dir}/alternatives_key.csv", separator=',')

    # Construct the alternatives.csv
    alt_csv = pl.concat([
        holdings.to_frame(),
        df_filtered.select(pl.col("identifier")).rename(
            {"identifier": "unit"}),
        schedules.to_frame(),
        df_filtered.select(
            # Per the R script:
            # indices 1-11 are the id, area, net present values and total volumes (of all trees together)
            # 15-18 are total harvest values from the cutting years (2, 7, 17, 22)
            # 33-184 are the total volumes for each tree species
            pl.nth(range(2, 12)),
            pl.nth(range(16, harvest_value_end)),
            pl.nth(range(harvest_end, w))
        )
        # Concatenate in terms of columns insead of rows
    ], how='horizontal')

    # Output
    # Again, might or might not help with write speed
    alt_csv = alt_csv.rechunk()
    alt_csv.write_csv(f"{data_dir}/alternatives.csv", separator=',')


if __name__ == "__main__":
    # If this script is run as a "main" script, import necessary tools.
    import sys
    import argparse

    # init the argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", dest="dir", help="Target directory")
    args = parser.parse_args(args=None if sys.argv[1:] else ["--help"])

    # Hop to the conversion function
    convert_to_opt(args.dir, 1)
