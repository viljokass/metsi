"""Writes the stands' carbon storages from different years as a list whose values represent different treatment plans.

Arguments:
    -d: Data directory. The directory that has the forest' data. Assumes that the directory has the following files:
        - alternatives.csv
        - alternatives_key.csv
        - trees.json.
        Defaults to 'C:/MyTemp/code/UTOPIA/alternatives/select'.
"""


import json
import os
from pathlib import Path
from sys import platform

import numpy as np
import polars as pl


class CarbonJsonException(Exception):
    '''
    '''


def write_carbon_json(data_dir: str):

    if not os.path.exists(f"{data_dir}/alternatives.csv"):
        raise CarbonJsonException(f"There's no alternatives.csv in {data_dir}")

    if not os.path.exists(f"{data_dir}/alternatives_key.csv"):
        raise CarbonJsonException(
            f"There's no alternatives_key.csv in {data_dir}")

    if not os.path.exists(f"{data_dir}/trees.json"):
        raise CarbonJsonException(f"There's no trees.json in {data_dir}")

    # reading the csv files, infer_schema_length's high number makes it slower but is necessary
    df = pl.read_csv(Path(f"{data_dir}/alternatives.csv"),
                     schema_overrides={"unit": pl.Float64}, infer_schema_length=10000)
    df_key = pl.read_csv(
        Path(f"{data_dir}/alternatives_key.csv"), schema_overrides={"unit": pl.Float64})
    unique_units = df_key.unique(
        ["unit"], maintain_order=True).get_column("unit")

    if platform == "win32":
        with Path.open(f"{data_dir}/trees.json", "r") as f:
            trees = json.load(f)

    if platform == "linux":
        with Path(f"{data_dir}/trees.json").open(mode="r") as f:
            trees = json.load(f)

    num_stands = 0
    schedules = []
    for _ in trees:
        s_dict = trees[str(unique_units[num_stands])]
        num_stands = num_stands + 1
        for s in s_dict:
            if s not in schedules:
                schedules.append(s)
    num_schedules = len(schedules)

    # constants from https://doi.org/10.1016/j.foreco.2003.07.008, https://doi.org/10.1007/s13280-023-01833-4
    a = {'pine': 0.7018, 'spruce': 0.7406, 'deciduous': 0.5616}
    b = {'pine': 0.0058, 'spruce': 0.1494, 'deciduous': -0.0179}

    # compute the CO2 storage from each year into different lists by year (unit Mg CO2)
    # also gather the CO2 storage of each stand of each schedule of each year to a dict if needed in something
    carbon_storage_per_unit = {}
    for i in range(num_stands):
        carbon_storage_per_schedule = {}
        carbon_dict = {
            0: [],
            5: [],
            10: [],
            20: [],
            25: []
        }
        # Get the stand data of a stand for further processing (break it down)
        stand_data = df.filter(pl.col("unit") == unique_units[i])

        for j in range(num_schedules):
            carbon_storage_per_year = {}
            # get schedule j tree data from each year
            years = trees[str(unique_units[i])].get(str(j), None)
            # we only want data from planning years
            event_years = [0, 5, 10, 20, 25]
            if years is not None:
                for year in years:
                    # check that the year is an event year because there is data from every 5 years
                    if int(year) not in event_years:
                        continue

                    # get the ages of the pines and spruces, defaults to 0 if no trees of that species
                    t_pine = years[year].get(str(1), 0)
                    t_spruce = years[year].get(str(2), 0)

                    # get the volumes of all the different species of trees
                    volumes = stand_data.filter(
                        (pl.col("schedule") == j)
                    )

                    # get the volumes of pines and spruces
                    vol_pine = volumes[f"stock_1_{year}"][0]
                    vol_spruce = volumes[f"stock_2_{year}"][0]

                    # compute CO2 storage of pine and spruce trees
                    CO2_tons_pine_spruce = (vol_pine * (a['pine'] + b['pine'] * np.exp(-0.01 * t_pine))
                                            + vol_spruce * (a['spruce'] + b['spruce'] * np.exp(-0.01 * t_spruce))) * 0.5 * (44/12)

                    # compute CO2 storage of other kinds of trees
                    CO2_tons_others = 0
                    for k in range(3, 39):
                        CO2_tons_others = CO2_tons_others + (
                            volumes[f"stock_{k}_{year}"][0]
                            * (a['deciduous'] + b['deciduous'] * np.exp(-0.01 * years[year].get(str(k), 0)))) * 0.5 * (44/12)

                    # total CO2 storage
                    CO2_tons = CO2_tons_pine_spruce + CO2_tons_others
                    carbon_storage_per_year[int(year)] = CO2_tons
                    carbon_dict[int(year)].append(CO2_tons)
            else:
                carbon_storage_per_year = {0: 0, 5: 0, 10: 0, 20: 0, 25: 0}
                for year in event_years:
                    carbon_dict[year].append(0)

        carbon_storage_per_unit[unique_units[i]] = carbon_dict

    if platform == "win32":
        with Path.open(f"{data_dir}/carbon.json", "w") as f:
            json.dump(carbon_storage_per_unit, f)

    if platform == "linux":
        with Path(f"{data_dir}/carbon.json").open(mode="w") as f:
            json.dump(carbon_storage_per_unit, f)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", dest="dir", default="C:/MyTemp/code/UTOPIA/alternatives/select")
    args = parser.parse_args()
    data_dir = args.dir

    write_carbon_json(data_dir)
