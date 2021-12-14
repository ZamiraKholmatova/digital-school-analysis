import pickle
import sys
from bisect import bisect_left, bisect_right
from collections import namedtuple
from multiprocessing import Pool
from pathlib import Path
from shutil import copyfile

import numpy as np
from tqdm import tqdm

import pandas as pd
from numpy_ext import rolling_apply

Record = namedtuple("Record", ["profile_id", "educational_course_id", "date", "day_start", "day_end"])


class StatisticsCombiner:
    def __init__(self):
        self.delim = ","
        self.check_45_min = False

    def set_column_order(self):
        self.column_order = None

    def get_positions(self):
        self.p_id_o = self.column_order["profile_id"]
        self.c_at_o = self.column_order["created_at"]
        self.ec_id_o = self.column_order["educational_course_id"]

    def preprocess_course_id(self, id_):
        return id_

    def read_line(self, line):
        fields = line.strip().split(self.delim)
        userId = fields[self.p_id_o]
        createdAt = fields[self.c_at_o]
        date = pd.to_datetime(createdAt.split(" ")[0])
        externalId = self.preprocess_course_id(fields[self.ec_id_o])

        day_start = pd.to_datetime(createdAt)

        return Record(userId, externalId, date, day_start, day_start)

    def merge_into(self, user_information, key, record):
        day_start = record.day_start
        day_end = record.day_end
        if key not in user_information:
            user_information[key] = {
                "day_start": day_start,
                "day_end": day_end
            }
        else:
            day_information = user_information[key]
            if day_start < day_information["day_start"]:
                day_information["day_start"] = day_start
            if day_end > day_information["day_end"]:
                day_information["day_end"] = day_end

    def read_partition_chunks(self, partition_file):
        return pd.read_csv(
            partition_file,
            header=0,
            names=[
                "statisticsTypeId", "educational_course_id", "created_at", "externalUserId", "profile_id"
            ],
            usecols=["profile_id", "educational_course_id", "created_at"],
            dtype={"educational_course_id": "string"},
            parse_dates=["created_at"],
            chunksize=3000000
        )

    def map_partition(self, partition_file):

        dump_filename = Path(str(partition_file.absolute()) + "___preprocessed.tsv")

        if dump_filename.is_file():
            return

        def get_border_time(profile_id, educational_course_id, created_at):
            if profile_id[0] != profile_id[1] or \
                    educational_course_id[0] != educational_course_id[1] or \
                    np.abs(created_at[1] - created_at[0]) > np.timedelta64(45, 'm'):
                return created_at[1]

        def get_start_time(profile_id, educational_course_id, time):
            if profile_id[0] == profile_id[1] and \
                    educational_course_id[0] == educational_course_id[1]:
                if np.isnan(time[1]):
                    return time[0]
                else:  # this situation can occur only when current row has duration 0
                    if np.abs(time[1] - time[0]) < np.timedelta64(45, 'm') and self.check_45_min:
                        return time[0]

        def get_end_time(profile_id, educational_course_id, time):
            if profile_id[0] == profile_id[1] and \
                    educational_course_id[0] == educational_course_id[1]:
                return time[1]

        col_order = ["profile_id", "educational_course_id", "created_at"]
        cols_to_write = ["profile_id", "educational_course_id", "date", "start_time", "end_time", "dt"]

        print(partition_file)

        for chunk in tqdm(self.read_partition_chunks(partition_file)):
            chunk.dropna(subset=col_order, inplace=True)
            chunk["educational_course_id"] = chunk["educational_course_id"].apply(self.preprocess_course_id)
            chunk.sort_values(by=col_order, inplace=True)


            backup = chunk.copy()

            reversed_chunk = chunk[::-1]

            chunk["start_time_"] = rolling_apply(
                get_border_time, 2, chunk["profile_id"].values,
                chunk["educational_course_id"].values, chunk["created_at"].values
            )
            chunk["end_time_"] = np.flip(rolling_apply(
                get_border_time, 2, reversed_chunk["profile_id"].values,
                reversed_chunk["educational_course_id"].values, reversed_chunk["created_at"].values
            ))

            chunk.dropna(subset=["start_time_", "end_time_"], how="all", inplace=True)

            chunk["start_time"] = rolling_apply(
                get_start_time, 2,
                chunk["profile_id"].values,
                chunk["educational_course_id"].values,
                chunk["start_time_"].values
            )

            chunk["end_time"] = rolling_apply(
                get_end_time, 2,
                chunk["profile_id"].values,
                chunk["educational_course_id"].values,
                chunk["end_time_"].values
            )

            chunk.dropna(subset=["start_time", "end_time"], inplace=True)
            chunk["date"] = chunk["created_at"].apply(lambda x: str(x).split(" ")[0])
            chunk["dt"] = (chunk["end_time"] - chunk["start_time"]).map(lambda x: x.seconds)

            chunk[cols_to_write].to_csv(dump_filename, sep="\t", mode="a", index=False, header=False)



def iterate_partition(partition):
    for key, day_information in partition.items():
        profile_id, educational_course_id, date = key
        yield profile_id, educational_course_id, date, day_information
    # for profile_id, course_information in partition.items():
    #     for educational_course_id, date_information in course_information.items():
    #         for date, day_information in date_information.items():
    #             yield profile_id, educational_course_id, date, day_information


def reduce_partitions(files, save_location, date_checkpoints):

    for file in files:
        filename = Path(str(file.absolute()) + "___preprocessed.tsv")
        if filename.is_file():
            dst_path = save_location.joinpath(filename.name)
            copyfile(filename, dst_path)


class StatisticsCombiner_Uchi(StatisticsCombiner):
    def __init__(self):
        super().__init__()


class StatisticsCombiner_Uchi_New(StatisticsCombiner):
    def __init__(self):
        super().__init__()
        self.delim = "\t"

    def read_partition_chunks(self, partition_file):
        return pd.read_csv(
            partition_file,
            header=0,
            names=[
                "statisticsTypeId", "educational_course_id", "created_at", "externalUserId", "profile_id"
            ],
            sep=self.delim,
            usecols=["profile_id", "educational_course_id", "created_at"],
            parse_dates=["created_at"],
            chunksize=3000000
        )


class StatisticsCombiner_FoxFord(StatisticsCombiner):
    def __init__(self):
        super().__init__()
        self.check_45_min = True

    def set_column_order(self):
        self.column_order = {"profile_id": 1, "created_at": 2, "educational_course_id": 5}

    def preprocess_course_id(self, id_):
        return "/".join(id_.split("/")[:5])

    def read_partition_chunks(self, partition_file):
        return pd.read_csv(
            partition_file,
            header=0,
            names=[
                "systemcode", "profile_id", "created_at", "statisticstypeid", "status", "educational_course_id"
            ],
            usecols=["profile_id", "educational_course_id", "created_at"],
            parse_dates=["created_at"],
            chunksize=3000000
        )


class StatisticsCombiner_MEO(StatisticsCombiner):
    def __init__(self):
        super().__init__()

    def read_partition_chunks(self, partition_file):
        return pd.read_csv(
            partition_file,
            header=0,
            names=[
                "profile_id", "educational_course_id", "date", "dt"
            ],
            sep=";",
            usecols=["profile_id", "educational_course_id", "date", "dt"],
            parse_dates=["date"],
            dtype={"educational_course_id": "string"},
            chunksize=3000000
        )

    # def read_partition_chunks(self, partition_file):
    #     return pd.read_csv(
    #         partition_file,
    #         header=0,
    #         names=[
    #             "profile_id", "start_time", "end_time", "educational_course_id"
    #         ],
    #         sep="|",
    #         usecols=["profile_id", "educational_course_id", "start_time", "end_time"],
    #         parse_dates=["start_time", "end_time"],
    #         dtype={"educational_course_id": "string"},
    #         chunksize=3000000
    #     )

    def map_partition(self, partition_file):

        dump_filename = Path(str(partition_file.absolute()) + "___preprocessed.tsv")

        if dump_filename.is_file():
            return

        cols_to_write = ["profile_id", "educational_course_id", "date", "start_time", "end_time", "dt"]

        print(partition_file)

        for chunk in tqdm(self.read_partition_chunks(partition_file)):
            # chunk.dropna(subset=["profile_id", "educational_course_id", "start_time", "end_time"], inplace=True)
            # chunk["date"] = chunk["start_time"].apply(lambda x: str(x).split(" ")[0])
            # chunk["dt"] = (chunk["end_time"] - chunk["start_time"]).map(lambda x: x.seconds)

            chunk.dropna(subset=["profile_id", "educational_course_id", "date", "dt"], inplace=True)
            chunk["start_time"] = chunk["date"]
            chunk["end_time"] = chunk["date"]
            chunk["dt"] = chunk["dt"].apply(lambda x: pd.to_timedelta(x+":00").seconds)

            chunk[cols_to_write].to_csv(dump_filename, sep="\t", mode="a", index=False, header=False)


class StatisticsCombiner_1C_ND(StatisticsCombiner):
    def __init__(self):
        super().__init__()

    def read_partition_chunks(self, partition_file):
        return pd.read_csv(
            partition_file,
            header=0,
            names=[
                "id","profile_id","statistic_type_id","created_at","status","educational_course_id","updated_at"
            ],
            usecols=["profile_id", "educational_course_id", "created_at"],
            parse_dates=["created_at"],
            chunksize=3000000
        )


def map_partitions_uchi(file):
    uchi_combiner = StatisticsCombiner_Uchi()
    uchi_combiner.map_partition(file)


def map_partitions_uchi_new(file):
    uchi_combiner = StatisticsCombiner_Uchi_New()
    uchi_combiner.map_partition(file)


def map_partitions_foxford(file):
    foxford_combiner = StatisticsCombiner_FoxFord()
    foxford_combiner.map_partition(file)


def map_partitions_meo(file):
    meo_combiner = StatisticsCombiner_MEO()
    meo_combiner.map_partition(file)


def map_partitions_1c_nd(file):
    combiner = StatisticsCombiner_1C_ND()
    combiner.map_partition(file)


def preprocess(files, save_location, partition_fn):

    with Pool(4) as p:
        p.map(partition_fn, files)

    # for file in files:
    #     partition_fn(file)

    save_location = Path(save_location)

    if not save_location.is_dir():
        save_location.mkdir()

    reduce_partitions(
        files,
        save_location,
        date_checkpoints=[
            (pd.to_datetime("2021-10-01"), pd.to_datetime("2021-10-15")),
            (pd.to_datetime("2021-10-15"), pd.to_datetime("2021-11-01")),
            (pd.to_datetime("2021-11-01"), pd.to_datetime("2021-11-15")),
            (pd.to_datetime("2021-11-15"), pd.to_datetime("2021-12-01"))
        ]
    )

    # pickle.dump(user_information, open(save_location.joinpath(f"preprocessed_{timestamp}.pkl", "wb")))
    #
    # with open(save_location.joinpath(f"preprocessed_{timestamp}.csv"), "w") as sink:
    #     sink.write("profile_id,educational_course_id,date,day_start,day_end,is_active\n")
    #     for profile_id, course_information in tqdm(user_information.items()):
    #         for educational_course_id, date_information in course_information.items():
    #             for date, day_information in date_information.items():
    #                 day_start = day_information["day_start"]
    #                 day_end = day_information["day_end"]
    #                 is_active = (day_end - day_start).seconds > 600
    #                 sink.write(
    #                     f"{profile_id},{educational_course_id},{date},{day_start},{day_end},{is_active}\n"
    #                 )


def get_files_from_dir(statistics_folder):
    return [file for file in statistics_folder.iterdir() if not file.name.startswith(".") and not file.name.startswith("_") and file.name.endswith(".csv")]


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform")
    parser.add_argument("--path")
    args = parser.parse_args()

    processing_fns = {
        "uchi": map_partitions_uchi,
        "uchi_new": map_partitions_uchi_new,
        "foxford": map_partitions_foxford,
        "meo": map_partitions_meo,
        "1c_nd": map_partitions_1c_nd,
    }

    platform = args.platform
    if platform in {"uchi", "uchi_new", "foxford", "meo"}:
        statistics_folder = Path(args.path)
        files = get_files_from_dir(statistics_folder)
        preprocess(files, statistics_folder.joinpath("preprocessed"), processing_fns[platform])
    elif platform == "1c_nd":
        statistics_file = Path(args.path)
        preprocess([statistics_file], statistics_file.parent.joinpath("preprocessed"), processing_fns[platform])

