from collections import namedtuple
from multiprocessing import Pool
from pathlib import Path

import numpy as np
from tqdm import tqdm

import pandas as pd
from numpy_ext import rolling_apply


Record = namedtuple("Record", ["profile_id", "educational_course_id", "date", "day_start", "day_end"])


class StatisticsCombiner:
    def __init__(self):
        self.delim = ","
        self.check_45_min = False

    def preprocess_course_id(self, id_):
        return id_

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

    def get_dump_path(self, path):
        preprocessed_folder = path.parent.joinpath("preprocessed")
        if not preprocessed_folder.is_dir():
            preprocessed_folder.mkdir()
        filename = path.name
        dump_filename = preprocessed_folder.joinpath(filename + "___preprocessed.csv.bz2")
        return dump_filename

    def append_to_dump_file(self, path, content):
        content.to_csv(path, mode="a", index=False, header=False)

    def map_partition(self, partition_file: Path):

        dump_filename = self.get_dump_path(partition_file)

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

            self.append_to_dump_file(dump_filename, chunk[cols_to_write])


class StatisticsCombiner_Uchi(StatisticsCombiner):
    def __init__(self):
        super().__init__()


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

        dump_filename = self.get_dump_path(partition_file)

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

            self.append_to_dump_file(dump_filename, chunk[cols_to_write])


class StatisticsCombiner_1C_ND(StatisticsCombiner):
    def __init__(self):
        super().__init__()

    def read_partition_chunks(self, partition_file):
        return pd.read_csv(
            partition_file,
            header=0,
            names=[
                "profile_id","educational_course_id","created_at", #"statistic_type_id"
            ],
            usecols=["profile_id", "educational_course_id", "created_at"],
            parse_dates=["created_at"],
            chunksize=3000000
        )


def map_partitions_uchi(file):
    uchi_combiner = StatisticsCombiner_Uchi()
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


def preprocess(files, partition_fn):

    files = list(files)

    if len(files) == 0:
        return

    with Pool(4) as p:
        p.map(partition_fn, files)

    # for file in files:
    #     partition_fn(file)


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
        "foxford": map_partitions_foxford,
        "meo": map_partitions_meo,
        "1c_nd": map_partitions_1c_nd,
    }

    platform = args.platform
    if platform in {"uchi", "uchi_new", "foxford", "meo"}:
        statistics_folder = Path(args.path)
        files = get_files_from_dir(statistics_folder)
        preprocess(files, processing_fns[platform])
    elif platform == "1c_nd":
        statistics_file = Path(args.path)
        preprocess([statistics_file], processing_fns[platform])

