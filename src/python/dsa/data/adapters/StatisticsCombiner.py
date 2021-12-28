from collections import namedtuple
from multiprocessing import Pool
from pathlib import Path

from tqdm import tqdm

import pandas as pd


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
        preprocessed_folder = path.parent.joinpath("preprocessed3d")
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

        col_order = ["profile_id", "educational_course_id", "created_at"]

        print(partition_file)

        for chunk in tqdm(self.read_partition_chunks(partition_file)):
            chunk.dropna(subset=col_order, inplace=True)
            chunk["educational_course_id"] = chunk["educational_course_id"].apply(self.preprocess_course_id)
            chunk.sort_values(by=col_order, inplace=True)

            chunk["created_at"] = chunk["created_at"].dt.normalize()
            chunk.drop_duplicates(col_order, inplace=True)

            self.append_to_dump_file(dump_filename, chunk[col_order])


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
                "profile_id", "educational_course_id", "created_at", "dt"
            ],
            sep=";",
            usecols=["profile_id", "educational_course_id", "created_at", "dt"],
            parse_dates=["created_at"], dayfirst=True,
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

