import pickle
import sys
from bisect import bisect_left, bisect_right
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
        self.set_column_order()
        self.get_positions()
        self.delim = ","

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

    def map_partition(self, partition_file):

        user_information = {}

        dump_filename = Path(str(partition_file.absolute()) + "___preprocessed.pkl")

        if dump_filename.is_file():
            return

        with open(partition_file, "r") as stat_file:
            print(partition_file)
            for ind, line in enumerate(tqdm(stat_file, leave=False)):
                if ind == 0:
                    continue
                record = self.read_line(line)

                profile_id = record.profile_id
                educational_course_id = record.educational_course_id
                date = record.date

                if profile_id == "" or date == "" or educational_course_id == "":
                    continue

                key = (profile_id, educational_course_id, date)

                self.merge_into(user_information, key, record)
                # if ind > 10000:
                #     break

        pickle.dump(user_information, open(dump_filename, "wb"))


def iterate_partition(partition):
    for key, day_information in partition.items():
        profile_id, educational_course_id, date = key
        yield profile_id, educational_course_id, date, day_information
    # for profile_id, course_information in partition.items():
    #     for educational_course_id, date_information in course_information.items():
    #         for date, day_information in date_information.items():
    #             yield profile_id, educational_course_id, date, day_information


def reduce_partitions(files, save_location, date_checkpoints):

    sorted_keys = {}

    for date_checkpoint in tqdm(date_checkpoints):

        storage = {}

        for file in files:
            filename = Path(str(file.absolute()) + "___preprocessed.pkl")
            if filename.is_file():
                partition = pickle.load(open(filename, "rb"))

                def date_key(key):
                    return key[2]

                if filename not in sorted_keys:
                    sorted_keys[filename] = sorted(list(partition.keys()), key=date_key)

                s_keys = sorted_keys[filename]
                start_from = bisect_left(date_checkpoint[0], s_keys, key=date_key)
                end_at = bisect_right(date_checkpoint[1], s_keys, key=date_key)

                # for ind, (profile_id, course_id, date, day_information) in enumerate(iterate_partition(partition)):
                for key in s_keys[start_from: end_at]:
                    profile_id, course_id, date = key
                    day_information = partition[key]
                    assert date >= date_checkpoint[0] and date < date_checkpoint[1]

                    if key in storage:
                        current_value = storage[key]
                        if current_value["day_start"] > day_information["day_start"]:
                            current_value["day_start"] = day_information["day_start"]
                        if current_value["day_end"] < day_information["day_end"]:
                            current_value["day_end"] = day_information["day_end"]
                        storage[key] = current_value
                    else:
                        storage[key] = day_information

        with open(save_location.joinpath(f"preprocessed_{date_checkpoint[0]}_{date_checkpoint[1]}.csv".replace(":","-")), "w") as sink:
            for key, day_information in storage.items():
                    profile_id, educational_course_id, date = key
                    day_start = day_information["day_start"]
                    day_end = day_information["day_end"]
                    is_active = (day_end - day_start).seconds > 600
                    sink.write(
                        f"{profile_id},{educational_course_id},{date},{day_start},{day_end},{is_active}\n"
                    )


# def reduce_partitions(files, save_location, date_checkpoints):
#
#     for date_checkpoint in tqdm(date_checkpoints):
#
#         storage = {}
#
#         for file in files:
#             filename = Path(str(file.absolute()) + "___preprocessed.pkl")
#             if filename.is_file():
#                 partition = pickle.load(open(filename, "rb"))
#                 for ind, (profile_id, course_id, date, day_information) in enumerate(iterate_partition(partition)):
#                     date = pd.to_datetime(date)
#                     if date >= date_checkpoint[0] and date < date_checkpoint[1]:
#                         key = (profile_id, course_id, date)
#                         if key in storage:
#                             current_value = storage[key]
#                             if current_value["day_start"] > day_information["day_start"]:
#                                 current_value["day_start"] = day_information["day_start"]
#                             if current_value["day_end"] < day_information["day_end"]:
#                                 current_value["day_end"] = day_information["day_end"]
#                             storage[key] = current_value
#                         else:
#                             storage[key] = day_information
#
#         with open(save_location.joinpath(f"preprocessed_{date_checkpoint[0]}_{date_checkpoint[1]}.csv".replace(":","-")), "w") as sink:
#             for key, day_information in storage.items():
#                     profile_id, educational_course_id, date = key
#                     day_start = day_information["day_start"]
#                     day_end = day_information["day_end"]
#                     is_active = (day_end - day_start).seconds > 600
#                     sink.write(
#                         f"{profile_id},{educational_course_id},{date},{day_start},{day_end},{is_active}\n"
#                     )


class StatisticsCombiner_Uchi(StatisticsCombiner):
    def __init__(self):
        super().__init__()

    def set_column_order(self):
        self.column_order = {"profile_id": 4, "created_at": 2, "educational_course_id": 1}


class StatisticsCombiner_Uchi_New(StatisticsCombiner):
    def __init__(self):
        super().__init__()
        self.delim = "\t"

    def set_column_order(self):
        self.column_order = {"profile_id": 4, "created_at": 2, "educational_course_id": 1}


class StatisticsCombiner_FoxFord(StatisticsCombiner):
    def __init__(self):
        super().__init__()

    def set_column_order(self):
        self.column_order = {"profile_id": 1, "created_at": 2, "educational_course_id": 5}

    def preprocess_course_id(self, id_):
        return "/".join(id_.split("/")[:5])

    def map_partition(self, partition_file):

        user_information = {}

        dump_filename = Path(str(partition_file.absolute()) + "___preprocessed.pkl")

        if dump_filename.is_file():
            return

        data = pd.read_csv(partition_file, parse_dates=["createdat"]).rename({
            "createdat": "created_at",
            "profileid": "profile_id",
            "statisticstypeid": "statistic_type_id",
            "externalid": "educational_course_id",
        }, axis=1).dropna(subset=["profile_id", "educational_course_id", "created_at"])
        data.sort_values(by=["profile_id", "educational_course_id", "created_at"], inplace=True)

        def get_border_time(profile_id, educational_course_id, created_at):
            if profile_id[0] != profile_id[1] or \
                    educational_course_id[0] != educational_course_id[1] or \
                    np.abs(created_at[1] - created_at[0]) > np.timedelta64(45, 'm'):
                return created_at[1]

        reversed_data = data[::-1]

        data["start_time_"] = rolling_apply(get_border_time, 2, data["profile_id"].values, data["educational_course_id"].values, data["created_at"].values)
        data["end_time_"] = np.flip(rolling_apply(get_border_time, 2, reversed_data["profile_id"].values, reversed_data["educational_course_id"].values, reversed_data["created_at"].values))

        data.dropna(subset=["start_time_", "end_time_"], how="all", inplace=True)

        def get_start_time(profile_id, educational_course_id, time):
            if profile_id[0] == profile_id[1] and \
                    educational_course_id[0] == educational_course_id[1]:
                return time[0]

        def get_end_time(profile_id, educational_course_id, time):
            if profile_id[0] == profile_id[1] and \
                    educational_course_id[0] == educational_course_id[1]:
                return time[1]

        data["start_time"] = rolling_apply(
            get_start_time, 2,
            data["profile_id"].values,
            data["educational_course_id"].values,
            data["start_time_"].values
        )

        data["end_time"] = rolling_apply(
            get_end_time, 2,
            data["profile_id"].values,
            data["educational_course_id"].values,
            data["end_time_"].values
        )

        data.dropna(subset=["start_time", "end_time"], inplace=True)
        data["date"] = data["created_at"].apply(lambda x: str(x).split(" ")[0])


        for win in data.rolling(2, method="table"):
            print(win[
                ["profile_id", "educational_course_id", "created_at", "start_time", "end_time"]
                  ].to_string())
            print()

        with open(partition_file, "r") as stat_file:
            print(partition_file)
            for ind, line in enumerate(tqdm(stat_file, leave=False)):
                if ind == 0:
                    continue
                record = self.read_line(line)

                profile_id = record.profile_id
                educational_course_id = record.educational_course_id
                date = record.date

                if profile_id == "" or date == "" or educational_course_id == "":
                    continue

                key = (profile_id, educational_course_id, date)

                self.merge_into(user_information, key, record)
                # if ind > 10000:
                #     break

        pickle.dump(user_information, open(dump_filename, "wb"))


class StatisticsCombiner_MEO(StatisticsCombiner):
    def __init__(self):
        super().__init__()

    def set_column_order(self):
        self.column_order = None

    def read_line(self, line):
        fields = line.strip().split("|")
        userId = fields[0]
        date = pd.to_datetime(fields[1].split(" ")[0])
        externalId = fields[3]

        day_start = pd.to_datetime(fields[1])
        day_end = pd.to_datetime(fields[2])

        return Record(userId, externalId, date, day_start, day_end)


class StatisticsCombiner_1C_ND(StatisticsCombiner):
    def __init__(self):
        super().__init__()

    def set_column_order(self):
        self.column_order = {"profile_id": 1, "created_at": 3, "educational_course_id": 5}


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

    # with Pool(4) as p:
    #     p.map(partition_fn, files)

    for file in files:
        partition_fn(file)

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


def main():
    statistics_folder = Path(sys.argv[1])
    files = [file for file in statistics_folder.iterdir() if not file.name.startswith(".") and not file.name.startswith("_") and file.name.endswith(".csv")]

    preprocess(files, statistics_folder.parent, map_partitions_uchi)


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

    main()
