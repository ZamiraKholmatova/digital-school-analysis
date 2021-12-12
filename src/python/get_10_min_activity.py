import pickle
import sys
from multiprocessing import Pool
from pathlib import Path
from tqdm import tqdm

import pandas as pd


def map_partition(partition_file, column_order):
    p_id_o = column_order["profile_id"]
    c_at_o = column_order["created_at"]
    ec_id_o = column_order["educational_course_id"]

    user_information = {}

    with open(partition_file, "r") as stat_file:
        print(partition_file)
        for ind, line in enumerate(tqdm(stat_file, leave=False)):
            if ind == 0:
                continue
            # statisticsTypeId, externalId, createdAt, externalUserId, userId = line.strip().split(",")
            fields = line.strip().split(",")
            userId = fields[p_id_o]
            createdAt = fields[c_at_o]
            externalId = fields[ec_id_o]

            if externalId == "" or createdAt == "" or userId == "":
                continue
            date = createdAt.split(" ")[0]
            createdAt = pd.to_datetime(createdAt)
            if userId not in user_information:
                user_information[userId] = {}
            course_information = user_information[userId]
            if externalId not in course_information:
                course_information[externalId] = {}
            date_information = course_information[externalId]
            if date not in date_information:
                date_information[date] = {
                    "day_start": None,
                    "day_end": None
                }
            day_information = date_information[date]
            if day_information["day_start"] is None or createdAt < day_information["day_start"]:
                day_information["day_start"] = createdAt
            if day_information["day_end"] is None or createdAt > day_information["day_end"]:
                day_information["day_end"] = createdAt

            # if ind > 10000:
            #     break

    dump_filename = str(partition_file.absolute()) + "___preprocessed.pkl"
    pickle.dump(user_information, open(dump_filename, "wb"))


def iterate_partition(partition):
    for profile_id, course_information in partition.items():
        for educational_course_id, date_information in course_information.items():
            for date, day_information in date_information.items():
                yield profile_id, educational_course_id, date, day_information


def reduce_partitions(files, save_location, date_checkpoints):

    for date_checkpoint in tqdm(date_checkpoints):

        storage = {}

        for file in files:
            filename = Path(str(file.absolute()) + "___preprocessed.pkl")
            if filename.is_file():
                partition = pickle.load(open(filename, "rb"))
                for ind, (profile_id, course_id, date, day_information) in enumerate(iterate_partition(partition)):
                    date = pd.to_datetime(date)
                    if date >= date_checkpoint[0] and date < date_checkpoint[1]:
                        key = (profile_id, course_id, date)
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


def map_partitions_uchi(files):
    map_partition(files, {"profile_id": 4, "created_at": 2, "educational_course_id": 1})


def preprocess(files, save_location, timestamp, partition_fn):

    with Pool(2) as p:
        p.map(partition_fn, files)

    # for file in files:
    #     partition_fn(file)

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

    preprocess(files, statistics_folder.parent, "XXX", map_partitions_uchi)



if __name__ == "__main__":
    main()