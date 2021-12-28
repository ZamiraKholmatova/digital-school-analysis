import pandas as pd
from tqdm import tqdm

from dsa.data import SQLTable


def normalize_month(date):
    return date.replace(day=1)

people_by_month = {}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("data")
    parser.add_argument("course_information")
    args = parser.parse_args()

    db = SQLTable("3d_activity.sqlite")

    ci = pd.read_csv(args.course_information)
    c2p = dict(zip(ci["course_id"], ci["provider"]))

    seen_before = set()
    records = []

    # for ind, chunk in enumerate():
    chunk = pd.read_csv(args.data, parse_dates=["created_at"]) # , chunksize=1000000
    chunk.sort_values(by=["created_at"], inplace=True)
    chunk["created_at"] = chunk["created_at"].apply(normalize_month)
    chunk["provider"] = chunk["educational_course_id"].apply(lambda x: c2p[x])
    # print(f"Chunk {ind}")
    for p, pid, d in tqdm(chunk[["provider", "profile_id", "created_at"]].values):
        key = (p, pid)
        if key not in seen_before:
            records.append({
                "profile_id": pid,
                "platform": p,
                "created_at": d
            })
            seen_before.add(key)

    records = pd.DataFrame.from_records(records)
    records.to_pickle("first_appearance.bz2")

    result = records.groupby(by=["platform", "created_at"]).count()

    print(result.to_string())

if __name__ == "__main__":
    main()