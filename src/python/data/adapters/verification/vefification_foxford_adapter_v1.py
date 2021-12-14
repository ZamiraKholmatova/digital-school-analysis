import pandas as pd

data = pd.read_csv(
    "stat_fox_aggregate.csv", header=0,
    names=["educational_course_id","profile_id","date","duration","video_last"]    
)
data["educational_course_id"] = data["educational_course_id"].apply(lambda x: f"gid://foxford/Course/{x}")
data["start_time"] = data["date"]
data["end_time"] = data["date"]
data["dt"] = data["duration"].apply(lambda x: pd.to_timedelta(x).seconds)

for ind, row in data.iterrows():
    if row["dt"] > 3600 * 24:
        print(row)


data[["profile_id", "educational_course_id", "date", "start_time", "end_time", "dt"]] \
    .to_csv("stat_fox_aggregate___preprocessed.tsv", sep="\t", index=False, header=None)
