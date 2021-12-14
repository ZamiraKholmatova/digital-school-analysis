import pandas as pd
from collections import Counter

data = pd.read_csv(
    "Новый Диск.csv", header=0,
    names=["profile_id","educational_course_id","date","dt"], dtype={"educational_course_id": "string"}    
)

mm = pd.read_csv("course_mapping.csv", dtype={"external_id": "string"})
course_mapping = dict(zip(mm["external_id"], mm["educational_course_id"]))

print(course_mapping)
unresolved_courses = Counter()
unresolved_users = set()


def resolve_course(course):
    if course in course_mapping:
        return course_mapping[course]
    else:
        unresolved_courses[course] += 1
    

data["educational_course_id"] = data["educational_course_id"].apply(resolve_course)
data["start_time"] = data["date"]
data["end_time"] = data["date"]
data["dt"] = data["dt"].apply(lambda x: x * 60)

for ind, row in data.iterrows():
    if row["dt"] > 3600 * 24:
        print(row)


data[["profile_id", "educational_course_id", "date", "start_time", "end_time", "dt"]] \
    .to_csv("new_disk___preprocessed.tsv", sep="\t", index=False, header=None)
