import pandas as pd
import pickle 
from collections import Counter


# def load_user_mappings():
#     data = pd.read_csv("user_mappings.csv", dtype="string")
#     data.drop_duplicates(inplace=True)
#     return dict(zip(data["external_id"], data["profile_id"]))

# user_mapping = load_user_mappings()

def load_course_mapping():
    courses = pd.read_csv("uchi_results_course_structure.csv", dtype="string")
    mapping = {}
    for cid, sid, grade in zip(courses["external_id"], courses["subject_id"], courses["grade"]):
        if pd.isna(grade):
            mapping[sid] = cid
        else:
            mapping[(sid, grade)] = cid
    return mapping # dict(zip(courses["subject_id"], courses["external_id"]))

course_mapping = load_course_mapping()

print(course_mapping)

activity = pd.read_csv("выгрузка_ЦОС__.csv", dtype="string", sep="\t")
activity.dropna(inplace=True)

unresolved_courses = Counter()
unresolved_users = set()


def resolve_course(course_grade):
    course, grade = course_grade
    if course in course_mapping:
        return course_mapping[course]
    else:
        if course_grade in course_mapping:
            return course_mapping[course_grade]
        unresolved_courses[course_grade] += 1


# def resolve_user(user):
#     if user in user_mapping:
#         return user_mapping[user]
#     else:
#         unresolved_users.add(user)

# "subject_id"	"primary_sec"	"externalUserId"	"created_date"	"userId"	"t_sub"

activity["course_grade"] = list(zip(activity["subject_id"], activity["grade"]))

activity["profile_id"] = activity["userId"]#.apply(resolve_user)
activity["educational_course_id"] = activity["course_grade"].apply(resolve_course)

activity.dropna(inplace=True)
print("Users", unresolved_users)
print("Course", unresolved_courses)

activity["date"] = activity["created_date"]
activity["start_time"] = activity["created_date"]
activity["end_time"] = activity["created_date"]
activity["dt"] = activity["t_sub"]

activity[["profile_id", "educational_course_id", "date", "start_time", "end_time", "dt"]]\
    .to_csv("preprocessed_uchi_activity___preprocessed.tsv", index=False, header=None, sep="\t")

