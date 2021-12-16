import pandas as pd
import pickle 


def load_user_mappings():
    data = pd.read_csv("user_mappings.csv", dtype="string")
    data.drop_duplicates(inplace=True)
    return dict(zip(data["external_id"], data["profile_id"]))

user_mapping = load_user_mappings()

def load_course_mapping():
    courses = pd.read_csv("uchi_results_course_structure.csv", dtype="string")
    return  dict(zip(courses["subject_id"], courses["external_id"]))

course_mapping = load_course_mapping()

print(course_mapping)

activity = pd.read_csv("выгрузка_ученики_время.csv", dtype="string")
activity.dropna(inplace=True)

unresolved_courses = set()
unresolved_users = set()


def resolve_course(course):
    if course in course_mapping:
        return course_mapping[course]
    else:
        unresolved_courses.add(course)


def resolve_user(user):
    if user in user_mapping:
        return user_mapping[user]
    else:
        unresolved_users.add(user)


activity["profile_id"] = activity["externalUserId"].apply(resolve_user)
activity["educational_course_id"] = activity["subject_id"].apply(resolve_course)

activity.dropna(inplace=True)
print("Users", unresolved_users)
print("Course", unresolved_courses)

activity["date"] = activity["created_date"]
activity["start_time"] = activity["created_date"]
activity["end_time"] = activity["created_date"]
activity["dt"] = activity["t_sub_sec"]

activity[["profile_id", "educational_course_id", "date", "start_time", "end_time", "dt"]]\
    .to_csv("preprocessed_uchi_activity___preprocessed.tsv", index=False, header=None, sep="\t")

