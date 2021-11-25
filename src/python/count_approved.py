from pprint import pprint

import pandas as pd


def load_role_descriptions(path):
    return {
        "ce259f6d-c50b-4a5a-b8e5-b26637dcae4b": "TEACHER",
        "1ab29d8a-0594-4b02-a342-96fa4674fcf4": "STUDENT",
        "baba1afd-4d5f-4faa-a9b9-c639fef100c7": "PARENT",
        "d9bb5b92-345d-4bfe-8f5f-c98e3021ae2e": "ADMIN",
        "b20342a5-5b86-41df-a6ea-5fb3627209e6": "INSTITUTE"
    }

def load_profile_approved_status(path):
    data = pd.read_csv(path)
    return dict(zip(data["profile_id"], data["approved_status"]))

def load_profile_roles(path):
    data = pd.read_csv(path)
    return {profile_id: role_id for profile_id, role_id, updated_at, created_at in data.values}

role_decr = load_role_descriptions("/Users/LTV/Documents/school/db_data/role_description_16.11.csv",)
appr_status = load_profile_approved_status("/Users/LTV/Documents/school/db_data/profile_educational_institution_17.11.csv",)
prof_roles = load_profile_roles("/Users/LTV/Documents/school/db_data/profile_role_16.11.csv",)

report = {
    "Teachers total": 0,
    "Teachers approved": 0,
    "Teachers rejected": 0,
    "Students total": 0,
    "Students approved": 0,
    "Students rejected": 0
}

no_status_t = 0
no_status_s = 0
for profile_id, profile_role in prof_roles.items():
    role = role_decr[profile_role]
    if role == "TEACHER":
        status = appr_status.get(profile_id, None)
        if status is None:
            no_status_t += 1
        report["Teachers total"] += 1
        if status == "APPROVED":
            report["Teachers approved"] += 1
        elif status == "NOT_APPROVED":
            report["Teachers rejected"] += 1
    elif role == "STUDENT":
        status = appr_status.get(profile_id, None)
        if status is None:
            no_status_s += 1
        report["Students total"] += 1
        if status == "APPROVED":
            report["Students approved"] += 1
        elif status == "NOT_APPROVED":
            report["Students rejected"] += 1


pprint(report)
print(no_status_t, no_status_s)