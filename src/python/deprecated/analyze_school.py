#%%
from collections import defaultdict
import pandas as pd
from tqdm import tqdm
from math import isnan

#%%
billing = pd.read_excel("/Users/LTV/Downloads/12.10/billing.xlsx")
sessins_raw = pd.read_csv("/Users/LTV/Downloads/12.10/sessions_raw.csv")
# %%

usage_duration = {}

def count_usage_duration(usage_duration, session):
    for date, student_id, subject_id, spent in tqdm(session.values):
        if student_id not in usage_duration:
            usage_duration[student_id] = {}
        if subject_id not in usage_duration[student_id]:
            usage_duration[student_id][subject_id] = 0.

        usage_duration[student_id][subject_id] += spent

billing = pd.read_excel("/Users/LTV/Downloads/12.10/billing.xlsx")
count_usage_duration(usage_duration, billing)
billing = pd.read_excel("/Users/LTV/Downloads/18.10/Billing 11.10-17.10.xlsx")
count_usage_duration(usage_duration, billing)

total_usage_duration = {}

for student in usage_duration:
    total_usage_duration[student] = sum(val for val in usage_duration[student].values())

#%%
diff_subj = [len(usage_duration[student]) for student in usage_duration]
mean_subj = sum(diff_subj) / len(diff_subj)
# 2.491


# %%

usage_count = {}

for date, student_id, subject_id, spent in tqdm(billing.values):
    if student_id not in usage_count:
        usage_count[student_id] = {}
    if subject_id not in usage_count[student_id]:
        usage_count[student_id][subject_id] = 0

    usage_count[student_id][subject_id] += 1

total_usage_count = {}

for student in usage_count:
    total_usage_count[student] = sum(val for val in usage_count[student].values())
# %%
# учи ру
dates_count = {}
convergence_info = []

def populate_dates_counts(dates_count, sessions):
    for statisticsTypeId,externalId,createdAt,externalUserId,userId in tqdm(sessions.values):
        # if isinstance(externalId, float) and isnan(externalId):
        #     continue
        if externalUserId not in dates_count:
            dates_count[externalUserId] = set()
        date = createdAt.split(" ")[0]
        assert date != ""

        dates_count[externalUserId].add(date)

def compute_convergence(period, dates_count):
    def conv(duration):
        return sum(1 for student in dates_count if len(dates_count[student]) >= duration)

    info = {"period": period, "unique_students": len(dates_count)}

    for i in range(2, 8):
        c = conv(i)
        info[f"convergence_{i}"] = c
        info[f"ratio_{i}"] = c / len(dates_count)
    return info

def incorporate_information(period_files):
    for ind, period_file in enumerate(period_files):
        sessins_raw = pd.read_csv(period_file)
        populate_dates_counts(dates_count, sessins_raw)
        convergence_info.append(compute_convergence(ind, dates_count))

period_files = [
    "/Users/LTV/Documents/school/code/data/uchi/statistics/sessions_raw 10.12.csv",
    "/Users/LTV/Documents/school/code/data/uchi/statistics/sessions_raw 10.18.csv",
    "/Users/LTV/Documents/school/code/data/uchi/statistics/sessions_raw 10.25.csv",
    "/Users/LTV/Documents/school/code/data/uchi/statistics/sessions_raw 11.01.csv",
    "/Users/LTV/Documents/school/code/data/uchi/statistics/sessions_raw 11.08.csv",
    "/Users/LTV/Documents/school/code/data/uchi/statistics/sessions_raw 11.15.csv",
    "/Users/LTV/Documents/school/code/data/uchi/statistics/sessions_raw 11.22.csv",
]

incorporate_information(period_files)

import json
with open("convergence_uchi_f.json", "w") as convergence_json:
    convergence_json.write(json.dumps(convergence_info, indent=4))


# sessins_raw = pd.read_csv("/Users/LTV/Downloads/12.10/sessions_raw.csv")
# populate_dates_counts(dates_count, sessins_raw)
# sessins_raw = pd.read_csv("/Users/LTV/Downloads/18.10/sessions_raw 11.10-17.10.csv")
# populate_dates_counts(dates_count, sessins_raw)
# sessins_raw = pd.read_csv("/Users/LTV/Downloads/sessions_raw 25.10.csv")
# populate_dates_counts(dates_count, sessins_raw)
# sessins_raw = pd.read_csv("/Users/LTV/Downloads/sessions_raw 01.11.csv")
# populate_dates_counts(dates_count, sessins_raw)


#%%

# FoxFord Stats
dates_count = {}
convergence_info = []

def populate_dates_counts(dates_count, sessions):
    print(sessions.columns)
    for systemcode, profileid, createdat, statisticstypeid, status, externalid in tqdm(sessions.values):
        if profileid not in dates_count:
            dates_count[profileid] = set()
        date = createdat.split(" ")[0]
        assert date != ""

        dates_count[profileid].add(date)

def compute_convergence(period, dates_count):
    def conv(duration):
        return sum(1 for student in dates_count if len(dates_count[student]) >= duration)

    info = {"period": period, "unique_students": len(dates_count)}

    for i in range(2, 8):
        c = conv(i)
        info[f"convergence_{i}"] = c
        info[f"ratio_{i}"] = c / len(dates_count)
    return info

def incorporate_information(period_files):
    for ind, period_file in enumerate(period_files):
        sessins_raw = pd.read_csv(period_file)
        populate_dates_counts(dates_count, sessins_raw)
        convergence_info.append(compute_convergence(ind, dates_count))

import os
parent_dir = "/Users/LTV/Downloads/учи.ру/FoxFord Stats"
period_files = os.listdir(parent_dir)
period_files = list(map(lambda x: os.path.join(parent_dir, x), period_files))
period_files = sorted(list(filter(lambda x: x.endswith("csv"), period_files)))


incorporate_information(period_files)

import json
with open("convergence_foxford.json", "w") as convergence_json:
    convergence_json.write(json.dumps(convergence_info, indent=4))


#%%

# 1c
dates_count = {}
convergence_info = []

def populate_dates_counts(dates_count, sessions):
    # print(sessions.columns)
    for id_,statistic_type_id,external_system_id,profile_id,created_at,external_user_id,updated_at in tqdm(sessions.values):
        if external_user_id not in dates_count:
            dates_count[external_user_id] = set()
        date = created_at.split(" ")[0]
        assert date != ""

        dates_count[external_user_id].add(date)

def compute_convergence(period, dates_count):
    def conv(duration):
        return sum(1 for student in dates_count if len(dates_count[student]) >= duration)

    info = {"period": period, "unique_students": len(dates_count)}

    for i in range(2, 8):
        c = conv(i)
        info[f"convergence_{i}"] = c
        info[f"ratio_{i}"] = c / len(dates_count)
    return info

def incorporate_information(period_files):
    for ind, period_file in enumerate(period_files):
        sessins_raw = pd.read_csv(period_file)
        populate_dates_counts(dates_count, sessins_raw)
        convergence_info.append(compute_convergence(ind, dates_count))

period_files = [
    "/Users/LTV/Downloads/учи.ру/1c/1c_login_logout_stats_10_11_p0.csv",
    "/Users/LTV/Downloads/учи.ру/1c/1c_login_logout_stats_10_11_p1.csv",
    "/Users/LTV/Downloads/учи.ру/1c/1c_login_logout_stats_10_11_p2.csv",
    "/Users/LTV/Downloads/учи.ру/1c/1c_login_logout_stats_10_11_p3.csv",
    "/Users/LTV/Downloads/учи.ру/1c/1c_login_logout_stats_10_11_p4.csv",
]


incorporate_information(period_files)

import json
with open("convergence_1c.json", "w") as convergence_json:
    convergence_json.write(json.dumps(convergence_info, indent=4))


#%%

# nd
dates_count = {}
convergence_info = []

def populate_dates_counts(dates_count, sessions):
    # print(sessions.columns)
    for id_,statistic_type_id,external_system_id,profile_id,created_at,external_user_id,updated_at in tqdm(sessions.values):
        if external_user_id not in dates_count:
            dates_count[external_user_id] = set()
        date = created_at.split(" ")[0]
        assert date != ""

        dates_count[external_user_id].add(date)

def compute_convergence(period, dates_count):
    def conv(duration):
        return sum(1 for student in dates_count if len(dates_count[student]) >= duration)

    info = {"period": period, "unique_students": len(dates_count)}

    for i in range(2, 8):
        c = conv(i)
        info[f"convergence_{i}"] = c
        info[f"ratio_{i}"] = c / len(dates_count) if len(dates_count) > 0 else 0.
    return info

def incorporate_information(period_files):
    for ind, period_file in enumerate(period_files):
        sessins_raw = pd.read_csv(period_file)
        populate_dates_counts(dates_count, sessins_raw)
        convergence_info.append(compute_convergence(ind, dates_count))

period_files = [
    "/Users/LTV/Downloads/учи.ру/nd/nd_login_logout_stats_10_11_p0.csv",
    "/Users/LTV/Downloads/учи.ру/nd/nd_login_logout_stats_10_11_p1.csv",
    "/Users/LTV/Downloads/учи.ру/nd/nd_login_logout_stats_10_11_p2.csv",
    "/Users/LTV/Downloads/учи.ру/nd/nd_login_logout_stats_10_11_p3.csv",
    "/Users/LTV/Downloads/учи.ру/nd/nd_login_logout_stats_10_11_p4.csv",
]


incorporate_information(period_files)

import json
with open("convergence_nd.json", "w") as convergence_json:
    convergence_json.write(json.dumps(convergence_info, indent=4))
# %%

convergence = sum(1 for student in dates_count if len(dates_count[student]) >= 7)

# 1914, 22927
# 19749, 79824
# 47106, 127866
# 81500, 212559


# %%
 
count_first = {}

sessins_raw = pd.read_csv("/Users/LTV/Downloads/12.10/sessions_raw.csv")
populate_dates_counts(count_first, sessins_raw)

sum(1 for student in count_first if len(count_first[student]) >= 5)
# %%

# %%
