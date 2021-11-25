#%%
import os
import pickle
from collections import defaultdict
from os import rename
from typing import Iterator
import pandas as pd
from math import isnan

from pandas.core.dtypes.missing import isna


def cached(func):
    def caching_wrapper(self, path):
        cache_name = path + '___cached.pkl'
        if os.path.isfile(cache_name):
            obj = pickle.load(open(cache_name, "rb"))
        else:
            obj = func(self, path)
            pickle.dump(obj, open(cache_name, "wb"))
        return obj
    return caching_wrapper


class SharedModel:
    def __init__(
            self, role_descriptions, statistics_type, external_system, profile_educational_institution,
            profile_roles, course_structure, course_types, course_statistics
    ):
        self.chunk_size = 1000000
        self.role_descriptions = self.load_role_descriptions(role_descriptions)
        self.statistic_types = self.load_statistics_type(statistics_type)
        self.external_system = self.load_external_system(external_system)
        self.profile_approved_status = self.load_profile_approved_status(profile_educational_institution)
        self.profile_roles = self.load_profile_roles(profile_roles)
        self.structure = self.load_course_structure(course_structure)
        self.course_types = self.load_course_types(course_types)
        self.load_course_statistics(course_statistics)
        self.corrupted = []
        self.student_statistics = {}

    @cached
    def load_role_descriptions(self, path):
        return {
            "ce259f6d-c50b-4a5a-b8e5-b26637dcae4b": "TEACHER",
            "1ab29d8a-0594-4b02-a342-96fa4674fcf4": "STUDENT",
            "baba1afd-4d5f-4faa-a9b9-c639fef100c7": "PARENT",
            "d9bb5b92-345d-4bfe-8f5f-c98e3021ae2e": "ADMIN",
            "b20342a5-5b86-41df-a6ea-5fb3627209e6": "INSTITUTE"
        }

    @cached
    def load_statistics_type(self, path):
        return {
            "1ad6841e-2e64-4720-852c-fa2ad2fd5714": "login",
            "8e4870de-468e-4bfa-9867-daa609693b49": "logout",
            "ab201fae-44b7-4d4c-95f2-50bd0a0c1cb7": "started_studying",
            "4f6c88b8-2131-4948-bd3c-b34e3d491171": "stopped_studying"
        }

    @cached
    def load_external_system(self, path):
        data = pd.read_csv(path)
        return dict(zip(data["system_code"], data["short_name"]))

    @cached
    def load_profile_approved_status(self, path):
        data = pd.read_csv(path)
        return dict(zip(data["profile_id"], data["approved_status"]))

    @cached
    def load_profile_roles(self, path):
        data = pd.read_csv(path)
        return {profile_id: role_id for profile_id, role_id, updated_at, created_at in data.values}

    def format_course_structure_columns(self, data):
        fields = ["id", "deleted", "course_type_id", "parent_id", "external_link", "course_name", "external_id",
                  "external_parent_id", "system_code"]
        return data

    def validate_structure_id(self, id, parent_id, structure):
        assert id not in structure

    # @cached
    def load_course_structure(self, path):
        data = self.format_course_structure_columns(pd.read_csv(path))
        fields = data.columns
        structure = {}
        for ind, row in data.iterrows():
            self.validate_structure_id(row["id"], row["parent_id"], structure)
            structure[row["id"]] = {key: row[key] for key in fields}
        return structure

    @cached
    def load_course_types(self, path):
        data = pd.read_csv(path)
        return {
            id: type_name for id, type_name, entity_id, created_at, updated_at in data.values
        }

    def get_course_type(self, type_id):
        return self.course_types[type_id]

    def remove_one_id_level(self, course_id):
        return "/".join(course_id.split("/")[:-1])

    def find_subject(self, course_id):
        if not isinstance(course_id, float) and "foxford" in course_id and course_id not in self.structure:
            course_id = self.remove_one_id_level(course_id)
        if not isinstance(course_id, float) and course_id not in self.structure:
            if f"Lesson_{course_id}" in self.structure:
                course_id = f"Lesson_{course_id}"
            elif f"Chapter_{course_id}" in self.structure:
                course_id = f"Chapter_{course_id}"
            elif f"Topic_{course_id}" in self.structure:
                course_id = f"Topic_{course_id}"
            elif f"Course_{course_id}" in self.structure:
                course_id = f"Course_{course_id}"
        if course_id not in self.structure:
            return None, None, None
        course = self.structure[course_id]
        # print(course["course_name"], course_types[course["course_type_id"]], course["external_link"], sep="\t")
        parent_id = course["parent_id"]
        # print(parent_id, type(parent_id))
        if isinstance(parent_id, float) and isnan(parent_id):
            return course["course_name"], self.get_course_type(course["course_type_id"]), self.external_system[course["system_code"]]
        return self.find_subject(parent_id)

    def load_course_statistics(self, path):
        self.course_statistics = pd.read_csv(path, chunksize=self.chunk_size, parse_dates=["created_at"])

    def get_role(self, profile_id):
        return self.role_descriptions[self.profile_roles[profile_id]]

    def entry_valid(self, profile_id, statistic_type_id, educational_course_id, created_at):
        profile_id = profile_id #entry["profile_id"]
        course_id = educational_course_id # entry["educational_course_id"]
        if isinstance(profile_id, float) and isnan(profile_id):
            # self.corrupted.append(
            #     # {"id": entry["id"], "reason": "no person_id", "external_id": entry["external_user_id"]})
            #     {
            #         "reason": "no person_id", "profile_id": profile_id, "statistic_type_id": statistic_type_id,
            #         "educational_course_id": educational_course_id, "created_at": created_at
            #     }
            # )
            return False
        elif isinstance(course_id, float) and isnan(course_id) or pd.isna(course_id):
            # self.corrupted.append(
            #     # {"reason": "no course id", "entry": entry})
            #     {
            #         "reason": "no course id", "profile_id": profile_id, "statistic_type_id": statistic_type_id,
            #         "educational_course_id": educational_course_id, "created_at": created_at
            #     }
            # )
            return False
        else:
            return True

    def get_statistics_type(self, type_id):
        return self.statistic_types[type_id]

    def validate_course(self, subject_name, course_type, provider, profile_id, statistic_type_id, educational_course_id, created_at):
        if subject_name == course_type == provider == None:
            # self.corrupted.append(
            #     # {
            #     #     "reason": "course id not found", "entry": row
            #     # }
            #     {
            #         "reason": "course id not found", "profile_id": profile_id, "statistic_type_id": statistic_type_id,
            #         "educational_course_id": educational_course_id, "created_at": created_at
            #     }
            # )
            return
        assert course_type == "ЦОМ"
        assert isinstance(provider, str)
        assert isinstance(subject_name, str)

    def add_entry(self, person_id, provider, subject_name, created_at, statistics_type):
        if person_id not in self.student_statistics:
            self.student_statistics[person_id] = {}
        if provider not in self.student_statistics[person_id]:
            self.student_statistics[person_id][provider] = {}
        if subject_name not in self.student_statistics[person_id][provider]:
            self.student_statistics[person_id][provider][subject_name] = []

        self.student_statistics[person_id][provider][subject_name].append((
            created_at, statistics_type
        ))

    def iterate_course_statistics_chunk(self, chunk):
        column_order = ["profile_id", "statistic_type_id", "educational_course_id", "created_at"]
        chunk["created_at"] = chunk["created_at"].dt.normalize()
        chunk.drop_duplicates(column_order, inplace=True)
        return chunk[column_order].values

    def process_course_statistics(
            self,
    ):
        for chunk in self.course_statistics:
            for profile_id, statistic_type_id, educational_course_id, created_at in self.iterate_course_statistics_chunk(chunk):
                if not self.entry_valid(profile_id, statistic_type_id, educational_course_id, created_at):
                    continue
                role = self.get_role(profile_id)
                if role != "STUDENT":
                    continue

                statistics_type = self.get_statistics_type(statistic_type_id)
                subject_name, course_type, provider = self.find_subject(educational_course_id)

                self.validate_course(subject_name, course_type, provider, profile_id, statistic_type_id, educational_course_id, created_at)

                # person_id = row["profile_id"]  # row["external_user_id"]
                # assert isinstance(person_id, int)

                self.add_entry(profile_id, provider, subject_name, created_at, statistics_type)
                # break
            # break

    def active_days(self, history):
        active_day_duration_minutes = 10

        days = defaultdict(list)
        for time_stamp, _ in history:
            days[time_stamp.date()].append(time_stamp)

        active_days = 0
        logged_in_days = 0
        for day, sessions in days.items():
            sessions = sorted(sessions)
            active_minutes = (sessions[-1] - sessions[0]).seconds // 60
            if active_minutes >= active_day_duration_minutes:
                active_days += 1
            logged_in_days += 1

        return active_days, logged_in_days

    def get_approved_status(self, person_id):
        return self.profile_approved_status.get(person_id, None)

    def verify_approved_status(self, approved_status, person_id, platform, course_info):
        if approved_status is None:
            # self.corrupted.append({
            #     "reason": "no approved_status", "platform": platform, "course_info": course_info, "person_id": person_id
            # })
            approved_status = "NONE"
        return approved_status


    def prepare_for_report(self):
        self.full_report = []
        for person_id, student_info in self.student_statistics.items():
            for platform, course_info in student_info.items():
                for course_name, course_history in course_info.items():
                    active_days, logged_in_days = self.active_days(course_history)
                    approved_status = self.get_approved_status(person_id)
                    self.verify_approved_status(approved_status, person_id, platform, course_info)

                    self.full_report.append({
                        "person_id": person_id,
                        "platform": platform,
                        "course_name": course_name,
                        "is_approved": approved_status,
                        "active_days": active_days,
                        "logged_in_days": logged_in_days
                    })

    def prepare_report(self):
        data = pd.DataFrame.from_records(self.full_report)

        user_report = []

        for platform, course in data.groupby("platform"):
            user_report.append({
                "platform": platform,
                "users_total": data["person_id"].nunique(),
                "users_active": course.query("logged_in_days >=5")["person_id"].nunique(),
                "uaers_approved": course.query("is_approved == 'APPROVED'")["person_id"].nunique(),
                "users_approved_and_active": course.query("logged_in_days >=5 and is_approved == 'APPROVED'")[
                    "person_id"].nunique(),
            })

        report = []

        for (platform, course_name), course in data.groupby(["platform", "course_name"]):
            report.append({
                "Платформа": platform,
                "Название": course_name,
                "Всего": course["person_id"].nunique(),
                "Активные Подтверждённые": course.query("active_days >=5 and is_approved == 'APPROVED'")["person_id"].nunique(),
                "Активные Всего": course.query("active_days >=5")["person_id"].nunique(),
                "Активные Login Подтверждённые": course.query("logged_in_days >=5 and is_approved == 'APPROVED'")["person_id"].nunique(),
                "Активные Login Всего": course.query("logged_in_days >=5")["person_id"].nunique()
            })

        report_df = pd.DataFrame(report)
        report_df.to_csv(f"{self.__class__.__name__}_report.csv", index=False)
        user_report_df = pd.DataFrame(user_report)
        user_report_df.to_csv(f"{self.__class__.__name__}_user_report.csv", index=False)



    def get_report(self):
        cached_statistics_path = f"{self.__class__.__name__}_student_statistics.pkl"
        if os.path.isfile(cached_statistics_path):
            self.student_statistics = pickle.load(open(cached_statistics_path, "rb"))
        else:
            self.process_course_statistics()
            pickle.dump(self.student_statistics, open(cached_statistics_path, "wb"))

        self.prepare_for_report()
        self.prepare_report()



class Course_1C_ND(SharedModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class Course_FoxFord(SharedModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def format_course_structure_columns(self, data):
        data.rename({
            "externalId": "id",
            "externalParentId": "parent_id",
            "courseName": "course_name",
            "courseTypeId": "course_type_id",
            "externalLink": "external_link"
        }, axis=1, inplace=True)
        data["system_code"] = "13788b9a-3426-45b2-9ba5-d8cec8c03c0c"
        return data

    def get_course_type(self, type_id):
        if type_id == 0:
            return "ЦОМ"
        elif type_id == 2:
            return "Урок"
        elif type_id == 3:
            return "Задача"
        else:
            raise ValueError()

    def get_statistics_type(self, type_id):
        if type_id == 0:
            return  "login"
        elif type_id == 2:
            return "started_studying"
        else:
            return "logout"

    def validate_structure_id(self, id, parent_id, structure):
        if id in structure:
            assert parent_id == structure[id]["parent_id"]

    def map_course_statistics_columns(self, data):
        data.rename({
            "createdat": "created_at",
            "profileid": "profile_id",
            "statisticstypeid": "statistic_type_id",
            "externalid": "educational_course_id",
        }, axis=1, inplace=True)
        return data

    def load_course_statistics(self, path):
        files = sorted(os.listdir(path))
        files = filter(lambda filename: filename.endswith(".csv"), files)
        self.current_file = None
        def load_data():
            for file in files:
                self.current_file = file
                for chunk in pd.read_csv(os.path.join(path, file), chunksize=self.chunk_size, parse_dates=["createdat"]):
                    yield self.map_course_statistics_columns(chunk)
        self.course_statistics = load_data()


class Course_Uchi(SharedModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_statistics_type(self, type_id):
        if type_id == 0:
            return  "login"
        elif type_id == 2:
            return "started_studying"
        else:
            return "logout"

    def format_course_structure_columns(self, data):
        data.drop(["id", "parent_id"], axis=1, inplace=True)
        data = data.query(f"system_code == 'd2735d92-6ad6-49c4-9b36-c3b16cee695d'")
        # uchi_system_code = "d2735d92-6ad6-49c4-9b36-c3b16cee695d"
        # uchi_course_data = data.query(f"system_code == '{uchi_system_code}'")
        data.rename({
            "external_id": "id",
            "external_parent_id": "parent_id",
            # "courseName": "course_name",
            "courseTypeId": "course_type_id",
            "externalLink": "external_link"
        }, axis=1, inplace=True)
        # data["system_code"] =
        return data  # lesson chapter topic course

    def map_course_statistics_columns(self, data):
        data.rename({
            "createdAt": "created_at",
            "userId": "profile_id",
            "statisticsTypeId": "statistic_type_id",
            "externalId": "educational_course_id",
        }, axis=1, inplace=True)
        return data

    def load_course_statistics(self, path):
        files = sorted(os.listdir(path))
        files = filter(lambda filename: filename.endswith(".csv"), files)
        self.current_file = None
        def load_data():
            for file in files:
                self.current_file = file
                for chunk in pd.read_csv(os.path.join(path, file), chunksize=self.chunk_size, parse_dates=["createdAt"],
                                         dtype={"externalId": "string"}):
                    yield self.map_course_statistics_columns(chunk)
        self.course_statistics = load_data()


def main():
    course_1c_nd = Course_1C_ND(
        "/Users/LTV/Documents/school/db_data/role_description_16.11.csv",
        "/Users/LTV/Documents/school/db_data/statistics_type_16.11.csv",
        "/Users/LTV/Documents/school/db_data/external_system_16.11.csv",
        "/Users/LTV/Documents/school/db_data/profile_educational_institution_17.11.csv",
        "/Users/LTV/Documents/school/db_data/profile_role_16.11.csv",
        "/Users/LTV/Documents/school/db_data/educational_courses_16.11.csv",
        "/Users/LTV/Documents/school/db_data/educational_course_type_16.11.csv",
        "/Users/LTV/Documents/school/db_data/educational_course_statistic_16.11.csv"
    )

    course_1c_nd.get_report()

    course_foxford = Course_FoxFord(
        "/Users/LTV/Documents/school/db_data/role_description_16.11.csv",
        "/Users/LTV/Documents/school/db_data/statistics_type_16.11.csv",
        "/Users/LTV/Documents/school/db_data/external_system_16.11.csv",
        "/Users/LTV/Documents/school/db_data/profile_educational_institution_17.11.csv",
        "/Users/LTV/Documents/school/db_data/profile_role_16.11.csv",
        "/Users/LTV/Documents/school/FoxFord Stats/course_structure_fox_ford.csv",
        "/Users/LTV/Documents/school/db_data/educational_course_type_16.11.csv",
        "/Users/LTV/Documents/school/FoxFord Stats/statistics"
    )

    course_foxford.get_report()

    course_uchi = Course_Uchi(
        "/Users/LTV/Documents/school/db_data/role_description_16.11.csv",
        "/Users/LTV/Documents/school/db_data/statistics_type_16.11.csv",
        "/Users/LTV/Documents/school/db_data/external_system_16.11.csv",
        "/Users/LTV/Documents/school/db_data/profile_educational_institution_17.11.csv",
        "/Users/LTV/Documents/school/db_data/profile_role_16.11.csv",
        "/Users/LTV/Documents/school/db_data/educational_courses_16.11.csv",
        "/Users/LTV/Documents/school/db_data/educational_course_type_16.11.csv",
        "/Users/LTV/Documents/school/uchi/statistics"
    )

    course_uchi.get_report()

    print()

if __name__ == "__main__":
    main()

    # issues uchi.ru first period ids; no approved status

#%%



# student_statistics_1c_nd, corrupted = process_course_statistics(
#     ,
#     profile_roles,
#     role_descriptions,
#     statistics_type,
#     course_structure,
#     course_types,
#     external_system
#     )
# # %%
#

#
# foxford_course_structure = load_foxford_course_structure("/Users/LTV/Documents/school/FoxFord Stats/Контент_для_загрузки_Фоксфорд_Иннополис.xlsx")

# %%
