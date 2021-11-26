import argparse
import json
import logging
import os
import sqlite3
from collections import defaultdict, namedtuple
from datetime import datetime
from pathlib import Path
from shutil import rmtree
from time import sleep
from typing import Optional
from zipfile import ZipFile

import numpy as np
import pandas as pd
from math import isnan

from natsort import index_natsorted
from tqdm import tqdm

Reports = namedtuple(
    "Reports",
    [
        "user_report",
        "courses_report",
        "convergence_report",
        "school_active_students_report",
        "region_active_students_report",
        "billing_report"
    ]
)


class SQLTable:
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename)
        self.path = filename

    def replace_records(self, table, table_name):
        table.to_sql(table_name, con=self.conn, if_exists='replace', index=False, index_label=table.columns)

    def add_records(self, table, table_name):
        table.to_sql(table_name, con=self.conn, if_exists='append', index=False, index_label=table.columns)

    def query(self, query_string, **kwargs):
        return pd.read_sql(query_string, self.conn, **kwargs)

    def drop_table(self, table_name):
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.conn.commit()

    def __del__(self):
        self.conn.close()
        # if os.path.isfile(self.path):
        #     os.remove(self.path)


class SharedModel:
    def __init__(
            self, *, billing_info, student_grades, statistics_type, external_system,
            profile_educational_institution, course_structure, course_types, course_statistics, last_export,
            resources_path
    ):
        self.resources_path = Path(resources_path)
        self.statistics_import_chunk_size = 1000000
        self.set_paths()
        self.load_state()

        self.db = SQLTable(self.db_path)

        self.set_new_data_flag(last_export)

        self.load_billing_info(billing_info)
        # self.load_grade_description(grade_description)
        self.load_student_grades(student_grades)
        self.load_statistics_type(statistics_type)
        self.load_external_system(external_system)
        self.load_course_types(course_types)
        self.load_profile_approved_status(profile_educational_institution)
        self.load_course_structure(course_structure)
        self.load_course_statistics(course_statistics)
        self.corrupted = []
        self.student_statistics = {}

    def set_paths(self):
        self.state_file_path = self.resources_path.joinpath(f"{self.__class__.__name__}___state_file.json")
        self.db_path = self.resources_path.joinpath(f"{self.__class__.__name__}.db")

    def __del__(self):
        self.save_state()

    def save_state(self):
        with open(self.state_file_path, "w") as state_file:
            state_file.write(
                json.dumps({
                    "processed_files": self.processed_files,
                    "last_processed_export": self.last_processed_export
                })
            )

    def load_state(self):
        if self.state_file_path.is_file():
            with open(self.state_file_path, "r") as state_file:
                state = json.loads(state_file.read())
                self.processed_files = state["processed_files"]
                self.last_processed_export = state["last_processed_export"]
        else:
            self.processed_files = []
            self.last_processed_export = ""

    def set_new_data_flag(self, last_export):
        self.last_export = last_export
        self.has_new_data = self.last_export != self.last_processed_export

    def check_for_nas(self, data, field, path):
        if data[field].hasnans:
            nan_data = data[data[field].isna()]
            path = Path(path)
            parent = path.parent
            name = path.name
            nan_data.to_csv(parent.joinpath(f"___{name}___{field}_nas.tsv"), index=False, sep="\t")
            data.dropna(subset=[field], inplace=True)

        return data

    def load_billing_info(self, path):
        data = pd.read_csv(path, dtype={"price": "Float32"})
        data.dropna(subset=["price"], inplace=True)
        data.drop_duplicates(subset=["short_name", "course_name"], inplace=True)
        self.billing_info = dict(zip(
            zip(data["short_name"], data["course_name"]), data["price"]
        # zip(data["short_name"], data["course_name"], data["grade"]), data["price"]
        ))

    def load_grade_description(self, path):
        data = pd.read_csv(path)
        self.grade_description = dict(zip(data["id"], data["grade"]))

    def load_student_grades(self, path):
        if self.has_new_data:
            data = pd.read_csv(path, dtype={"grade": "Int32"})
            self.check_for_nas(data, "grade", path)
            data["profile_id"] = data["id"]
            self.db.replace_records(data[["profile_id", "grade"]], "student_grades")

    def load_role_descriptions(self, path):
        if self.has_new_data:
            data = pd.read_csv(path)
            data["role_id"] = data["id"]
            data["description"] = data["role"]
            self.db.replace_records(data[["role_id", "description"]], "role_description")

    def load_statistics_type(self, path):
        data = pd.read_csv(path)
        self.statistics_type = dict(zip(data["id"], data["type_name"]))
        # return {
        #     "1ad6841e-2e64-4720-852c-fa2ad2fd5714": "login",
        #     "8e4870de-468e-4bfa-9867-daa609693b49": "logout",
        #     "ab201fae-44b7-4d4c-95f2-50bd0a0c1cb7": "started_studying",
        #     "4f6c88b8-2131-4948-bd3c-b34e3d491171": "stopped_studying"
        # }

    def load_external_system(self, path):
        data = pd.read_csv(path)
        self.external_system = dict(zip(data["system_code"], data["short_name"]))

    def load_profile_approved_status(self, path):
        if self.has_new_data:
            data = pd.read_csv(path)
            self.db.replace_records(data[["profile_id", "approved_status", "role", "updated_at"]], "profile_approved_status")

    def format_course_structure_columns(self, data):
        fields = ["id", "deleted", "course_type_id", "parent_id", "external_link", "course_name", "external_id",
                  "external_parent_id", "system_code"]
        return data

    def validate_structure_id(self, id, parent_id, structure):
        assert id not in structure

    def load_course_structure(self, path):
        if self.has_new_data:
            data = self.format_course_structure_columns(pd.read_csv(path))
            fields = data.columns
            structure = {}
            for ind, row in data.iterrows():
                self.validate_structure_id(row["id"], row["parent_id"], structure)
                structure[row["id"]] = {key: row[key] for key in fields}
            self.structure = structure

            mapping = []
            for id_ in structure:
                course_name, provider = self.find_subject(id_)
                mapping.append({
                    "material_id": id_,
                    "course_name": course_name,
                    "provider": provider
                })
            data = pd.DataFrame(mapping)
            self.db.replace_records(data, "course_information")

    def load_course_types(self, path):
        data = pd.read_csv(path)
        self.course_types = {
            id: type_name for id, type_name in data.values
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
            course_name = course["course_name"]
            course_type = self.get_course_type(course["course_type_id"])
            provider = self.external_system[course["system_code"]]
            self.validate_course(course_name, course_type, provider)
            return course_name, provider
        return self.find_subject(parent_id)

    def map_course_statistics_columns(self, data):
        pass

    def preprocess_chunk(self, chunk: pd.DataFrame, path):
        self.map_course_statistics_columns(chunk)
        column_order = ["profile_id", "educational_course_id", "created_at"]
        chunk = chunk[column_order]
        # chunk.dropna(axis=0, inplace=True)
        for col in column_order:
            self.check_for_nas(chunk, col, path)
            # chunk = chunk[~chunk[col].isnull()]
        chunk["created_at"] = chunk["created_at"].dt.normalize()
        chunk.drop_duplicates(subset=column_order, inplace=True)
        return chunk

    def prepare_statistics_table(self):
        pass

    def load_course_statistics(self, path, date_field="created_at", **kwargs):
        self.prepare_statistics_table()
        filename = os.path.basename(path)
        if filename not in self.processed_files:
            for chunk in pd.read_csv(path, chunksize=self.statistics_import_chunk_size, parse_dates=[date_field], **kwargs):
                self.db.add_records(self.preprocess_chunk(chunk, path), "course_statistics")
            self.processed_files.append(filename)

    def entry_valid(self, profile_id, statistic_type_id, educational_course_id, created_at):
        profile_id = profile_id #entry["profile_id"]
        course_id = educational_course_id # entry["educational_course_id"]
        if isinstance(profile_id, float) and isnan(profile_id):
            return False
        elif isinstance(course_id, float) and isnan(course_id) or pd.isna(course_id):
            return False
        else:
            return True

    def validate_course(self, subject_name, course_type, provider):
        if subject_name == course_type == provider == None:
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

    def prepare_for_report(self):

        if self.has_new_data:
            self.full_report = self.db.query(
                """
                SELECT
                platform, course_name, course_usage.profile_id, grade, approved_status, active_days
                FROM (
                    SELECT 
                    platform, course_name, profile_id,
                    approved_status, role, COUNT(DISTINCT created_at) AS "active_days"
                    from (
                        SELECT
                        course_information.provider as "platform", 
                        course_information.course_name as "course_name",
                        course_statistics.profile_id as "profile_id", 
                        profile_approved_status.approved_status as "approved_status", 
                        profile_approved_status.role as "role",
                        course_statistics.created_at as "created_at"
                        from course_statistics 
                        INNER JOIN course_information on course_statistics.educational_course_id = course_information.material_id
                        LEFT JOIN profile_approved_status on course_statistics.profile_id = profile_approved_status.profile_id
                        WHERE profile_approved_status.role = 'STUDENT'
                    ) 
                    GROUP BY platform, course_name, profile_id
                ) as course_usage
                LEFT JOIN student_grades on course_usage.profile_id = student_grades.profile_id
                """
            )
            self.db.drop_table("full_report")
            self.db.add_records(self.full_report, "full_report")
        else:
            self.full_report = self.db.query(
                """
                SELECT * from full_report
                """
            )

    def sort_course_names(self, report_df):
        report_df.sort_values(
            by="Название", key=lambda x: np.argsort(index_natsorted(report_df["Название"])), inplace=True
        )
        report_df.sort_values(
            by="Платформа", key=lambda x: np.argsort(index_natsorted(report_df["Платформа"])), inplace=True
        )

    def add_licence_info(self, user_report: pd.DataFrame, courses_report: pd.DataFrame):
        licences = []
        sum_total = []
        for provider in user_report["Платформа"]:
            table = courses_report.query(f"Платформа == '{provider}'")
            licences.append(table["Активные Подтверждённые"].sum())
            sum_total.append(table["Всего за курс"].sum())

        user_report["Общее количество лицензий на оплату"] = licences
        user_report["Общая сумма на оплату"] = sum_total

        # empty = {key: "" for key in user_report.columns}
        # total = {key: user_report[key].sum() for key in user_report.columns if key != "Платформа"}
        # total["Платформа"] = "Итого"

        # user_report = user_report.append(pd.DataFrame.from_records([empty, total]))
        return user_report

    def convergence_stat(self):

        active_data = self.db.query(
            """
            SELECT
            platform as "Активных дней",
            COUNT(CASE WHEN active_days >= 2 THEN 1 ELSE NULL END) as "2 дня",
            COUNT(CASE WHEN active_days >= 3 THEN 1 ELSE NULL END) as "3 дня",
            COUNT(CASE WHEN active_days >= 4 THEN 1 ELSE NULL END) as "4 дня",
            COUNT(CASE WHEN active_days >= 5 THEN 1 ELSE NULL END) as "5 дня и более",
            COUNT(profile_id) as "Всего пользоателей"
            FROM (
                SELECT
                platform, profile_id, max(active_days) as "active_days"
                FROM full_report
                GROUP BY platform, profile_id
            )
            GROUP BY platform
            """
        ).set_index("Активных дней").T

        col_order = []

        for col in active_data.columns:
            active_data[f"{col}, %"] = active_data[col] / active_data.loc["Всего пользоателей", col] * 100.
            col_order.append(col)
            col_order.append(f"{col}, %")

        self._conv_stat = active_data[col_order]


    def prepare_report(self):

        user_report = []
        timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

        for platform, course in self.full_report.groupby("platform"):
            user_report.append({
                "Платформа": platform,
                "Всего пользователей": course["profile_id"].nunique(),
                "Активных пользователей": course.query("active_days >=5")["profile_id"].nunique(),
                "Подтверждённых пользователей использующих сервис": course.query("approved_status == 'APPROVED'")["profile_id"].nunique(),
                "Активных и подтверждённых пользователей": course.query("active_days >=5 and approved_status == 'APPROVED'")[
                    "profile_id"].nunique(),
            })

        report = []

        for (platform, course_name), course in self.full_report.groupby(["platform", "course_name"]):
            billing_key = (platform, course_name)
            course_price = self.billing_info.get(billing_key, 0.)
            approved_and_active = course.query("active_days >=5 and approved_status == 'APPROVED'")["profile_id"].nunique()
            report.append({
                "Платформа": platform,
                "Название": course_name,
                # "Класс": grade,
                "Всего": course["profile_id"].nunique(),
                "Активные Подтверждённые": approved_and_active,
                "Активные Всего": course.query("active_days >=5")["profile_id"].nunique(),
                "Цена за одну лицензию": course_price,
                "Всего за курс": course_price * approved_and_active
            })

        self.courses_report = pd.DataFrame(report)

        self.sort_course_names(self.courses_report)
        # self.courses_report.to_csv(f"{self.__class__.__name__}_report_{timestamp}.csv", index=False)
        self.user_report = pd.DataFrame(user_report)
        # self.user_report.to_csv(f"{self.__class__.__name__}_user_report_{timestamp}.csv", index=False)

    def get_report(self):
        # if self.has_new_data:
        self.prepare_for_report()
        self.prepare_report()
        self.convergence_stat()
        self.last_processed_export = self.last_export
        self.user_report = self.add_licence_info(self.user_report, self.courses_report)
        return self.courses_report, self.user_report, self._conv_stat

    def get_people_for_billing(self, num_days_to_be_considered_active=5):
        people_courses_for_billing = self.db.query(
            f"""
            SELECT platform, course_name, profile_id
            FROM full_report
            WHERE approved_status = 'APPROVED' and active_days >= {num_days_to_be_considered_active}
            """
        )

        people_approved_date = self.db.query(
            f"""
            SELECT profile_id, updated_at as "approved_date"
            FROM profile_approved_status
            """
        )

        people_approved_date = dict(zip(people_approved_date["profile_id"], people_approved_date["approved_date"]))

        people_courses_filter = set(
            (pl, c, per) for pl, c, per in people_courses_for_billing.values
        )

        # pre_billing = self.db.query(
        #     """
        #     SELECT
        #     profile_id, course_name, provider, created_at as "visit_date"
        #     FROM (
        #         SELECT
        #         *
        #         FROM course_statistics AS a
        #         WHERE (a.profile_id, a.educational_course_id, a.created_at) IN (
        #               SELECT DISTINCT b.profile_id, b.educational_course_id, b.created_at
        #               FROM course_statistics AS b
        #               WHERE a.profile_id = b.profile_id and a.educational_course_id = b.educational_course_id
        #               ORDER BY b.created_at
        #               LIMIT 5
        #          )
        #     )
        #     LEFT JOIN course_information on educational_course_id = material_id
        #     """,
        #     chunksize=self.statistics_import_chunk_size
        # )
        course_statistics = self.db.query(
            """
            SELECT 
            DISTINCT provider, course_name, profile_id, created_at as "visit_date" 
            FROM 
            course_statistics
            LEFT JOIN course_information on educational_course_id = material_id
            ORDER BY created_at
            """,
            chunksize=self.statistics_import_chunk_size
        )

        people_courses_visits = defaultdict(list)
        for chunk in course_statistics:
            for provider, course_name, profile_id, visit_date in chunk[
                ["provider", "course_name", "profile_id", "visit_date"]
            ].values:
                key = (provider, course_name, profile_id)
                if key not in people_courses_filter:
                    continue
                if len(people_courses_visits[key]) == num_days_to_be_considered_active:
                    continue
                people_courses_visits[key].append(visit_date)

        records = []
        for key, dates in people_courses_visits.items():
            assert len(dates) == num_days_to_be_considered_active
            platform, course_name, profile_id = key
            record = {
                "Наименование образовательной цифровой площадки": platform,
                "Наименование ЦОК": course_name,
                "Идентификационный номер обучающегося": profile_id,
            }

            for ind, date in enumerate(dates):
                record[f"День {ind}"] = date.split(" ")[0]

            record["Дата подтверждения обучающегося"] = people_approved_date[profile_id].split(" ")[0]

            records.append(record)

        return pd.DataFrame.from_records(records).sort_values(by=[
            "Наименование образовательной цифровой площадки", "Наименование ЦОК"
        ])


class Course_1C_ND(SharedModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def prepare_statistics_table(self):
        if self.has_new_data:
            self.db.drop_table("course_statistics")
            self.processed_files = []


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

    def load_course_statistics(self, path, date_field=None, dtype=None):
        files = sorted(os.listdir(path))
        files = filter(lambda filename: filename.endswith(".csv"), files)
        for file in files:
            super().load_course_statistics(os.path.join(path, file), "createdat")


class Course_MEO(SharedModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def format_course_structure_columns(self, data):
        return data

    def map_course_statistics_columns(self, data):
        data.rename({
            "Start": "created_at",
            "profileId": "profile_id",
            "CourseId": "educational_course_id",
        }, axis=1, inplace=True)
        return data

    def load_course_statistics(self, path, date_field=None, dtype=None):
        files = sorted(os.listdir(path))
        files = filter(lambda filename: filename.endswith(".csv"), files)
        for file in files:
            super().load_course_statistics(os.path.join(path, file), "Start", sep=";", dtype={"CourseId": "string"})

    def load_course_structure(self, path):
        if self.has_new_data:
            data = self.format_course_structure_columns(pd.read_csv(path, dtype={"material_id": "string"}))
            self.db.replace_records(data, "course_information")

    def prepare_for_report(self):
        # self.full_report = self.db.query(
        #     """
        #     SELECT
        #     platform, course_name, course_usage.profile_id, grade, active_days
        #     FROM (
        #         SELECT
        #         platform, course_name, profile_id, COUNT(DISTINCT created_at) AS "active_days"
        #         from (
        #             SELECT
        #             course_information.provider as "platform",
        #             course_information.course_name as "course_name",
        #             course_statistics.profile_id as "profile_id",
        #             course_statistics.created_at as "created_at"
        #             from course_statistics
        #             INNER JOIN course_information on course_statistics.educational_course_id = course_information.material_id
        #         )
        #         GROUP BY platform, course_name, profile_id
        #     ) as course_usage
        #     LEFT JOIN student_grades on course_usage.profile_id = student_grades.profile_id
        #     """
        # )
        self.full_report = self.db.query(
            """
            SELECT
            platform, course_name, course_usage.profile_id, grade, approved_status, active_days
            FROM (
                SELECT 
                platform, course_name, profile_id,
                approved_status, COUNT(DISTINCT created_at) AS "active_days"
                from (
                    SELECT
                    course_information.provider as "platform", 
                    course_information.course_name as "course_name",
                    course_statistics.profile_id as "profile_id", 
                    profile_approved_status.approved_status as "approved_status", 
                    course_statistics.created_at as "created_at"
                    from course_statistics 
                    INNER JOIN course_information on course_statistics.educational_course_id = course_information.material_id
                    LEFT JOIN profile_approved_status on course_statistics.profile_id = profile_approved_status.profile_id
                ) 
                GROUP BY platform, course_name, profile_id
            ) as course_usage
            LEFT JOIN student_grades on course_usage.profile_id = student_grades.profile_id
            """
        )
        self.full_report = self.full_report.astype({"grade": "Int32"})
        self.db.drop_table("full_report")
        self.db.add_records(self.full_report, "full_report")


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
            "statisticsTypeId": "statistic_type_id",
            "userId": "profile_id",
            "externalId": "educational_course_id",
        }, axis=1, inplace=True)
        return data[["profile_id", "educational_course_id", "created_at"]]

    def load_course_statistics(self, path, date_field=None, dtype=None):
        files = sorted(os.listdir(path))
        files = filter(lambda filename: filename.endswith(".csv"), files)
        for file in files:
            super().load_course_statistics(os.path.join(path, file), "createdAt", dtype={"externalId": "string"})


class ReportWriter:
    def __init__(self, last_export, html_path, queries_path: Path):
        self.last_export = last_export
        self.html_path = Path(html_path)
        # self.special_files = special_files
        self.queries_path = queries_path

        self.sheet_names = []
        self.sheet_data = []
        self.sheet_options = []

        self.special_files = set([
            "student_16.11.csv",
            "educational_course_statistic_16.11.csv",
            "educational_course_type_16.11.csv",
            "educational_courses_16.11.csv",
            "educational_courses_only_courses_16.11.csv",
            "external_system_16.11.csv",
            "last_export",
            "profile_educational_institution_16.11.csv",
            "profile_role_16.11.csv",
            "role_16.11.csv",
            "school_students.csv",
            "statistic_type_16.11.csv"
        ])

    def create_definitions(self, tab_names):
        definitions = ""
        for ind, name in enumerate(tab_names):
            if ind == 0:
                default_tab = ' id="defaultOpen"'
            else:
                default_tab = ""
            definitions += f"""<button class="tablinks" onclick="openCity(event, '{name}')"{default_tab}>{name}</button>\n"""

        definiitons_div = f"""
<div class="tab">
  {definitions}
</div>
        """
        return definiitons_div

    def create_content(self, tab_names, tab_content):
        contents = ""
        for name, content in zip(tab_names, tab_content):
            contents += f"""
<div id="{name}" class="tabcontent">
  {content.to_html(classes="striped", index=False)}
</div>
"""
        return contents

    def write_html(self, sheet_names, sheet_data, sheet_options, name):
        from html_string import html_string
        self.html_string = html_string
        html = self.html_string.format(
            xlsx_location=f"{name}.xlsx",
            tabdefinitions=self.create_definitions(sheet_names),
            tabcontent=self.create_content(sheet_names, sheet_data)
        )
        with open(self.html_path.joinpath(f"{name}.html"), "w") as report_html:
            report_html.write(html)

    def format_worksheet(self, workbook, worksheet, data, max_column_len=80, long_column=None):
        format = workbook.add_format({'text_wrap': True})
        for ind, col in enumerate(data.columns):
            def get_max_len(data):
                return max(data.astype(str).map(len))
            col_width = min(
                max(get_max_len(data[col]), len(col)),
                max_column_len if ind != long_column else max_column_len * 2
            ) + 4
            worksheet.set_column(ind, ind, col_width, format)

    def normalize_worksheet_name(self, name):
        prohibited = "[]:*?/\\"
        for c in prohibited:
            name = name.replace(c, " ")
        if len(name) > 31:
            parts = name.split("_")
            name = "_".join([part[:4] + "." for part in parts])
        return name

    def write_xlsx(self, sheet_names, sheet_data, sheet_options, name):
        with pd.ExcelWriter(self.html_path.joinpath(f'{name}.xlsx'), engine='xlsxwriter') as writer:
            for sheet_name, data, options in zip(sheet_names, sheet_data, sheet_options):
                data.to_excel(writer, sheet_name=sheet_name, index=False)
                self.format_worksheet(
                    writer.book, writer.sheets[sheet_name], data, long_column=options.get("long_column", None)
                )

    def write_index_html(self, name):
        # <head>
        # <meta http-equiv="refresh" content="0; url={name}.html" />
        html = f"""
<html>
<head>
<meta charset="UTF-8">
<title>Отчет по платформам</title>
</head>
<body>
<p>
Последнее обновление: {self.last_export}
</p>
<p>
<a id="report" href="report_{self.last_export}.xlsx">Отчёт</a>
</p>
<p>
<a id="billing" href="billing_report_{self.last_export}.xlsx">Биллинг</a>
</p>
<p>
<a id="region_arc" href="region_report_{self.last_export}.zip">Отчет по регионам zip</a>
</p>
</body>
</html>
"""
        with open(self.html_path.joinpath("index.html"), "w") as index_html:
            index_html.write(html)

    def add_sheet(self, name, data, options=None):
        if options is None:
            options = {"long_column": None}
        self.sheet_names.append(name)
        self.sheet_data.append(data)
        self.sheet_options.append(options)

    def add_extra_sheets(self):

        max_len, max_width = 1048576, 16384
        for file_path in self.queries_path.iterdir():
            filename = file_path.name
            if not filename.endswith(".csv") or filename.startswith("___") or filename in self.special_files:
                continue

            data = pd.read_csv(file_path)
            if data.shape[0] >= max_len or data.shape[1] > max_width:
                logging.info(f"Skipping {filename}")
                continue
            data = data.dropna(how="all")

            self.add_sheet(self.normalize_worksheet_name(filename.strip(".csv")), data)

    def get_report_name(self):
        return f"report_{self.last_export}"

    def save_report(self):
        export_file_name = self.get_report_name()

        self.write_xlsx(self.sheet_names, self.sheet_data, self.sheet_options, export_file_name)
        self.write_html(self.sheet_names, self.sheet_data, self.sheet_options, export_file_name)
        self.write_index_html(export_file_name)


class RegionReportWriter(ReportWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def add_region_info_as_sheets(self, region_report):
        region_index = []

        for ind, (region, data) in enumerate(tqdm(
                region_report.groupby([
                    "Регион"
                ]),
                desc="Preparing billing sheets", leave=False
        )):
            sheet_name = f"{ind + 1}"
            region_index.append({
                "Регион": region,
                "Страница": sheet_name,
            })
            self.add_sheet(sheet_name,
                           data.sort_values(["Всего учеников"], ascending=False),
                           {"long_column": 1})

        course_index_df = pd.DataFrame.from_records(region_index)

        self.sheet_names.insert(0, "Индекс регионов")
        self.sheet_data.insert(0, course_index_df)
        self.sheet_options.insert(0, {"long_column": 1})

    def get_report_name(self):
        return f"region_report_{self.last_export}"

    def save_report(self):
        # export_file_name = self.get_report_name()

        region_report_folder = f"region_report_{self.last_export}"

        save_location = self.html_path.joinpath(region_report_folder)

        if not save_location.is_dir():
            save_location.mkdir()

        for ind, (name, data, options) in enumerate(zip(self.sheet_names, self.sheet_data, self.sheet_options)):
            if ind == 0:
                continue
            region_name = data.iloc[0, 0].replace("/", "")
            export_file_name = os.path.join(region_report_folder, region_name)
            self.write_xlsx(["Данные по региону"], [data], [options], export_file_name)
            break

        curr_dir = os.getcwd()
        os.chdir(self.html_path)

        with ZipFile(
            region_report_folder + ".zip",
            mode='w'
        ) as zipper:
            for file in save_location.iterdir():
                zipper.write(Path(region_report_folder).joinpath(file.name))

        os.chdir(curr_dir)

        rmtree(save_location)


class BillingReportWriter(ReportWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def add_billing_info_as_sheets(self, billing_report):
        course_index = []

        for ind, ((platform, course), data) in enumerate(tqdm(
                billing_report.groupby([
                    "Наименование образовательной цифровой площадки", "Наименование ЦОК"
                ]),
                desc="Preparing billing sheets", leave=False
        )):
            sheet_name = f"{ind + 1}"
            course_index.append({
                "Наименование образовательной цифровой площадки": platform,
                "Наименование ЦОК": course,
                "Страница": sheet_name,
                "Всего лицензий": len(data)
            })
            self.add_sheet(sheet_name, data, {"long_column": 1})

        course_index_df = pd.DataFrame.from_records(course_index)

        self.sheet_names.insert(0, "Индекс курсов")
        self.sheet_data.insert(0, course_index_df)
        self.sheet_options.insert(0, {"long_column": 1})

    def get_report_name(self):
        return f"billing_report_{self.last_export}"

    def write_index_html(self, name):
        pass


class SchoolActivityReportWriter(ReportWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_report_name(self):
        return f"active_and_approved_by_schools_{self.last_export}"

    def write_index_html(self, name):
        pass


def get_last_export(path):
    with open(path, "r") as last_export:
        last_export = last_export.read().strip()
    return last_export


def enrich_user_report(user_report):
    empty = {key: "------" for key in user_report.columns}
    total = {key: user_report[key].sum() for key in user_report.columns if key != "Платформа"}
    extra = [
        {
            "Платформа": "Цена за лицензию",
            "Всего пользователей": total["Общая сумма на оплату"] / total["Общее количество лицензий на оплату"]
        }, {
            "Платформа": "Лицензий на человека",
            "Всего пользователей": total["Общее количество лицензий на оплату"] / total[
                "Активных и подтверждённых пользователей"]
        }
    ]
    total["Платформа"] = "Итого"
    user_report = user_report.append(pd.DataFrame.from_records([empty, total, empty] + extra))
    return user_report


def get_reports(provider_data, region_info_path) -> Optional[Reports]:
    user_reports = []
    courses_reports = []
    convergence_reports = []
    billing_reports = []

    for provider_d in tqdm(provider_data, desc="Preparing reports"):
        courses_report, user_report, conv_stat = provider_d.get_report()
        user_reports.append(user_report)
        courses_reports.append(courses_report)
        convergence_reports.append(conv_stat)
        billing_reports.append(provider_d.get_people_for_billing())

    if len(user_reports) > 0:
        user_report = pd.concat(user_reports, axis=0)
        courses_report = pd.concat(courses_reports, axis=0)
        convergence_report = pd.concat(convergence_reports, axis=1)
        billing_report = pd.concat(billing_reports, axis=0)
        convergence_report = convergence_report.reset_index().rename({"index": "Активных дней"}, axis=1)
        from merge_activity_informaiton import merge_activity_and_regions
        schools_active = merge_activity_and_regions([p.db for p in provider_data], region_info_path)
        regions_active = schools_active.groupby('Регион').sum().reset_index()

        return Reports(
            enrich_user_report(user_report),
            courses_report,
            convergence_report,
            schools_active,
            regions_active,
            billing_report
        )
    else:
        return None


def process_statistics(
        *, billing, student_grades, statistics_type, external_system, profile_educational_institution,
        course_structure, course_structure_foxford, course_structure_meo, course_types, course_statistics, course_statistics_foxford,
        course_statistics_uchi, course_statistics_meo, last_export, html_path, resources_path, region_info_path
):
    last_export = get_last_export(last_export)

    logging.info("Running scheduled processing")

    provider_data = [
        Course_1C_ND(
            billing_info=billing, student_grades=student_grades, statistics_type=statistics_type,
            external_system=external_system, profile_educational_institution=profile_educational_institution,
            course_structure=course_structure, course_types=course_types, course_statistics=course_statistics,
            last_export=last_export, resources_path=resources_path
        ),
        Course_FoxFord(
            billing_info=billing, student_grades=student_grades, statistics_type=statistics_type,
            external_system=external_system, profile_educational_institution=profile_educational_institution,
            course_structure=course_structure_foxford, course_types=course_types,
            course_statistics=course_statistics_foxford, last_export=last_export, resources_path=resources_path
        ),
        Course_MEO(
            billing_info=billing, student_grades=student_grades, statistics_type=statistics_type,
            external_system=external_system, profile_educational_institution=profile_educational_institution,
            course_structure=course_structure_meo, course_types=course_types,
            course_statistics=course_statistics_meo, last_export=last_export, resources_path=resources_path
        ),
        Course_Uchi(
            billing_info=billing, student_grades=student_grades, statistics_type=statistics_type,
            external_system=external_system, profile_educational_institution=profile_educational_institution,
            course_structure=course_structure, course_types=course_types, course_statistics=course_statistics_uchi,
            last_export=last_export, resources_path=resources_path
        )
    ]

    reports = get_reports(provider_data, region_info_path)

    if reports is not None:
        logging.info("Preparing new report")
        report_writer = ReportWriter(last_export, html_path, queries_path=Path(course_types).parent)

        report_writer.add_sheet("Сводная", reports.user_report)

        for provider, table in reports.courses_report.groupby("Платформа"):
            report_writer.add_sheet(
                report_writer.normalize_worksheet_name(provider),
                table, {"long_column": 1}
            )

        report_writer.add_sheet("Статистика активности", reports.convergence_report)
        report_writer.add_sheet(
            "Активно и подтвержд. по школам", reports.school_active_students_report, {"long_column": 1}
        )

        report_writer.add_sheet(
            "Активно и подтвержд. по рег.", reports.region_active_students_report, {"long_column": 1}
        )

        report_writer.add_extra_sheets()

        report_writer.save_report()

        billing_report_writer = BillingReportWriter(last_export, html_path, queries_path=Path(course_types).parent)
        billing_report_writer.add_billing_info_as_sheets(reports.billing_report)
        billing_report_writer.save_report()

        region_report_writer = RegionReportWriter(last_export, html_path, queries_path=Path(course_types).parent)
        region_report_writer.add_region_info_as_sheets(reports.school_active_students_report)
        region_report_writer.save_report()

        school_report_writer = SchoolActivityReportWriter(last_export, html_path, queries_path=Path(course_types).parent)
        school_report_writer.add_sheet(
            "Активно и подтвержд. по школам", reports.region_active_students_report, {"long_column": 1}
        )
        school_report_writer.save_report()


    else:
        logging.info("No new data")





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--billing", default=None)
    parser.add_argument("--student_grades", default=None)
    parser.add_argument("--statistics_type", default=None)
    parser.add_argument("--external_system", default=None)
    parser.add_argument("--profile_educational_institution", default=None)
    parser.add_argument("--course_structure", default=None)
    parser.add_argument("--course_structure_foxford", default=None)
    parser.add_argument("--course_structure_meo", default=None)
    parser.add_argument("--course_types", default=None)
    parser.add_argument("--course_statistics", default=None)
    parser.add_argument("--course_statistics_foxford", default=None)
    parser.add_argument("--course_statistics_uchi", default=None)
    parser.add_argument("--course_statistics_meo", default=None)
    parser.add_argument("--region_info_path", default=None)
    parser.add_argument("--last_export", default=None)
    parser.add_argument("--html_path", default=None)
    parser.add_argument("--resources_path", default=None)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(module)s:%(lineno)d:%(message)s")

    process_statistics(**args.__dict__)

