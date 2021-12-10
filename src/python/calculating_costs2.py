import argparse
import json
import logging
import os
import pickle
import sqlite3
from collections import defaultdict, namedtuple
from datetime import datetime
from pathlib import Path
from shutil import rmtree
from typing import Optional
from zipfile import ZipFile

import numpy as np
import pandas as pd
from math import isnan

from natsort import index_natsorted
from tqdm import tqdm

from src.python.merge_activity_informaiton import merge_activity_and_regions

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
        table.to_sql(table_name, con=self.conn, if_exists='replace', index=False)
        self.create_index_for_table(table, table_name)

    def add_records(self, table, table_name):
        table.to_sql(table_name, con=self.conn, if_exists='append', index=False)
        self.create_index_for_table(table, table_name)

    def create_index_for_table(self, table, table_name):
        self.execute(
            f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name} 
                    ON {table_name}({','.join(repr(col) for col in table.columns)})
                    """
        )

    def query(self, query_string, **kwargs):
        return pd.read_sql(query_string, self.conn, **kwargs)

    def execute(self, query_string):
        self.conn.execute(query_string)
        self.conn.commit()

    def drop_table(self, table_name):
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.conn.execute(f"DROP INDEX IF EXISTS idx_{table_name}")
        self.conn.commit()

    def __del__(self):
        self.conn.close()
        # if os.path.isfile(self.path):
        #     os.remove(self.path)


class SharedModel:
    def __init__(
            self, *, billing_info, student_grades, statistics_type, external_system,
            profile_educational_institution, course_structure, course_types, course_statistics, last_export,
            resources_path, freeze_date=None, educational_institution=None, minute_activity=False
    ):
        self.minute_activity = minute_activity
        self.resources_path = Path(resources_path)
        self.statistics_import_chunk_size = 1000000
        self.freeze_date = freeze_date
        self.set_paths()
        self.load_state()

        self.db = SQLTable(self.db_path)

        self.set_new_data_flag(last_export)

        # self.load_grade_description(grade_description)
        self.load_student_grades(student_grades)
        # self.load_statistics_type(statistics_type)
        self.load_external_system(external_system)
        self.load_educational_institution(educational_institution)
        self.load_course_types(course_types)
        self.load_profile_approved_status(profile_educational_institution)
        self.load_course_structure(course_structure)
        self.load_billing_info(billing_info)
        self.load_course_statistics(course_statistics)
        if self.minute_activity:
            self.compute_active_days()
        # self.corrupted = []
        # self.student_statistics = {}

    def set_paths(self):
        self.state_file_path = self.resources_path.joinpath(f"{self.__class__.__name__}___state_file.json")
        self.db_path = self.resources_path.joinpath(f"{self.__class__.__name__}.db")
        self.mappings_path = self.resources_path.joinpath(f"{self.__class__.__name__}___mappings.pkl")

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
        self.save_mappings()

    def load_state(self):
        if self.state_file_path.is_file():
            with open(self.state_file_path, "r") as state_file:
                state = json.loads(state_file.read())
                self.processed_files = state["processed_files"]
                self.last_processed_export = state["last_processed_export"]
        else:
            self.processed_files = []
            self.last_processed_export = ""

        self.load_mappings()

    def set_new_data_flag(self, last_export):
        self.last_export = last_export
        self.has_new_data = self.last_export != self.last_processed_export

    def load_mappings(self):
        if self.mappings_path.is_file():
            self.mappings = pickle.load(open(self.mappings_path, "rb"))
        else:
            self.mappings = {}

    def save_mappings(self):
        pickle.dump(self.mappings, open(self.mappings_path, "wb"))

    def add_missing_to_mapping(self, ids, mapping):
        for id_ in ids:
            if id_ not in mapping:
                mapping[id_] = len(mapping)

    def convert_ids_to_int(self, table, columns, add_new=True):
        for column in columns:
            if column not in self.mappings:
                mapping = {}
                self.mappings[column] = mapping
            else:
                mapping = self.mappings[column]
            uuid_name = f"{column}_uuid"
            if add_new:
                self.add_missing_to_mapping(table[column].unique(), mapping)
            table.eval(f"{uuid_name} = {column}", inplace=True)
            table.eval(
                f"{column} = {column}.map(@mapping)",
                local_dict={"mapping": lambda id_: mapping.get(id_, pd.NA)},
                inplace=True
            )
            # table[uuid_name] = table[column]
            # table[column] = table[column].apply(lambda id_: mapping.get(id_, pd.NA))  # should fail for unknown ids
            if add_new:
                assert table[uuid_name].nunique() == table[column].nunique()

    def check_for_nas(self, data, field, path, error_buffer=None):
        if data[field].hasnans:
            nan_data = data[data[field].isna()]
            path = Path(path)
            parent = path.parent
            name = path.name
            error_filename = parent.joinpath(f"___{name}___{field}_nas.tsv")
            if error_buffer is None:
                nan_data.to_csv(error_filename, index=False, sep="\t")
            else:
                if error_filename in error_buffer:
                    error_buffer[error_filename] = error_buffer[error_filename].append(nan_data)
                else:
                    error_buffer[error_filename] = nan_data
            data.dropna(subset=[field], inplace=True)

        return data

    def load_billing_info(self, path):
        data = pd.read_csv(path, dtype={"price": "Float32"}).rename({"short_name": "provider"}, axis=1)
        # data.dropna(subset=["price"], inplace=True)
        data.eval("price = price.fillna(0.)", inplace=True)
        data.drop_duplicates(subset=["provider", "course_name"], inplace=True)
        data.eval("provider_course_name = provider + course_name", inplace=True)
        self.convert_ids_to_int(data, ["provider_course_name"])
        data.drop("provider_course_name_uuid", axis=1, inplace=True)
        data.rename({"provider_course_name": "course_id"}, axis=1, inplace=True)
        self.db.replace_records(data[["provider", "course_name", "course_id", "price"]], "billing_info")

    def load_student_grades(self, path):
        if self.has_new_data:
            # data = pd.read_csv(path, dtype={"grade": "Int32"}).rename({"id": "profile_id"}, axis=1)
            data = pd.read_pickle(path, compression=None).rename({"id": "profile_id"}, axis=1)
            self.check_for_nas(data, "grade", path)
            self.convert_ids_to_int(data, ["profile_id"])
            self.db.replace_records(data[["profile_id", "profile_id_uuid", "grade"]], "student_grades")

    def load_external_system(self, path):
        # data = pd.read_csv(path)
        data = pd.read_pickle(path, compression=None)
        self.external_system = dict(zip(data["system_code"], data["short_name"]))

    def read_paper_letters_info(self, path):
        cached = str(path.absolute()).replace(".xlsx", ".bz2")
        if Path(cached).is_file():
            letters_status = pd.read_pickle(cached, compression=None)
        else:
            letters_status = pd.read_excel(
                path, dtype={"ИНН": "string"}
            )
            letters_status.to_pickle(cached, compression=None)
        return letters_status[["ИНН", "Статус письма"]]

    def read_schools_approved_in_november(self, path):
        cached = str(path.absolute()).replace(".xlsx", ".bz2")
        if Path(cached).is_file():
            november_schools = pd.read_pickle(cached, compression=None)
        else:
            november_schools = pd.read_excel(
                path, dtype={"ИНН": "string"}
            )
            november_schools.to_pickle(cached, compression=None)
        return november_schools[["ИНН"]]

    def merge_special_status(self, paper_letters, approved_in_november):
        approved_status = paper_letters.copy()

        approved_in_november = set(
            approved_in_november["ИНН"]
        )

        activated_in_november_status = "Активировали в ноябре"

        special_status_records = []
        for ind, row in approved_status.iterrows():
            rec = {
                "inn": row["ИНН"],
                "special_status": row["Статус письма"]
            }
            if pd.isna(rec["special_status"]) and rec["inn"] in approved_in_november:
                rec["special_status"] = activated_in_november_status
            special_status_records.append(rec)

        for school in approved_in_november - set(approved_status["ИНН"]):
            rec = {
                "inn": school,
                "special_status": activated_in_november_status
            }
            special_status_records.append(rec)

        special_status = pd.DataFrame.from_records(special_status_records)
        return special_status

    def load_educational_institution(self, path):
        if self.has_new_data:
            path = Path(path)
            # data = pd.read_csv(path, dtype={"inn": "string"}) \
            #     .rename({"id": "educational_institution_id"}, axis=1)
            data = pd.read_pickle(path, compression=None) \
                .rename({"id": "educational_institution_id"}, axis=1)

            paper_letters = self.read_paper_letters_info(path.parent.joinpath("schools_paper_letters.xlsx"))

            approved_in_november = self.read_schools_approved_in_november(
                path.parent.joinpath("schools_approved_in_november.xlsx")
            )

            special_status = self.merge_special_status(paper_letters, approved_in_november)

            paper_letters.merge(special_status, how="outer", left_on="ИНН", right_on="inn").to_csv("schools_special_status.csv", index=False)

            merged = data.merge(special_status, how="left", left_on="inn", right_on="inn").drop("inn", axis=1)

            self.convert_ids_to_int(merged, ["educational_institution_id"])

            self.db.replace_records(
                merged[["educational_institution_id", "educational_institution_id_uuid", "special_status"]],
                "educational_institution"
            )

    def load_profile_approved_status(self, path):
        if self.has_new_data:
            # data = pd.read_csv(path)
            data = pd.read_pickle(path, compression=None)
            self.convert_ids_to_int(data, ["profile_id", "educational_institution_id"])

            self.db.replace_records(
                data[[
                    "profile_id", "profile_id_uuid", "approved_status", "role",
                    "educational_institution_id", "educational_institution_id_uuid"
                ]], "profile_approved_status")  # updated_at

    def format_course_structure_columns(self, data):
        # fields = ["id", "deleted", "course_type_id", "parent_id", "external_link", "course_name", "external_id",
        #           "external_parent_id", "system_code"]
        return data

    def validate_structure_id(self, id_, parent_id, structure):
        assert id_ not in structure

    def resolve_structure(self, data):
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
                "educational_course_id": id_,
                "course_name": course_name,
                "provider": provider
            })
        return pd.DataFrame(mapping)

    def prepare_course_ids(self, data):
        data.eval("provider_course_name = provider + course_name", inplace=True)
        self.convert_ids_to_int(data, ["educational_course_id", "provider_course_name"])
        data.eval("course_id = provider_course_name", inplace=True)
        self.educational_course_id2course_id = dict(zip(data["educational_course_id"], data["course_id"]))
        return data

    def load_course_structure(self, path):
        if self.has_new_data:
            data = self.format_course_structure_columns(pd.read_csv(path))
            data = self.resolve_structure(data)
            data = self.prepare_course_ids(data)
            self.db.replace_records(
                data[[
                    "educational_course_id", "educational_course_id_uuid",
                    "course_name", "provider", "course_id"]],
                "course_information"
            )

    def load_course_types(self, path):
        # data = pd.read_csv(path)
        data = pd.read_pickle(path, compression=None)
        self.course_types = {
            id_: type_name for id_, type_name in data.values
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

    def preprocess_chunk(self, chunk: pd.DataFrame, path, error_buffer, drop_duplicates=False):
        path = Path(path)
        self.map_course_statistics_columns(chunk)
        column_order = ["profile_id", "educational_course_id", "created_at"]
        chunk = chunk[column_order]
        # chunk.dropna(axis=0, inplace=True)
        for col in column_order:
            self.check_for_nas(chunk, col, path, error_buffer=error_buffer)
            # chunk = chunk[~chunk[col].isnull()]
        chunk.eval("created_at_original = created_at", inplace=True)
        chunk.eval("created_at = created_at.dt.normalize()", inplace=True)
        # chunk["created_at_original"] = chunk["created_at"]
        # chunk["created_at"] = chunk["created_at"].dt.normalize()
        self.convert_ids_to_int(chunk, ["profile_id", "educational_course_id"], add_new=False)
        chunk.eval( # this lookup can fail only if convert_ids_to_int fails
            "educational_course_id = educational_course_id.map(@resolve_id)",
            local_dict={"resolve_id": lambda id_: self.educational_course_id2course_id.get(id_, pd.NA)},
            inplace=True
        )
        # chunk["educational_course_id"] = chunk["educational_course_id"].apply(
        #     lambda id_: self.educational_course_id2course_id.get(id_, pd.NA)
        # )
        for col in ["profile_id", "educational_course_id"]:
            self.check_for_nas(chunk, col, str(path.absolute()) + f"___unresolved", error_buffer=error_buffer)
        # self.check_for_nas(chunk, "educational_course_id", path.parent.joinpath("statistics_resolved_course_id"))
        if drop_duplicates:
            chunk.drop_duplicates(subset=column_order, inplace=True)
        chunk.drop(["profile_id_uuid", "educational_course_id_uuid"], axis=1, inplace=True)
        return chunk

    def prepare_statistics_table(self):
        pass

    def load_course_statistics(self, path, date_field="created_at", **kwargs):
        self.prepare_statistics_table()
        target_table = "course_statistics" if self.minute_activity is False else "course_statistics_pre"
        filename = os.path.basename(path)
        if filename not in self.processed_files:
            error_buffer = {}
            for ind, chunk in enumerate(pd.read_csv(
                    path, chunksize=self.statistics_import_chunk_size, parse_dates=[date_field], **kwargs
            )):
                self.db.add_records(
                    self.preprocess_chunk(chunk, path, error_buffer, drop_duplicates=not self.minute_activity), target_table
                )
            for error_file, content in error_buffer.items():
                content.to_csv(error_file, index=False, sep="\t")
            self.processed_files.append(filename)
            self.has_new_data = True

    def entry_valid(self, profile_id, statistic_type_id, educational_course_id, created_at):
        profile_id = profile_id  # entry["profile_id"]
        course_id = educational_course_id  # entry["educational_course_id"]
        if isinstance(profile_id, float) and isnan(profile_id):
            return False
        elif isinstance(course_id, float) and isnan(course_id) or pd.isna(course_id):
            return False
        else:
            return True

    def validate_course(self, subject_name, course_type, provider):
        if subject_name is None and course_type is None and provider is None:
            return
        assert course_type == "ЦОМ"
        assert isinstance(provider, str)
        assert isinstance(subject_name, str)

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

    def get_filtration_rules(self):
        filtration_rules = "WHERE profile_approved_status.role = 'STUDENT' AND approved_status != 'NOT_APPROVED'"
        return filtration_rules

    def get_freeze_date_filtration_rule(self):
        if self.freeze_date is not None:
            return f"WHERE course_statistics.created_at < '{self.freeze_date}'"
        else:
            return ""

    def get_active_days_count_rule(self):
        if self.minute_activity:
            active_days_request = 'COUNT(CASE WHEN is_active = true THEN 1 ELSE NULL END)'
        else:
            active_days_request = 'COUNT(DISTINCT created_at)'
        return active_days_request

    def get_extra_column_names(self):
        if self.minute_activity:
            extra_columns = 'course_statistics.is_active as "is_active",'  # make sure has coma in the end
        else:
            extra_columns = ""
            return extra_columns

    def compute_active_days(self, minimum_active_minutes=10.0):
        seconds_in_minute = 60

        if self.has_new_data:
            self.db.drop_table("course_statistics")
            chunks = self.db.query(
                """
                SELECT 
                profile_id, educational_course_id, created_at, 
                min(created_at_original) as day_start, max(created_at_original) as day_end  
                FROM 
                course_statistics_pre
                GROUP BY profile_id, educational_course_id, created_at
                """, chunksize=self.statistics_import_chunk_size
            )

            for chunk in chunks:
                chunk["is_active"] = (
                        pd.to_datetime(chunk["day_end"]) - pd.to_datetime(chunk["day_start"])
                ).map(lambda x: x.seconds / seconds_in_minute > minimum_active_minutes)
                self.db.add_records(chunk, "course_statistics")

    def prepare_for_report(self):

        if self.has_new_data:
            for table_name in ["active_days_count", "full_report"]:
                self.db.drop_table(table_name)

            self.db.execute(
                f"""
                CREATE TABLE active_days_count AS
                SELECT
                educational_course_id, profile_id,
                {self.get_active_days_count_rule()} AS "active_days"
                FROM 
                course_statistics
                {self.get_freeze_date_filtration_rule()}
                GROUP BY educational_course_id, profile_id
                """
            )

            self.db.execute(
                f"""
                CREATE TABLE full_report AS
                SELECT
                course_titles.provider as "platform",
                course_titles.course_name as "course_name",
                profile_approved_status.profile_id as "profile_id",
                profile_approved_status.profile_id_uuid as "profile_id_uuid",
                profile_approved_status.approved_status as "approved_status",
                profile_approved_status.role as "role",
                active_days_count.active_days as "active_days",
                active_days_count.educational_course_id as "course_id",
                educational_institution.special_status as "special_status"
                FROM active_days_count
                LEFT JOIN profile_approved_status ON 
                active_days_count.profile_id = profile_approved_status.profile_id
                LEFT JOIN educational_institution ON 
                profile_approved_status.educational_institution_id = educational_institution.educational_institution_id
                INNER JOIN (
                    SELECT DISTINCT provider, course_name, course_id
                    FROM course_information
                ) as course_titles
                on active_days_count.educational_course_id = course_titles.course_id
                LEFT JOIN student_grades on active_days_count.profile_id = student_grades.profile_id
                {self.get_filtration_rules()}
                """
            )

        self.full_report = self.db.query(
            """
            SELECT * from full_report
            """
        )
        self.db.create_index_for_table(self.full_report, "full_report")

    def sort_course_names(self, report_df, order):
        for column in order:
            report_df.sort_values(
                by=column, key=lambda x: np.argsort(index_natsorted(report_df[column])), inplace=True
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

        if self.has_new_data:
            self.db.drop_table("active_data")

            self.db.execute(
                """
                CREATE TABLE active_data AS
                SELECT
                platform as "Активных дней",
                COUNT(DISTINCT CASE WHEN active_days >= 2 THEN profile_id ELSE NULL END) as "2 дня и более",
                COUNT(DISTINCT CASE WHEN active_days >= 3 THEN profile_id ELSE NULL END) as "3 дня и более",
                COUNT(DISTINCT CASE WHEN active_days >= 4 THEN profile_id ELSE NULL END) as "4 дня и более",
                COUNT(DISTINCT CASE WHEN active_days >= 5 THEN profile_id ELSE NULL END) as "5 дней и более",
                COUNT(profile_id) as "Всего пользоателей"
                FROM (
                    SELECT
                    platform, profile_id, max(active_days) as "active_days"
                    FROM full_report
                    GROUP BY platform, profile_id
                )
                GROUP BY platform
                """
            )

        active_data = self.db.query("SELECT * FROM active_data").set_index("Активных дней").T

        col_order = []

        for col in active_data.columns:
            active_data[f"{col}, %"] = active_data[col] / active_data.loc["Всего пользоателей", col] * 100.
            col_order.append(col)
            col_order.append(f"{col}, %")

        self._conv_stat = active_data[col_order]

    def prepare_report(self):

        if self.has_new_data:
            self.db.drop_table("user_report")
            self.db.drop_table("courses_report")

            self.db.execute(
                """
                CREATE TABLE user_report AS
                SELECT
                platform as "Платформа",
                COUNT(DISTINCT profile_id) as "Всего пользователей",
                COUNT(DISTINCT CASE WHEN active_days >= 5 THEN profile_id ELSE NULL END) as "Активных пользователей",
                COUNT(DISTINCT CASE WHEN approved_status = 'APPROVED' AND 
                            (
                                    special_status = 'Получено адресатом' OR special_status == 'Активировали в ноябре'
                            ) THEN profile_id ELSE NULL END) as "Подтверждённых пользователей использующих сервис",
                COUNT(DISTINCT CASE WHEN approved_status = 'APPROVED' AND active_days >= 5 AND 
                            (
                                    special_status = 'Получено адресатом' OR special_status == 'Активировали в ноябре'
                            ) THEN profile_id ELSE NULL END) as "Активных и подтверждённых пользователей"
                FROM
                full_report
                GROUP BY
                platform
                """
            )

            # this query has THEN 1 because one person can take one course only once
            # no need to do DISTINCT
            self.db.execute(
                """
                CREATE TABLE courses_report AS
                SELECT
                Платформа,
                Название, 
                Всего,
                "Активные Подтверждённые", 
                "Активные Всего",
                price as "Цена за одну лицензию",
                price * "Активные Подтверждённые" AS "Всего за курс"
                FROM (
                    SELECT
                    course_id,
                    platform as "Платформа",
                    course_name as "Название",
                    COUNT(DISTINCT profile_id) as "Всего",
                    COUNT(CASE WHEN active_days >=5 AND approved_status == 'APPROVED' AND 
                            (
                                special_status == 'Получено адресатом' OR special_status == 'Активировали в ноябре'
                            ) THEN 1 ELSE NULL END) AS "Активные Подтверждённые",
                    COUNT(CASE WHEN active_days >=5 THEN 1 ELSE NULL END) AS "Активные Всего"
                    FROM
                    full_report
                    GROUP BY
                    platform, course_name
                ) AS usage 
                LEFT JOIN billing_info on usage.course_id = billing_info.course_id
                """
            )

        self.user_report = self.db.query("SELECT * FROM user_report")
        self.courses_report = self.db.query("SELECT * FROM courses_report")

        self.sort_course_names(self.courses_report, order=["Название", "Платформа"])

    def get_report(self):
        self.prepare_for_report()
        self.prepare_report()
        self.convergence_stat()
        self.last_processed_export = self.last_export
        self.user_report = self.add_licence_info(self.user_report, self.courses_report)
        return self.courses_report, self.user_report, self._conv_stat

    def get_people_for_billing(self, num_days_to_be_considered_active=5):
        if self.has_new_data:
            self.db.drop_table("billing")
            self.db.drop_table("people_billing_report")

            self.db.execute(
                f"""
                CREATE TABLE billing AS
                SELECT platform, course_name, profile_id, profile_id_uuid, special_status
                FROM full_report
                WHERE approved_status = 'APPROVED' 
                AND special_status NOT NULL 
                AND active_days >= {num_days_to_be_considered_active}
                """
            )

            people_courses_for_billing = self.db.query("SELECT * FROM billing") \
                .drop("special_status", axis=1) \
                .drop("profile_id", axis=1) \
                .rename({"profile_id_uuid": "profile_id"}, axis=1)

            people_courses_filter = set(
                (pl, c, per) for pl, c, per in people_courses_for_billing.values
            )

            course_statistics = self.db.query(  # can improve filtration by adding course name to the filter
                """
                SELECT 
                DISTINCT provider, course_name, profile_id_uuid as "profile_id", created_at as "visit_date" 
                FROM 
                course_statistics
                LEFT JOIN course_information ON course_statistics.educational_course_id = course_information.course_id
                LEFT JOIN profile_approved_status ON course_statistics.profile_id = profile_approved_status.profile_id
                WHERE course_statistics.profile_id IN (
                    SELECT DISTINCT profile_id FROM billing
                ) 
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
                    record[f"День {ind + 1}"] = date.split(" ")[0]

                records.append(record)

            data = pd.DataFrame.from_records(records, columns=[
                "Наименование образовательной цифровой площадки",
                "Наименование ЦОК",
                "Идентификационный номер обучающегося",
            ] + [f"День {ind + 1}" for ind in range(num_days_to_be_considered_active)]).astype("string")
            if len(data) > 0:
                self.sort_course_names(data, ["Наименование ЦОК", "Наименование образовательной цифровой площадки"])

            self.db.add_records(data, "people_billing_report")

        data = self.db.query("SELECT * FROM people_billing_report")

        return data


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

    def validate_structure_id(self, id_, parent_id, structure):
        if id_ in structure:
            assert parent_id == structure[id_]["parent_id"]

    def map_course_statistics_columns(self, data):
        data.rename({
            "createdat": "created_at",
            "profileid": "profile_id",
            "statisticstypeid": "statistic_type_id",
            "externalid": "educational_course_id",
        }, axis=1, inplace=True)
        def normalize(id_):
            if pd.isna(id_):
                return id_
            else:
                return "/".join(id_.split("/")[:5])
        data.eval(
            "educational_course_id = educational_course_id.map(@normalize)",
            local_dict={"normalize": normalize},
            inplace=True
        )
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
        data.rename({"material_id": "educational_course_id"}, axis=1, inplace=True)
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

    def resolve_structure(self, data):
        return data

    def load_course_structure(self, path):
        if self.has_new_data:
            data = self.format_course_structure_columns(pd.read_csv(path, dtype={"material_id": "string"}))
            data = self.resolve_structure(data)
            data = self.prepare_course_ids(data)
            self.db.replace_records(
                data[[
                    "educational_course_id", "educational_course_id_uuid",
                    "course_name", "provider", "course_id"]],
                "course_information"
            )


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

        self.special_files = {"student_16.11.bz2", "educational_course_statistic_16.11.bz2",
                              "educational_course_type_16.11.bz2", "educational_courses_16.11.bz2",
                              "educational_courses_only_courses_16.11.bz2", "external_system_16.11.bz2", "last_export",
                              "profile_educational_institution_16.11.bz2", "profile_role_16.11.bz2", "role_16.11.bz2",
                              "school_students.bz2", "statistic_type_16.11.bz2", "educational_institution_16.11.bz2"}

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
                if len(data) == 0:
                    return 15
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
            if not filename.endswith(".bz2") or filename.startswith("___") or filename in self.special_files:
                continue

            data = pd.read_pickle(file_path, compression=None)
            if data.shape[0] >= max_len or data.shape[1] > max_width:
                logging.info(f"Skipping {filename}")
                continue
            data = data.dropna(how="all")

            self.add_sheet(self.normalize_worksheet_name(filename.strip(".bz2")), data)

    def get_report_name(self):
        return f"report_{self.last_export}"

    def save_report(self):
        export_file_name = self.get_report_name()

        self.write_xlsx(self.sheet_names, self.sheet_data, self.sheet_options, export_file_name)
        # self.write_html(self.sheet_names, self.sheet_data, self.sheet_options, export_file_name)
        self.write_index_html(export_file_name)


class CommonBillingReportWriter(ReportWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_report_name(self):
        return f"billing_report_{self.last_export}"


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

        save_location = save_location.absolute()

        curr_dir = os.getcwd()
        os.chdir(self.html_path)

        with ZipFile(
            region_report_folder + ".zip",
            mode='w'
        ) as zipper:
            for file in save_location.iterdir():
                if file.name.endswith(".xlsx"):
                    zipper.write(Path(region_report_folder).joinpath(file.name))

        rmtree(save_location)
        os.chdir(curr_dir)


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
        return f"billing_report_uchi_{self.last_export}"

    def write_index_html(self, name):
        pass


class SchoolActivityReportWriter(ReportWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_report_name(self):
        return f"active_and_approved_by_schools_{self.last_export}"

    def write_index_html(self, name):
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
<a id="region_arc" href="active_and_approved_by_schools_{self.last_export}.xlsx">Отчет по регионам</a>
</p>
</body>
</html>
"""
        with open(self.html_path.joinpath("index_regions.html"), "w") as index_html:
            index_html.write(html)


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
        course_structure, course_structure_foxford, course_structure_meo, course_types, course_statistics,
        course_statistics_foxford, course_statistics_uchi, course_statistics_meo, last_export, html_path,
        resources_path, region_info_path, freeze_date, educational_institution_path, minute_activity
):
    last_export = get_last_export(last_export)

    logging.info("Processing")

    provider_data = [
        Course_1C_ND(
            billing_info=billing, student_grades=student_grades, statistics_type=statistics_type,
            external_system=external_system, profile_educational_institution=profile_educational_institution,
            course_structure=course_structure, course_types=course_types, course_statistics=course_statistics,
            last_export=last_export, resources_path=resources_path, freeze_date=freeze_date,
            educational_institution=educational_institution_path, minute_activity=minute_activity
        ),
        Course_FoxFord(
            billing_info=billing, student_grades=student_grades, statistics_type=statistics_type,
            external_system=external_system, profile_educational_institution=profile_educational_institution,
            course_structure=course_structure_foxford, course_types=course_types,
            course_statistics=course_statistics_foxford, last_export=last_export, resources_path=resources_path,
            freeze_date=freeze_date, educational_institution=educational_institution_path, minute_activity=minute_activity
        ),
        Course_MEO(
            billing_info=billing, student_grades=student_grades, statistics_type=statistics_type,
            external_system=external_system, profile_educational_institution=profile_educational_institution,
            course_structure=course_structure_meo, course_types=course_types,
            course_statistics=course_statistics_meo, last_export=last_export, resources_path=resources_path,
            freeze_date=freeze_date, educational_institution=educational_institution_path, minute_activity=minute_activity
        ),
        Course_Uchi(
            billing_info=billing, student_grades=student_grades, statistics_type=statistics_type,
            external_system=external_system, profile_educational_institution=profile_educational_institution,
            course_structure=course_structure, course_types=course_types, course_statistics=course_statistics_uchi,
            last_export=last_export, resources_path=resources_path, freeze_date=freeze_date,
            educational_institution=educational_institution_path, minute_activity=minute_activity
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

        common_billing_report_writer = CommonBillingReportWriter(last_export, html_path, queries_path=Path(course_types).parent)
        common_billing_report_writer.add_sheet(
            "ЦОК",
            reports.billing_report.query("`Наименование образовательной цифровой площадки` != 'Учи.Ру'")
        )
        common_billing_report_writer.save_report()

        billing_report_writer = BillingReportWriter(last_export, html_path, queries_path=Path(course_types).parent)
        billing_report_writer.add_billing_info_as_sheets(
            reports.billing_report.query("`Наименование образовательной цифровой площадки` == 'Учи.Ру'")
        )
        billing_report_writer.save_report()

        region_report_writer = RegionReportWriter(last_export, html_path, queries_path=Path(course_types).parent)
        region_report_writer.add_region_info_as_sheets(reports.school_active_students_report)
        region_report_writer.save_report()

        school_report_writer = SchoolActivityReportWriter(last_export, html_path, queries_path=Path(course_types).parent)
        school_report_writer.add_sheet(
            "Активно и подтвержд. по школам", reports.school_active_students_report, {"long_column": 1}
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
    parser.add_argument("--educational_institution_path", default=None)
    parser.add_argument("--last_export", default=None)
    parser.add_argument("--html_path", default=None)
    parser.add_argument("--resources_path", default=None)
    parser.add_argument("--freeze_date", default=None, type=str)
    parser.add_argument("--minute_activity", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(module)s:%(lineno)d:%(message)s")

    process_statistics(**args.__dict__)

