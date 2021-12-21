import logging
from collections import namedtuple, defaultdict
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# from dsa.SharedModel import SharedModel
from natsort import index_natsorted

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


class Reporter:
    def __init__(
            self, args, shared_model, freeze_date=None
    ):
        self.statistics_import_chunk_size = 1000000
        self.freeze_date = freeze_date
        self.shared_model = shared_model
        self.args = args

        self.has_new_data = self.shared_model.needs_new_report()

        self.compute_active_days()


    @property
    def db(self):
        return self.shared_model.db

    def get_filtration_rules(self):
        filtration_rules = "WHERE approved_status != 'NOT_APPROVED'"
        # filtration_rules = "WHERE profile_approved_status.role = 'STUDENT' AND approved_status != 'NOT_APPROVED'"
        return filtration_rules

    def get_freeze_date_filtration_rule(self):
        if self.freeze_date is not None:
            return f"WHERE course_statistics.date < '{self.freeze_date}'"
        else:
            return ""

    def compute_active_days(self, minimum_active_minutes=10.0):
        seconds_in_minute = 60

        if self.has_new_data:
            logging.info("Computing active days")
            self.db.drop_table("course_statistics")
            self.db.execute(
                """
                CREATE TABLE course_statistics AS
                SELECT
                profile_id, educational_course_id, date, 
                sum(dt) as active_time,
                sum(dt) >= 600 as is_active
                FROM 
                course_statistics_pre
                GROUP BY
                profile_id, educational_course_id, date
                """,
            )

    def prepare_for_report(self):

        if self.has_new_data:
            logging.info("Computing full report")
            for table_name in ["active_days_count", "full_report"]:
                self.db.drop_table(table_name)

            self.db.execute(
                f"""
                CREATE TABLE active_days_count AS
                SELECT
                educational_course_id, profile_id,
                CAST(COUNT(CASE WHEN is_active = true THEN 1 ELSE NULL END) AS INTEGER) AS "active_days"
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
        licences_esia = []
        sum_total = []
        sum_total_esia = []
        sum_total_approved = []
        sum_total_esia_approved = []
        for provider in user_report["Платформа"]:
            table = courses_report.query(f"Платформа == '{provider}'")
            licences.append(table["Активные Подтверждённые"].sum())
            licences_esia.append(table["Активные Подтверждённые (ЕСИА)"].sum())
            sum_total.append(table["Всего за курс"].sum())
            sum_total_esia.append(table["Всего за курс (ЕСИА)"].sum())
            sum_total_approved.append(table["Всего за курс (Соответствует)"].sum())
            sum_total_esia_approved.append(table["Всего за курс (ЕСИА) (Соответствует)"].sum())

        user_report["Общее количество лицензий на оплату"] = licences
        user_report["Общее количество лицензий на оплату (ЕСИА)"] = licences_esia
        user_report["Общая сумма на оплату"] = sum_total
        user_report["Общая сумма на оплату (ЕСИА)"] = sum_total_esia
        user_report["Общая сумма на оплату (Соответствует)"] = sum_total_approved
        user_report["Общая сумма на оплату (ЕСИА) (Соответствует)"] = sum_total_esia_approved

        # empty = {key: "" for key in user_report.columns}
        # total = {key: user_report[key].sum() for key in user_report.columns if key != "Платформа"}
        # total["Платформа"] = "Итого"

        # user_report = user_report.append(pd.DataFrame.from_records([empty, total]))
        return user_report

    def convergence_stat(self):

        if self.has_new_data:
            logging.info("Computing convergence report")
            self.db.drop_table("active_data")

            self.db.execute(
                """
                CREATE TABLE active_data AS
                SELECT
                platform as "Активных дней",
                CAST(COUNT(DISTINCT CASE WHEN active_days >= 2 THEN profile_id ELSE NULL END) AS INTEGER) as "2 дня и более",
                CAST(COUNT(DISTINCT CASE WHEN active_days >= 3 THEN profile_id ELSE NULL END) AS INTEGER) as "3 дня и более",
                CAST(COUNT(DISTINCT CASE WHEN active_days >= 4 THEN profile_id ELSE NULL END) AS INTEGER) as "4 дня и более",
                CAST(COUNT(DISTINCT CASE WHEN active_days >= 5 THEN profile_id ELSE NULL END) AS INTEGER) as "5 дней и более",
                CAST(COUNT(profile_id) AS INTEGER) as "Всего пользоателей"
                FROM (
                    SELECT
                    platform, profile_id, max(active_days) as "active_days"
                    FROM full_report
                    WHERE role = 'STUDENT'
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
            logging.info("Computing user and courses report")
            self.db.drop_table("user_report")
            self.db.drop_table("courses_report")

            self.db.execute(
                """
                CREATE TABLE user_report AS
                SELECT
                platform as "Платформа",
                CAST(COUNT(DISTINCT profile_id) AS INTEGER) as "Всего пользователей",
                CAST(COUNT(DISTINCT CASE WHEN active_days >= 5 THEN profile_id ELSE NULL END) AS INTEGER) as "Активных пользователей",
                CAST(COUNT(DISTINCT CASE WHEN approved_status = 'APPROVED' AND 
                            (
                                    special_status = 'Получено адресатом' OR special_status == 'Активировали в ноябре'
                            ) THEN profile_id ELSE NULL END) AS INTEGER) as "Подтверждённых пользователей использующих сервис",
                CAST(COUNT(DISTINCT CASE WHEN approved_status = 'APPROVED' AND active_days >= 5 AND 
                            (
                                    special_status = 'Получено адресатом' OR special_status == 'Активировали в ноябре'
                            ) THEN profile_id ELSE NULL END) AS INTEGER) as "Активных и подтверждённых пользователей",
                CAST(COUNT(DISTINCT CASE WHEN approved_status = 'APPROVED' AND 
                            (
                                    special_status == 'Активировали в ноябре'
                            ) THEN profile_id ELSE NULL END) AS INTEGER) as "Подтверждённых пользователей использующих сервис (ЕСИА)",
                CAST(COUNT(DISTINCT CASE WHEN approved_status = 'APPROVED' AND active_days >= 5 AND 
                            (
                                    special_status == 'Активировали в ноябре'
                            ) THEN profile_id ELSE NULL END) AS INTEGER) as "Активных и подтверждённых пользователей (ЕСИА)"
                FROM
                full_report
                WHERE role = 'STUDENT'
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
                "Активные Подтверждённые (ЕСИА)",
                "Активные Всего",
                billing_info.approved as "Соответствует требованиям",
                price as "Цена за одну лицензию",
                price * "Активные Подтверждённые" AS "Всего за курс",
                price * "Активные Подтверждённые (ЕСИА)" AS "Всего за курс (ЕСИА)",
                price * "Активные Подтверждённые" * billing_info.approved AS "Всего за курс (Соответствует)",
                price * "Активные Подтверждённые (ЕСИА)" * billing_info.approved AS "Всего за курс (ЕСИА) (Соответствует)"
                FROM (
                    SELECT
                    course_id,
                    platform as "Платформа",
                    course_name as "Название",
                    CAST(COUNT(DISTINCT profile_id) AS INTEGER) as "Всего",
                    CAST(COUNT(DISTINCT CASE WHEN active_days >=5 AND approved_status == 'APPROVED' AND 
                            (
                                special_status == 'Получено адресатом' OR special_status == 'Активировали в ноябре'
                            ) THEN profile_id ELSE NULL END) AS INTEGER) AS "Активные Подтверждённые",
                    CAST(COUNT(DISTINCT CASE WHEN active_days >=5 AND approved_status == 'APPROVED' AND 
                            (
                                special_status == 'Активировали в ноябре'
                            ) THEN profile_id ELSE NULL END) AS INTEGER) AS "Активные Подтверждённые (ЕСИА)",
                    CAST(COUNT(DISTINCT CASE WHEN active_days >=5 THEN profile_id ELSE NULL END) AS INTEGER) AS "Активные Всего"
                    FROM
                    full_report
                    WHERE role = 'STUDENT'
                    GROUP BY
                    platform, course_name
                ) AS usage 
                LEFT JOIN billing_info on usage.course_id = billing_info.course_id
                """
            )

        self.user_report = self.db.query("SELECT * FROM user_report")
        self.courses_report = self.db.query("SELECT * FROM courses_report")

        self.sort_course_names(self.courses_report, order=["Название", "Платформа"])

    def compute_region_info(self):
        if self.has_new_data:
            logging.info("Computing region report")
            region_info = self.shared_model.read_table_dump(self.args.region_info, dtype={"ИНН": "string"})
            self.db.replace_records(region_info, "region_info")

            self.db.drop_table("region_info_activity")

            self.db.execute(
                """
                CREATE TABLE region_info_activity AS
                SELECT 
                Регион, Школа, ИНН, Адрес,
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' THEN profile_id ELSE NULL END) AS "Всего учеников",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND active_days = 1 THEN profile_id ELSE NULL END) AS "Воспользовались 1 день",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND active_days = 2 THEN profile_id ELSE NULL END) AS "Воспользовались 2 дня",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND active_days = 3 THEN profile_id ELSE NULL END) AS "Воспользовались 3 дня",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND active_days = 4 THEN profile_id ELSE NULL END) AS "Воспользовались 4 дня",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND active_days >= 5 THEN profile_id ELSE NULL END) AS "Воспользовались 5 дней и более (лицензия на год при подтверждении уч.з.)",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND approved_status = 'APPROVED' THEN profile_id ELSE NULL END) AS "Всего подтверждённых учеников",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND approved_status = 'NOT_APPROVED' AND active_days >= 5 THEN profile_id ELSE NULL END) AS "Активных и отклонённых",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND approved_status = 'APPROVED' AND active_days >= 5 THEN profile_id ELSE NULL END) AS "Активных и подтрверждённых",
                COUNT(DISTINCT CASE WHEN role = 'TEACHER' THEN profile_id ELSE NULL END) AS "Всего преподавателей",
                COUNT(DISTINCT CASE WHEN role = 'TEACHER' AND approved_status = 'APPROVED' THEN profile_id ELSE NULL END) AS "Подтверждённых преподавателей",
                COUNT(DISTINCT CASE WHEN role = 'TEACHER' AND approved_status = 'NOT_APPROVED' THEN profile_id ELSE NULL END) AS "Отклонённых преподавателей"
                FROM
                region_info
                LEFT JOIN (
                    SELECT
                    profile_id_uuid, max(active_days) as active_days
                    FROM
                    full_report
                    GROUP BY profile_id
                ) as active_people
                ON active_people.profile_id_uuid = region_info.profile_id
                GROUP BY Регион, Школа, ИНН, Адрес
                ORDER BY "Всего подтверждённых учеников" DESC
                """
            )

        self.schools_activity = self.db.query("select * from region_info_activity")

    def enrich_user_report(self, user_report):
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

    def get_reports(self):
        # self.has_new_data = True
        self.prepare_for_report()
        self.prepare_report()
        self.convergence_stat()
        self.get_people_for_billing()
        self.compute_region_info()

        self.shared_model.set_latest_report()
        self.has_new_data = False

        self.user_report = self.add_licence_info(self.user_report, self.courses_report)
        return Reports(
            self.enrich_user_report(self.user_report),
            self.courses_report,
            self._conv_stat.reset_index().rename({"index": "Активных дней"}, axis=1),
            self.schools_activity,
            self.schools_activity.groupby('Регион').sum().reset_index(),
            self.billing
        )

    def get_people_for_billing(self, num_days_to_be_considered_active=5):
        if self.has_new_data:
            logging.info("Computing people for billing")
            self.db.drop_table("billing")
            self.db.drop_table("people_billing_report")

            self.db.execute(
                f"""
                CREATE TABLE billing AS
                SELECT platform, course_name, profile_id, profile_id_uuid, special_status
                FROM full_report
                WHERE approved_status = 'APPROVED' and special_status = 'Активировали в ноябре' and role = 'STUDENT'
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
                DISTINCT provider, course_name, profile_approved_status.profile_id_uuid as "profile_id", date as "visit_date",
                active_time as "active_time", grade, special_status
                FROM 
                course_statistics
                LEFT JOIN course_information ON course_statistics.educational_course_id = course_information.course_id
                LEFT JOIN profile_approved_status ON course_statistics.profile_id = profile_approved_status.profile_id
                LEFT JOIN student_grades on course_statistics.profile_id = student_grades.profile_id
                LEFT JOIN educational_institution ON profile_approved_status.educational_institution_id = educational_institution.educational_institution_id
                WHERE course_statistics.profile_id IN (
                    SELECT DISTINCT profile_id FROM billing
                ) 
                ORDER BY date
                """,
                chunksize=self.statistics_import_chunk_size
            )

            people_courses_visits = defaultdict(list)
            grades = {}
            for chunk in course_statistics:
                for provider, course_name, profile_id, visit_date, active_time, grade, special_status in chunk[
                    ["provider", "course_name", "profile_id", "visit_date", "active_time", "grade", "special_status"]
                ].values:
                    key = (provider, course_name, profile_id)
                    grades[profile_id] = grade
                    if key not in people_courses_filter:
                        continue
                    if len(people_courses_visits[key]) == num_days_to_be_considered_active:
                        continue
                    if active_time >= 600:
                        assert special_status == "Активировали в ноябре"
                        people_courses_visits[key].append((visit_date, active_time))

            records = []
            for key, dates in people_courses_visits.items():
                assert len(dates) == num_days_to_be_considered_active
                platform, course_name, profile_id = key
                for ind, (date, duration) in enumerate(dates):
                    record = {
                        "Наименование образовательной цифровой площадки": platform,
                        "Наименование ЦОК": course_name,
                        "Идентификационный номер обучающегося": profile_id,
                        # "Класс": grades[profile_id],
                        "Дата использования курса": date.split(" ")[0],
                        "Длительность использования курса, ч:м:с": str(timedelta(seconds=int(duration)))
                    }
                    records.append(record)

            data = pd.DataFrame.from_records(records, columns=[
                "Наименование образовательной цифровой площадки", "Наименование ЦОК",
                "Идентификационный номер обучающегося", "Дата использования курса",
                "Длительность использования курса, ч:м:с"
            ]).astype("string")
            if len(data) > 0:
                self.sort_course_names(data, ["Наименование ЦОК", "Наименование образовательной цифровой площадки"])

            self.db.add_records(
                data, "people_billing_report",
                dtype={

                }
            )

        self.billing = self.db.query("SELECT * FROM people_billing_report")

