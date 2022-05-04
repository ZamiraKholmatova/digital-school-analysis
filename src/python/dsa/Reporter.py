import logging
from collections import namedtuple, defaultdict
from datetime import date

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
            self, args, shared_model, freeze_date=None, start_date=None, current_month=None
    ):
        self.statistics_import_chunk_size = 1000000
        self.freeze_date = freeze_date
        self.start_date = start_date
        self.shared_model = shared_model
        self.args = args

        self.has_new_data = self.shared_model.needs_new_report()
        self.check_if_payed = self.shared_model.already_payed
        # self.compute_active_days()
        self.licence_threshold = 3
        self.current_month = date.today().replace(day=1).strftime("%Y-%m-%d %H:%M:%S") #'2022-04-01 00:00:00'


    @property
    def db(self):
        return self.shared_model.db

    def get_filtration_rules(self):
        filtration_rules = "WHERE approved_status != 'NOT_APPROVED'"
        # filtration_rules = "WHERE profile_approved_status.role = 'STUDENT' AND approved_status != 'NOT_APPROVED'"
        return filtration_rules

    def get_date_filtration_rule(self):
        rules = []
        if self.freeze_date is not None:
            rules.append(f"course_statistics.date < '{self.freeze_date}'")
        if self.freeze_date is not None:
            rules.append(f"course_statistics.date >= '{self.start_date}'")
        if len(rules) > 1:
            rule_str = " AND ".join(rules)
            return f"WHERE {rule_str}"
        else:
            if len(rules) == 1:
                rule_str = rules[0]
                return f"WHERE {rule_str}"
            else:
                return ""

    # def compute_active_days(self, minimum_active_minutes=10.0):
    #     seconds_in_minute = 60
    #
    #     if self.has_new_data:
    #         logging.info("Computing active days")
    #         self.db.drop_table("course_statistics")
    #         self.db.execute(
    #             """
    #             CREATE TABLE course_statistics AS
    #             SELECT
    #             profile_id, educational_course_id, date,
    #             sum(dt) as active_time,
    #             sum(dt) >= 600 as is_active
    #             FROM
    #             course_statistics_pre
    #             GROUP BY
    #             profile_id, educational_course_id, date
    #             """,
    #         )

    def course_statistics_union(self):
        return """
        SELECT * from course_statistics_unified 
        UNION ALL SELECT * from course_statistics_foxford
        UNION ALL SELECT * from course_statistics_meo
        UNION ALL SELECT * from course_statistics_uchi
        """

    def prepare_for_report(self):

        if self.has_new_data:
            logging.info("Computing full report")
            for table_name in ["full_report", "active_days_count"]:
                self.db.drop_table(table_name)

            self.db.execute(
                f"""
                CREATE TABLE active_days_count AS
                SELECT
                educational_course_id, profile_id, created_at as "month_start",
                CAST(COUNT(created_at) AS INTEGER) AS "active_days"
                FROM
                course_statistics
                {self.get_date_filtration_rule()}
                GROUP BY educational_course_id, profile_id, created_at
                """
            )

            self.db.execute(
                f"""
                CREATE TABLE full_report AS
                SELECT
                course_titles.provider as "platform",
                course_titles.course_name as "course_name",
                active_days_count.month_start as "month_start",
                profile_approved_status.profile_id as "profile_id",
                profile_approved_status.profile_id_uuid as "profile_id_uuid",
                profile_approved_status.approved_status as "approved_status",
                profile_approved_status.role as "role",
                active_days_count.active_days as "active_days",
                active_days_count.educational_course_id as "course_id"
--                 educational_institution.special_status as "special_status"
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
                WHERE (profile_approved_status.educational_institution_id != 'f04e94ca-f99f-4a77-af0a-a07094ccbcea' OR profile_approved_status.educational_institution_id != 'b345f7f7-bd59-42b7-80b9-a57613bd2924')
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
        for provider, month_start in zip(user_report["Платформа"], user_report["Начало месяца"]):
            table = courses_report.query(f"Платформа == '{provider}' and `Начало месяца` == @month_start", local_dict={"month_start": month_start})
            licences.append(table["Активные и подтвержденные"].sum())
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
            logging.info("Computing convergence report")
            self.db.drop_table("active_data")

            self.db.execute(
                """
                CREATE TABLE active_data AS
                SELECT
                platform as "Активных дней", month_start as "Месяц",
                CAST(COUNT(DISTINCT CASE WHEN active_days >= 1 THEN profile_id ELSE NULL END) AS INTEGER) as "1 день и более",
                CAST(COUNT(DISTINCT CASE WHEN active_days >= 2 THEN profile_id ELSE NULL END) AS INTEGER) as "2 дня и более",
                CAST(COUNT(DISTINCT CASE WHEN active_days >= 3 THEN profile_id ELSE NULL END) AS INTEGER) as "3 дня и более"
                FROM (
                    SELECT
                    platform, profile_id, month_start, max(active_days) as "active_days"
                    FROM full_report
                    WHERE ((role = 'TEACHER' AND platform = '1С:Урок') OR (role = 'STUDENT' AND platform != '1С:Урок'))     --role = 'STUDENT'
                    GROUP BY platform, profile_id, month_start
                )
                GROUP BY platform, month_start
                """
            )
                        # CAST(COUNT(profile_id) AS INTEGER) as "Всего пользоателей"

        active_data = self.db.query("SELECT * FROM active_data").set_index("Активных дней").T

        col_order = []

        for col in active_data.columns:
            active_data[f"{col}, %"] = None # active_data[col] / active_data.loc["Всего пользоателей", col] * 100.
            col_order.append(col)
            col_order.append(f"{col}, %")

        self._conv_stat = active_data[col_order]

    def prepare_report(self):

        if self.has_new_data:
            logging.info("Computing user and courses report")
            self.db.drop_table("user_report")
            self.db.drop_table("courses_report")

            self.db.execute(
                f"""
                CREATE TABLE user_report AS
                SELECT
                platform as "Платформа",
                month_start as "Начало месяца",
                CAST(COUNT(DISTINCT profile_id) AS INTEGER) as "Всего пользователей",
                CAST(COUNT(DISTINCT CASE WHEN active_days >= {self.licence_threshold} THEN profile_id ELSE NULL END) AS INTEGER) as "Активных пользователей"
                FROM
                full_report
                WHERE ((role = 'TEACHER' AND platform = '1С:Урок') OR (role = 'STUDENT' AND platform != '1С:Урок'))           --role = 'STUDENT'
                GROUP BY
                platform, month_start
                """
            )


            # this query has THEN 1 because one person can take one course only once
            # no need to do DISTINCT
            self.db.execute(
                f"""
                CREATE TABLE courses_report AS
                SELECT
                Платформа,
                Название,
                "Начало месяца", 
                Всего,
                "Активные и подтвержденные",
                price as "Цена за одну лицензию",
                price * "Активные и подтвержденные" AS "Всего за курс"
                FROM (
                    SELECT
                    course_id,
                    platform as "Платформа",
                    course_name as "Название",
                    month_start as "Начало месяца",
                    CAST(COUNT(DISTINCT profile_id) AS INTEGER) as "Всего",
                    CAST(COUNT(DISTINCT CASE WHEN active_days >= {self.licence_threshold} THEN profile_id ELSE NULL END) AS INTEGER) AS "Активные и подтвержденные"
                    FROM
                    full_report
                    WHERE approved_status='APPROVED' AND ((role = 'TEACHER' AND platform = '1С:Урок') OR (role = 'STUDENT' AND platform != '1С:Урок'))  -------role="STUDENT"
                    GROUP BY
                    platform, course_name, month_start
                ) AS usage 
                LEFT JOIN billing_info on usage.course_id = billing_info.course_id
                """
            )

        self.user_report = self.db.query("SELECT * FROM user_report")
        self.courses_report = self.db.query("SELECT * FROM courses_report")

        self.sort_course_names(self.courses_report, order=["Начало месяца", "Название", "Платформа"])

    # def compute_current_report(self):
    #     if self.has_new_data:
    #         logging.info("Computing current report")
    #         self.db.drop_table("current_report")
    #
    #         self.db.execute(
    #             f"""
    #             CREATE TABLE current_report AS
    #             SELECT
    #             platform,
    #             profile_id_uiid,
    #             role,
    #             active_days,
    #             month_start as "Начало месяца"
    #             from full_report
    #             CAST(COUNT(DISTINCT profile_id) AS INTEGER) as "Всего пользователей",
    #             CAST(COUNT(DISTINCT CASE WHEN active_days >= {self.licence_threshold} THEN profile_id ELSE NULL END) AS INTEGER) as "Активных пользователей"
    #             FROM
    #             full_report
    #             WHERE role = 'STUDENT'
    #             GROUP BY
    #             platform, month_start
    #             """
    #         )

    def compute_region_info(self):
        if self.has_new_data:
            logging.info("Computing region report")
            region_info = self.shared_model.read_table_dump(self.args.region_info, dtype={"ИНН": "string"})
            self.db.replace_records(region_info, "region_info")

            self.db.drop_table("region_info_activity")

            self.db.execute(
                f"""
                CREATE TABLE region_info_activity AS
                SELECT
                Регион, Школа, ИНН, Адрес,
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' THEN profile_id ELSE NULL END) AS "Всего учеников",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND active_days = 1 THEN profile_id ELSE NULL END) AS "Воспользовались 1 день",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND active_days = 2 THEN profile_id ELSE NULL END) AS "Воспользовались 2 дня",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND active_days >= 3 THEN profile_id ELSE NULL END) AS "Воспользовались 3 дня и более",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND approved_status = 'APPROVED' THEN profile_id ELSE NULL END) AS "Всего подтверждённых учеников",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND approved_status = 'NOT_APPROVED' AND active_days >= {self.licence_threshold} THEN profile_id ELSE NULL END) AS "Активных и отклонённых",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND approved_status = 'NONE' AND active_days >= {self.licence_threshold} THEN profile_id ELSE NULL END) AS "Активных и неподтвержденных",
                COUNT(DISTINCT CASE WHEN role = 'STUDENT' AND approved_status = 'APPROVED' AND active_days >= {self.licence_threshold} THEN profile_id ELSE NULL END) AS "Активных и подтрверждённых",
                COUNT(DISTINCT CASE WHEN role = 'TEACHER' THEN profile_id ELSE NULL END) AS "Всего преподавателей",
                COUNT(DISTINCT CASE WHEN role = 'TEACHER' AND approved_status = 'APPROVED' THEN profile_id ELSE NULL END) AS "Подтверждённых преподавателей",
                COUNT(DISTINCT CASE WHEN role = 'TEACHER' AND approved_status = 'NONE' THEN profile_id ELSE NULL END) AS "Неподтверждённых преподавателей",
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

        t = []
        extras = []

        for month in sorted(user_report["Начало месяца"].unique()):
            for_compute = user_report.query("`Начало месяца` == @month_start", local_dict={"month_start": month})

            total = {key: for_compute[key].sum() for key in for_compute.columns if key != "Платформа" and key != "Начало месяца"}
            # extras += [
            #     {
            #         "Платформа": f"Цена за лицензию {month}",
            #         "Всего пользователей": total["Общая сумма на оплату"] / total["Общее количество лицензий на оплату"]
            #     }, {
            #         "Платформа": f"Лицензий на человека {month}",
            #         "Всего пользователей": total["Общее количество лицензий на оплату"] / total[
            #             "Активных пользователей"]
            #     }
            # ]
            total["Платформа"] = f"Итого за месяц"
            total["Начало месяца"] = str(month)
            t.append(total)
        user_report = user_report.append(pd.DataFrame.from_records([empty] + t + [empty] + extras))
        return user_report

    def get_reports(self):
        # self.has_new_data = True
        self.prepare_for_report()
        self.prepare_report()
        self.convergence_stat()
        self.get_people_for_billing()
        self.compute_region_info()

        # self.shared_model.set_latest_report()
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

    def get_people_for_billing(self):
        if self.has_new_data:
            logging.info("Computing people for billing")
            self.db.drop_table("billing")
            self.db.drop_table("people_billing_report")

            self.db.execute(
                f"""
                CREATE TABLE billing AS
                SELECT platform, course_name, month_start, profile_id, profile_id_uuid
                FROM full_report
                WHERE ((role = 'TEACHER' AND platform = '1С:Урок') OR (role = 'STUDENT' AND platform != '1С:Урок'))
                AND active_days >= {self.licence_threshold} AND month_start = '{self.current_month}'
                """
            )

            people_courses_for_billing = self.db.query("SELECT * FROM billing") \
                .drop("profile_id", axis=1) \
                .rename({"profile_id_uuid": "profile_id"}, axis=1)

            list_of_already_payed = set()  ################ADDED HERE
            if self.check_if_payed is not None:  ################ADDED HERE
                list_of_already_payed = set(self.check_if_payed.copy())  ################ADDED HERE

            people_courses_filter = set(
                (pl, c, m, per) for pl, c, m, per in people_courses_for_billing.values if (str(per), str(c), str(pl)) not in list_of_already_payed
            )
            # people_courses_filter_df = pd.DataFrame.from_records(list(people_courses_filter))
            # people_courses_filter_df.to_csv('проверка_связи.csv')

            course_statistics = self.db.query(  # can improve filtration by adding course name to the filter
                f"""
                SELECT
                DISTINCT provider, course_information.course_name, course_statistics.created_at as "month_start", profile_approved_status.profile_id_uuid as "profile_id", date as "visit_date",
                grade
                FROM
                course_statistics
--                 LEFT JOIN active_days_count ON active_days_count.profile_id = course_statistics.profile_id
                LEFT JOIN course_information ON course_statistics.educational_course_id = course_information.course_id
                LEFT JOIN profile_approved_status ON course_statistics.profile_id = profile_approved_status.profile_id
                LEFT JOIN student_grades on course_statistics.profile_id = student_grades.profile_id
                LEFT JOIN educational_institution ON profile_approved_status.educational_institution_id = educational_institution.educational_institution_id
                WHERE (course_statistics.profile_id, provider, course_information.course_name, course_statistics.created_at) IN (
                    SELECT DISTINCT profile_id, platform, course_name,month_start FROM billing
                )
                ORDER BY date
                """,
                chunksize=self.statistics_import_chunk_size
            )

            people_courses_visits = defaultdict(list)
            grades = {}
            for chunk in course_statistics:
                for provider, course_name, month_start, profile_id, visit_date, grade in chunk[
                    ["provider", "course_name", "month_start", "profile_id", "visit_date", "grade"]
                ].values:
                    key = (provider, course_name, month_start, profile_id)
                    grades[profile_id] = grade
                    if key not in people_courses_filter:
                        continue
                    if len(people_courses_visits[key]) == self.licence_threshold:
                        continue
                    people_courses_visits[key].append(visit_date)

            records = []
            for key, dates in people_courses_visits.items():
                assert len(dates) == self.licence_threshold
                platform, course_name, month_start, profile_id = key
                # if (profile_id, course_name, platform) not in list_of_already_payed:  ################ADDED HERE
                for ind, date in enumerate(dates):
                    record = {
                        "Наименование образовательной цифровой площадки": platform,
                        "Наименование ЦОК": course_name,
                        "Месяц": month_start,
                        "Идентификационный номер обучающегося": profile_id,
                        # "Класс": grades[profile_id],
                        "Дата использования курса": date.split(" ")[0]
                    }
                    records.append(record)

            data = pd.DataFrame.from_records(records, columns=[
                "Наименование образовательной цифровой площадки", "Наименование ЦОК",
                "Месяц", "Идентификационный номер обучающегося", "Дата использования курса"
            ]).astype("string")

            if len(data) > 0:
                self.sort_course_names(data, ["Наименование ЦОК", "Наименование образовательной цифровой площадки"])

            self.db.add_records(
                data, "people_billing_report",
                dtype={

                }
            )

        self.billing = self.db.query("SELECT * FROM people_billing_report")

