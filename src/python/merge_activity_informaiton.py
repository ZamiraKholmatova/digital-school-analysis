import pandas as pd
from tqdm import tqdm


def merge_activity_and_regions(dbs, region_info_path):

    # dbs = [
    #     db_1c_nd, db_foxford, db_meo, db_uchi
    # ]

    tables = []

    for db in dbs:
        tables.append(db.query(
            """
            SELECT
            profile_id, active_days
            FROM 
            full_report
            """
        ))

    full_activity = pd.concat(tables, axis=0)
    activity = full_activity.groupby(["profile_id"]).max()

    region_info = pd.read_csv(region_info_path, dtype={"ИНН": "string"})
    merged = region_info.merge(activity, left_on="profile_id", right_on="profile_id", how="left")

    # records = []
    records = {}
    for region, school, inn, address, profile_id, approved_status, role, active_days in tqdm(
            merged[
                ["Регион", "Школа", "ИНН", "Адрес", "profile_id", "approved_status", "role", "active_days"]
            ].values,
            leave=False
    ):
        key = (region, school, inn)
        if key not in records:
            records[key] = {
                "Регион": region,
                "Школа": school,
                "ИНН": inn,
                "Адрес": address,
                "Всего учеников": 0,
                "Всего подтверждённых учеников": 0,
                "Активных учеников": 0,
                "Активных и отклонённых": 0,
                "Активных и подтрверждённых": 0,
                "Всего преподавателей": 0,
                "Подтверждённых преподавателей": 0,
                "Отклонённых преподавателей": 0
            }

        current_record = records[key]

        if role == "TEACHER":
            current_record["Всего преподавателей"] += 1
            if approved_status == "APPROVED":
                current_record["Подтверждённых преподавателей"] += 1
            elif approved_status == "NOT_APPROVED":
                current_record["Отклонённых преподавателей"] += 1
        elif role == "STUDENT":
            current_record["Всего учеников"] += 1
            if approved_status == "APPROVED":
                current_record["Всего подтверждённых учеников"] += 1
            if active_days >= 5:
                current_record["Активных учеников"] += 1
                if approved_status == "APPROVED":
                    current_record["Активных и подтрверждённых"] += 1
                elif approved_status == "NOT_APPROVED":
                    current_record["Активных и отклонённых"] += 1

    return pd.DataFrame.from_records(list(records.values()))

    # for (region, school, inn), group in tqdm(merged.groupby(["Регион","Школа","ИНН"]), total=len(merged[["Регион","Школа","ИНН"]].drop_duplicates())):
    #     students = group.query("role == 'STUDENT'")
    #     active_students = students.query("active_days >= 5")
    #     teachers = group.query("role == 'TEACHER'")
    #     records.append({
    #         "Регион": region,
    #         "Школа": school,
    #         "ИНН": inn,
    #         "Всего учеников": len(students),
    #         "Всего подтверждённых": len(students.query("approved_status == 'APPROVED'")),
    #         "Активных учеников": len(active_students),
    #         "Активных и отклонённых": len(active_students.query("approved_status == 'NOT_APPROVED'")),
    #         "Активных и подтрверждённых": len(active_students.query("approved_status == 'APPROVED'")),
    #         "Всего учителей": len(teachers),
    #         "Подтверждённых учителей": len(teachers.query("approved_status == 'APPROVED'")),
    #         "Отклонённых учителей": len(teachers.query("approved_status == 'NOT_APPROVED'"))
    #     })
    #
    # return pd.DataFrame.from_records(records)


if __name__ == "__main__":
    from calculating_costs2 import SQLTable
    db_1c_nd = SQLTable("/Users/LTV/Documents/school/code/resources/Course_1C_ND.db")
    db_foxford = SQLTable("/Users/LTV/Documents/school/code/resources/Course_FoxFord.db")
    db_meo = SQLTable("/Users/LTV/Documents/school/code/resources/Course_MEO.db")
    db_uchi = SQLTable("/Users/LTV/Documents/school/code/resources/Course_Uchi.db")
    region_info_path = "/Users/LTV/Documents/school/code/school_students.csv"

    merged = merge_activity_and_regions([db_1c_nd, db_foxford, db_meo, db_uchi], region_info_path)

    merged.to_csv("active_and_approved_by_region.csv", index=False)


# scp -P2221 -i credentials/id_rsa root@188.130.155.194:/root/export/school_students.csv .
# docker cp db:/tmp/export_34625/school_students.csv /root/export/school_students.csv