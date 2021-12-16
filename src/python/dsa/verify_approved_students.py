import pandas as pd

educational_institutions = pd.read_csv("/Users/LTV/Documents/school/code/data/db_data/educational_institution_16.11.csv", dtype={"inn": "string"})
approved_status = pd.read_excel("/Users/LTV/Documents/school/code/data/db_data/статистика_по_школам_за_7_декабря_2.xlsx", dtype={"ИНН": "string"})
profile_educational_institution = pd.read_csv("/Users/LTV/Documents/school/code/data/db_data/profile_educational_institution_16.11.csv")

merged = educational_institutions.merge(approved_status, how="left", left_on="inn", right_on="ИНН").drop("ИНН",  axis=1)
merged = merged[["id", "letter_received_status"]]

pei = profile_educational_institution.merge(merged, left_on="educational_institution_id", right_on="id")[
    ["profile_id", "letter_received_status", "updated_at"]
]


billing_report = pd.read_excel("/Users/LTV/Documents/school/code/html/billing_report_2021-12-01_07-02-24.xlsx")
billing_report_uchi = pd.read_excel("/Users/LTV/Documents/school/code/html/billing_report_uchi_2021-12-01_07-02-24.xlsx", sheet_name=None)

already_active = billing_report[['Наименование образовательной цифровой площадки', 'Наименование ЦОК',
       'Идентификационный номер обучающегося']]

for sheet_name, sheet  in billing_report_uchi.items():
    if sheet_name == "Индекс курсов":
        continue
    already_active = already_active.append(sheet[['Наименование образовательной цифровой площадки', 'Наименование ЦОК',
       'Идентификационный номер обучающегося']])

active_with_info = already_active.merge(pei, how="left", left_on="Идентификационный номер обучающегося", right_on="profile_id")
active_with_info.to_csv("active_with_info.csv", index=False)