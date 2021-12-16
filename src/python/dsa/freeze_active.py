#%%
import pandas as pd


#%%

billing_report = pd.read_excel("/Users/LTV/Documents/school/code/html/temp/billing_report_2021-12-01_07-02-24.xlsx")
billing_report_uchi = pd.read_excel("/Users/LTV/Documents/school/code/html/temp/billing_report_uchi_2021-12-01_07-02-24.xlsx", sheet_name=None)
#%%
already_active = billing_report[['Наименование образовательной цифровой площадки', 'Наименование ЦОК',
       'Идентификационный номер обучающегося']]

for sheet_name, sheet  in billing_report_uchi.items():
    if sheet_name == "Индекс курсов":
        continue
    already_active = already_active.append(sheet[['Наименование образовательной цифровой площадки', 'Наименование ЦОК',
       'Идентификационный номер обучающегося']])

already_active.to_csv("frozen_already_active.csv", index=False)