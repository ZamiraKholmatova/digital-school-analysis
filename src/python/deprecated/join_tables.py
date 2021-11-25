#%%
import pandas as pd

table1 = pd.read_excel("/Users/LTV/Downloads/Список ОО +emails.xlsx")


#%%
table2 = pd.read_csv("/Users/LTV/Downloads/school.csv", sep=";")
# %%
table2.rename({"inn":"ИНН"},axis=1,inplace=True)
# %%
result = table1.merge(table2[["ИНН","teacher_activate_count","student_activate_count"]],how="left",on="ИНН")
result = result.astype({"teacher_activate_count":"Int32","student_activate_count":"Int32"})
# %%
result[["ИНН","teacher_activate_count","student_activate_count"]].to_csv("/Users/LTV/Downloads/Список ОО +emails +activate_count.csv")
# %%
mapping1 = dict(zip(table2["ИНН"], table2["teacher_activate_count"]))
mapping2 = dict(zip(table2["ИНН"], table2["student_activate_count"]))

sink = table1.copy()
# %%
sink["teacher_activate_count"] = sink["ИНН"].apply(lambda x: mapping1.get(x, pd.NA))
sink["student_activate_count"] = sink["ИНН"].apply(lambda x: mapping2.get(x, pd.NA))

def gender(name):
    if isinstance(name, str):
        if name.endswith("а"):
            return "жен"
        else:
            return "муж"
    else:
        return name
sink["пол"] = sink["ФИО директора"].apply(gender)
# %%
sink[["ИНН","teacher_activate_count","student_activate_count","пол"]].to_csv("/Users/LTV/Downloads/Список ОО +emails +activate_count.csv")
# %%
from collections import Counter
inn = Counter(table2["ИНН"].tolist())

# %%
inn.most_common(10)
# %%
