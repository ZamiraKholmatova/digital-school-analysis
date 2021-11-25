#%%

import pandas as pd
from tqdm import tqdm

#%%

target = pd.read_excel("/Users/LTV/Downloads/учи.ру/match_by_inn/Список ОО (база Минпроса).xlsx", dtype = {'ИНН': str})
source = pd.read_excel("/Users/LTV/Downloads/учи.ру/match_by_inn/Статистика по регионам + emails 211110.xlsx", dtype = {'инн_школ': str})
# %%

target.dropna(subset=["ИНН"], inplace=True)
source.dropna(subset=["инн_школ"], inplace=True)
#%%

def normalize(inn):
    to_add = 12 - len(inn)
    return "0" * to_add + inn

target["inn_norm"] = target["ИНН"].apply(normalize)
source["inn_norm"] = source["инн_школ"].apply(normalize)

# assert all(target["ИНН"].apply(lambda x: len(x) == 10))
# assert all(source["инн_школ"].apply(lambda x: len(x) == 10))

#%%

from collections import Counter

non_unique = set()

for inn, count in Counter(target["ИНН"]).most_common():
    if count > 1:
        non_unique.add(inn)
        # print(inn, count)
        candidates = target.query(f"ИНН == '{inn}'")
        if len(set(candidates["Наименование полное"])) != 1:
            print(candidates.to_string())


# assert len(target["inn_norm"]) == len(set(target["inn_norm"]))
# assert len(source["inn_norm"]) == len(set(source["inn_norm"]))

len(set(target["ИНН"]))

# {'000208001927',
#  '000216004664',
#  '000220000768',
#  '000222004119',
#  '000230002362',
#  '000232003668',
#  '000234003127',
#  '000237002114',
#  '000241002830',
#  '000241005407',
#  '000242004692',
#  '000242004928',
#  '000242005015',
#  '000249003190',
#  '000251003670',
#  '000254005468',
#  '000254006214',
#  '000255009994',
#  '000267007007',
#  '000268020890',
#  '000705001620',
#  '001425003137',
#  '001647007380',
#  '002011001779',
#  '002016082450',
#  '002501004948',
#  '002503006683',
#  '002506001757',
#  '002507006268',
#  '002508022784',
#  '002511008571',
#  '002511009342',
#  '002513002208',
#  '002513003427',
#  '002515002348',
#  '002516001058',
#  '002518000998',
#  '002518001381',
#  '002529005490',
#  '002534004728',
#  '002534004982',
#  '002535003798',
#  '002535003942',
#  '002536038419',
#  '002536052438',
#  '002539009825',
#  '002539041794',
#  '002540014562',
#  '002814002459',
#  '002819003181',
#  '002824003691',
#  '002828006527',
#  '002922001090',
#  '003662070245',
#  '004710023602',
#  '006206001464'}

#%%

for inn, count in Counter(source["inn_norm"]).most_common():
    if count > 1:
        print(inn, count)
        # candidates = source.query(f"inn_norm == '{inn}'")
        # if len(set(candidates["Наименование полное"])) != 1:
        #     print(candidates.to_string())

assert len(source["inn_norm"]) == len(set(source["inn_norm"]))
#%%

columns_to_move = [
    "учителей активировано",
"учеников активировано",
"Unnamed: 11",
"Unnamed: 12"
]

info = {}

for ind, row in tqdm(source.iterrows()):
    info[row["inn_norm"]] = {key: row[key] for key in columns_to_move}

# %%

def get(inn, key):
    records = info.get(inn, None)
    if records is not None:
        return records[key]
    return pd.NA


for key in columns_to_move:
    target[key] = target["inn_norm"].apply(lambda x: get(x, key))
# %%

to_out = target.rename({"Unnamed: 11": "emails__", "Unnamed: 12": "email верифицирован"}, axis = 1)
# %%
to_out[[
    "учителей активировано",
    "учеников активировано",
    "emails__",
    "email верифицирован"
]].replace(pd.NA, "").to_csv("email_match.csv", index=False)
# %%
