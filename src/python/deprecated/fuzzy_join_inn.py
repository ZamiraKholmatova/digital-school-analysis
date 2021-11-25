#%% 
import pandas as pd
import editdistance

#%%
file_with_inn = "/Users/LTV/Downloads/учи.ру/get_inn/Список_ОО_только_ИНН,_название,_район,_регион.xlsx"
file_without_inn = "/Users/LTV/Downloads/учи.ру/get_inn/Для регистрации и отчета (360).xlsx"

#%%
inn_table = pd.read_excel(file_with_inn, dtype = {'ИНН': str})
no_inn_table = pd.read_excel(file_without_inn)

# %%
inn_table.dropna(subset=["ИНН"], inplace=True)
no_inn_table = no_inn_table[no_inn_table.columns[:3]]
# %%

inn_dict = {}
for _, inn, name, _, region in inn_table.values:
    if region not in inn_dict:
        inn_dict[region] = []

    inn_dict[region].append((inn, name))

#%%
no_inn_dict = {}
for region, _, name in no_inn_table.values:
    if region not in no_inn_dict:
        no_inn_dict[region] = []

    no_inn_dict[region].append(name)

#%%
from collections import Counter

words = []
for key in no_inn_dict:
    words.extend(key.split(" "))
for key in inn_dict:
    words.extend(key.split(" "))

for word, count in Counter(words).most_common():
    print(word, count)
#%%

stop_words = {"область", "Республика", "край", "округ", "автономный", "город", "автономная", "-", "республика"}

def normalize(word):
    parts = word.split(" ")
    return " ".join(part for part in parts if part not in stop_words)

no_inn_keys = list(no_inn_dict.keys())
inn_keys = list(inn_dict.keys())

key_matching = {}

for no_inn_key in no_inn_keys:
    no_inn_key_normalized = normalize(no_inn_key)
    for inn_key in inn_keys:
        inn_key_normalized = normalize(inn_key)
        dist = editdistance.eval(no_inn_key_normalized, inn_key_normalized)
        old_dist, old_match = key_matching.get(no_inn_key, (100000000,"none"))
        if dist < old_dist:
            key_matching[no_inn_key] = (dist, inn_key)

# %%
for key, val in key_matching.items():
    print(key, val)

# key_matching["Республика Адыгея"] = None
# key_matching["Республика Алтай"] = None
# key_matching["Республика Башкортостан"] = None
# key_matching["Республика Бурятия"] = None
# key_matching["Республика Адыгея"] = None
# key_matching["Республика Адыгея"] = None
# key_matching["Республика Адыгея"] = None
# key_matching["Республика Адыгея"] = None
# key_matching["Республика Адыгея"] = None
# key_matching["Республика Адыгея"] = None
# %%

words = []
for name in inn_table["Наименование"]:
    words.extend(key.split(" "))
for key in no_inn_table["Наименование образовательной организации"]:
    words.extend(key.split(" "))

name_stop_words = set()
for word, count in Counter(words).most_common():
    print(word, count)
    if count > 2:
        name_stop_words.add(word)
# %%

name_stop_words = {"ОБРАЗОВАТЕЛЬНАЯ", "ОРГАНИЗАЦИЯ", "НЕ", "НАЙДЕНА", "О", "У", "М", "ОБЩЕОБРАЗОВАТЕЛЬНАЯ", "Б", "ШКОЛА", "СРЕДНЯЯ", "№", "РАЙОНА", "ШКОЛА", "СРЕДНЯЯ", "К", "ОБЛАСТИ", "Г", "ИМЕНИ", "МУНИЦИПАЛЬНОГО", "РЕСПУБЛИКИ", "ОСНОВНАЯ", "А", "П", "ГОРОДА", "Г.", "С.", "РАЙОН", "ОКРУГА", "ГОРОДСКОГО", "ГЕРОЯ", "С", "СОВЕТСКОГО", "МО", "СОЮЗА", "ШКОЛА", "БАШКОРТОСТАН", "-", "ИЗУЧЕНИЕМ", "НАЧАЛЬНАЯ", "И", "УГЛУБЛЕННЫМ", "ОБЛАСТИ", "ОСНОВНАЯ", "ИМ.", "КОЛЛЕДЖ", "ТАТАРСТАН", "КРАЯ", "Ч", "САМАРСКОЙ", "ДЛЯ", "САРАТОВСКОЙ", "ГИМНАЗИЯ", "КОЛЛЕДЖ", "ОТДЕЛЬНЫХ", "ТЕХНИКУМ", "ШКОЛА-ИНТЕРНАТ", "ГИМНАЗИЯ", "МОСКВЫ", "САНКТ-ПЕТЕРБУРГА", "СЕЛА", "ГОРОД"}

def normalize(word):
    return word
    parts = word.split(" ")
    return " ".join(part for part in parts if part not in name_stop_words)

no_inn_keys = list(no_inn_dict.keys())

final_match = {}

for no_inn_key in no_inn_keys:
    _, inn_key = key_matching[no_inn_key]

    name_matching = {}

    for no_inn_candidate in no_inn_dict[no_inn_key]:
        no_inn_candidate_norm = normalize(no_inn_candidate)
        for inn, inn_candidate in inn_dict[inn_key]:

            inn_candidate_norm = normalize(inn_candidate)

            dist = editdistance.eval(no_inn_candidate_norm, inn_candidate_norm)
            old_dist, old_match, old_inn = name_matching.get(no_inn_candidate, (100000000, "none", "inn"))

            if dist < old_dist:
                name_matching[no_inn_candidate] = (dist, inn_candidate, inn)

    final_match[no_inn_key] = name_matching
    break

# %%

name_match = {}

name_stop_words = {"ОБРАЗОВАТЕЛЬНАЯ", "ОРГАНИЗАЦИЯ", "ОБЩЕОБРАЗОВАТЕЛЬНАЯ", "ШКОЛА", "СРЕДНЯЯ", "РАЙОНА", "ШКОЛА", "СРЕДНЯЯ", "ОБЛАСТИ", "ИМЕНИ", "МУНИЦИПАЛЬНОГО", "РЕСПУБЛИКИ", "ГОРОДА", "РАЙОН", "ОКРУГА", "ГОРОДСКОГО", "ШКОЛА"}

def normalize(word):
    word.replace("СРЕДНЯЯ ОБЩЕОБРАЗОВАТЕЛЬНАЯ ШКОЛА", "СОШ")
    word.replace("СРЕДНЯЯ ШКОЛА", "СШ")
    parts = word.split(" ")
    return " ".join(part for part in parts if part not in name_stop_words)


for ind, no_inn_candidate in enumerate(no_inn_table["Наименование образовательной организации"]):
    no_inn_candidate_norm = normalize(no_inn_candidate)

    for inn, inn_candidate in inn_table[["ИНН", "Наименование"]].values:
        inn_candidate_norm = normalize(inn_candidate)

        dist = editdistance.eval(no_inn_candidate_norm, inn_candidate_norm)
        old_dist, old_match, old_inn = name_match.get(no_inn_candidate, (100000000, "none", "inn"))

        if dist <= old_dist:
            name_match[no_inn_candidate] = (dist, inn_candidate, inn)
    
    if ind > 20:
        break
# %%
