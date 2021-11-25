#%%
import pandas as pd
from tqdm import tqdm

checkpoints = pd.to_datetime(["2021-10-12", "2021-10-18", "2021-10-25", "2021-11-01", "2021-11-11", "2021-11-15", "2021-11-22"])
#%%

def read_chunks(filenames, name_map, type_map, date_checkpoints):
    date_checkpoints = copy(date_checkpoints)

    ckpt_lists = [[]]
    for filename in filenames:
        for chunk in pd.read_csv(
            filename, parse_dates=True, infer_datetime_format=True,
            chunksize=1000,
            ):
            chunk = chunk.rename(name_map, axis=1)
            chunk = chunk.as_type(type_map, axis=1)

            start_with = 0
            for ind, createdAt, externalUserId in enumerate(chunk.values):
                if createdAt > date_checkpoints[0]:
                    ckpt_lists[-1].append(chunk.iloc[start_with:ind])
                    start_with = ind
                    ckpt_lists.append([])
            if start_with != len(chunk) - 1:
                ckpt_lists[-1].append(chunk.iloc[start_with:ind])

            if len(ckpt_lists[-1]) == 0:
                ckpt_lists.pop(-1)




data = pd.read_csv("/Users/LTV/Downloads/учи.ру/nd/nd_login_logout_stats_10_11.csv")
data["created_at"] = pd.to_datetime(data["created_at"])
# %%

# %%
ilocs = []
for ind, row in tqdm(data.iterrows()):
    if row["created_at"] > checkpoints[0]:
        ilocs.append(ind)
        checkpoints = checkpoints[1:]
        if len(checkpoints) == 0:
            break
        
# %%

for i, ind in enumerate(ilocs):
    data.iloc[:ind].to_csv(f"/Users/LTV/Downloads/учи.ру/nd/nd_login_logout_stats_10_11_p{i}.csv", index=False)
data.iloc[ilocs[-1]:].to_csv(f"/Users/LTV/Downloads/учи.ру/nd/nd_login_logout_stats_10_11_p{len(ilocs)}.csv", index=False)
# %%

