import sys
from tqdm import tqdm

user_mapping = {}

for line in tqdm(sys.stdin):
    parts = line.split(",")
    if len(parts) == 2:
        external_id, uuid = parts
        if external_id not in user_mapping:
            user_mapping[external_id] = set()

        user_mapping[external_id].add(uuid)


with open("user_mappings.csv", "w") as sink:
    sink.write("external_id,profile_id\n")
    for external_id, items in tqdm(user_mapping.items()):
        items = list(items)
        if len(items) == 1:
            sink.write(f"{external_id},{items[0]}\n")
        else:
            print(external_id, items, sep="\t")