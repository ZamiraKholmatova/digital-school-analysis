import pandas as pd
import sys

from tqdm import tqdm

from dsa.writers import ReportWriter

def main():
    region_info_path = sys.argv[1]
    billing_path = sys.argv[2]

    ri = pd.read_csv(region_info_path)
    s2r = dict(zip(ri["profile_id"], ri["Регион"]))
    bl = pd.read_excel(billing_path, sheet_name=None)

    report_writer = ReportWriter("", "", queries_path=None)

    missing = set()

    def get_region(id):
        if id in s2r:
            return s2r[id]
        else:
            missing.add(id)

    for ind, (sheet_name, sheet) in tqdm(enumerate(bl.items())):

        if ind != 0:
            sheet["Регион"] = sheet["Идентификационный номер обучающегося"].apply(get_region)

        report_writer.add_sheet(sheet_name, sheet)

    report_writer.save_report()

    print(len(missing))
    print(missing)



if __name__ == "__main__":
    main()