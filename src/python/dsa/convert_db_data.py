import logging
import sys
import pandas as pd
from pathlib import Path

def main():
    folder = Path(sys.argv[1])

    specifications = {
        "student_16.11.csv": {"dtype": {"grade": "Int32"}},
        "educational_institution_16.11.csv": {"dtype": {"inn": "string"}},
        "school_students.csv": {"dtype": {"ИНН": "string"}}
    }

    for file in folder.iterdir():
        fname = str(file.name)
        if fname.endswith(".csv"):
            print(f"Converting {fname} to pickle")
            if fname in specifications:
                data = pd.read_csv(file, **specifications[fname])
            else:
                data = pd.read_csv(file)
            data.to_pickle(str(file.absolute()).replace(".csv", ".bz2"), compression=None)


if __name__ == "__main__":
    main()
