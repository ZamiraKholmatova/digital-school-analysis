import logging
from collections import namedtuple
from pathlib import Path

import pandas as pd

from dsa.data import SQLTable, DBKVStore
from dsa.data.adapters.DataAdapter import DataAdapter_United, DataAdapter_FoxFord, DataAdapter_MEO, DataAdapter_Uchi

MappingSpecification = namedtuple("MappingSpecification", ["table_name", "key_column", "value_column"])


class SharedModel:
    def __init__(
            self, args
    ):
        self.minute_activity = args.minute_activity
        self.resources_path = Path(args.resources_path)
        self.file_version_table_name = "file_versions"
        self.has_new_data = False
        self.set_paths()

        self.db = SQLTable(self.db_path)
        self.version_store = DBKVStore(
            self.db.conn, table_name=self.file_version_table_name, key_column_name="filename",
            value_column_name="version"
        )
        # self.version_store = DBKVStore(
        #     self.db, table_name="program_state", key_column_name="parameter",
        #     value_column_name="value"
        # )

        self.load_state()
        self.load_educational_institution(Path(args.educational_institution))
        self.load_profile_approved_status(Path(args.profile_educational_institution))
        self.load_student_grades(Path(args.student_grades))
        self.load_external_system(Path(args.external_system))
        self.load_course_types(args.course_types)
        self.load_billing_info(Path(args.billing))

        self.prepare_data_adapters(args)
        self.import_statistics()

    @staticmethod
    def get_file_version(path: Path):
        return int(path.stat().st_mtime)

    @staticmethod
    def read_table_dump(path, *args, **kwargs):
        return pd.read_csv(path, *args, **kwargs)

    def set_paths(self):
        self.state_file_path = self.resources_path.joinpath(f"{self.__class__.__name__}___state_file.json")
        self.db_path = self.resources_path.joinpath(f"{self.__class__.__name__}.db")
        self.mappings_path = self.resources_path.joinpath(f"{self.__class__.__name__}___mappings.pkl")

    def load_mapping_from_db(self, specification: MappingSpecification):
        table_name = specification.table_name
        key_column = specification.key_column
        value_column = specification.value_column

        try:
            mapping_df = self.db.query(
                f"""
                SELECT {key_column}, {value_column} from {table_name}
                """
            )
            mapping = dict(zip(mapping_df[key_column], mapping_df[value_column]))

            assert len(mapping_df) == len(mapping)
        except pd.io.sql.DatabaseError:
            mapping = {}
        return mapping

    def load_state(self):
        logging.info("Loading previous state")
        self.load_mappings()

    def set_new_data_flag(self, flag_value):
        self.has_new_data = flag_value

    def load_mappings(self):
        self.mapping_specifications = {
            'profile_id': MappingSpecification("profile_approved_status", "profile_id_uuid", "profile_id"),
            'educational_institution_id': MappingSpecification("educational_institution", "educational_institution_id_uuid", "educational_institution_id"),
            'provider_course_name': MappingSpecification("billing_info", "provider_course_name", "course_id"),
            'educational_course_id': MappingSpecification("course_information", "educational_course_id_uuid", "educational_course_id"),
            'educational_course_id2course_id': MappingSpecification("course_information", "educational_course_id", "course_id")
        }
        self.mappings = {key: self.load_mapping_from_db(spec) for key, spec in self.mapping_specifications.items()}

    def load_course_types(self, path):
        data = self.read_table_dump(path)
        self.course_types = {
            id_: type_name for id_, type_name in data.values
        }

    def get_course_type(self, type_id):
        return self.course_types[type_id]

    @staticmethod
    def add_missing_to_mapping(ids, mapping):
        if len(mapping.values()) == 0:
            valid_key = 0
        else:
            valid_key = max(mapping.values()) + 1
        for id_ in ids:
            if id_ not in mapping:
                mapping[id_] = valid_key
                valid_key += 1

    def convert_ids_to_int(self, table, columns, add_new=True):
        for column in columns:
            if column not in self.mappings:
                mapping = {}
                self.mappings[column] = mapping
            else:
                mapping = self.mappings[column]
            uuid_name = f"{column}_uuid"
            if add_new:
                self.add_missing_to_mapping(table[column].unique(), mapping)
            table.eval(f"{uuid_name} = {column}", inplace=True)
            table.eval(
                f"{column} = {column}.map(@mapping)",
                local_dict={"mapping": lambda id_: mapping.get(id_, pd.NA)},
                inplace=True
            )
            # table[uuid_name] = table[column]
            # table[column] = table[column].apply(lambda id_: mapping.get(id_, pd.NA))  # should fail for unknown ids
            if add_new:
                assert table[uuid_name].nunique() == table[column].nunique()

    @staticmethod
    def check_for_nas(data, field, path, error_buffer=None):
        if data[field].hasnans:
            nan_data = data[data[field].isna()]
            path = Path(path)
            parent = path.parent
            name = path.name
            error_filename = parent.joinpath(f"___{name}___{field}_nas.tsv")
            if error_buffer is None:
                nan_data.to_csv(error_filename, index=False, sep="\t")
            else:
                if error_filename in error_buffer:
                    error_buffer[error_filename] = error_buffer[error_filename].append(nan_data)
                else:
                    error_buffer[error_filename] = nan_data
            data.dropna(subset=[field], inplace=True)

        return data

    @staticmethod
    def normalize_is_deleted_field(table):
        table.eval(
            "is_deleted = is_deleted.map(@to_false)",
            local_dict={"to_false": lambda x: x == "t"}, inplace=True
        )

    def is_new_version(self, path: Path):
        file_version = self.get_file_version(path)
        last_file_version = self.version_store.get(str(path.name), default=0)
        return file_version > last_file_version

    def save_current_file_version(self, path: Path):
        self.version_store[str(path.name)] = self.get_file_version(path)

    @staticmethod
    def merge_provider_with_course_name(table):
        table.eval("provider_course_name = provider.map(@add_spacing) + course_name", inplace=True,
                  local_dict={"add_spacing": lambda x: x + "_"})

    def load_billing_info(self, path):
        if self.is_new_version(path):
            logging.info("Importing billing info")
            data = self.read_table_dump(path, dtype={"price": "Float32", "approved": "Float32"}) \
                .rename({"short_name": "provider"}, axis=1)

            # assert data["price"].isna().any() is False

            data.drop_duplicates(subset=["provider", "course_name"], inplace=True)
            data.eval("approved = approved.fillna(0.)", inplace=True)
            self.merge_provider_with_course_name(data)
            self.convert_ids_to_int(data, ["provider_course_name"])
            data.rename({"provider_course_name": "course_id", "provider_course_name_uuid": "provider_course_name"}, axis=1, inplace=True)
            self.db.replace_records(
                data[["provider", "course_name", "provider_course_name", "course_id", "price", "approved"]],
                "billing_info",
                dtype={
                    "course_id": "INT PRIMARY KEY",
                    "provider": "TEXT NOT NULL",
                    "course_name": "TEXT NOT NULL",
                    "price": "REAL NOT NULL",
                    "approved": "REAL NOT NULL"
                }
            )
            self.save_current_file_version(path)

    def load_student_grades(self, path):
        if self.is_new_version(path):
            logging.info("Importing student grades")
            data = self.read_table_dump(path, dtype={"grade": "Int32"}).rename({"id": "profile_id"}, axis=1)
            self.convert_ids_to_int(data, ["profile_id"], add_new=False)
            for field in ["grade", "profile_id"]:
                self.check_for_nas(data, field, str(path.absolute()) + f"_{self.__class__.__name__}")
            self.normalize_is_deleted_field(data)
            self.db.replace_records(
                data[["profile_id", "grade", "is_deleted"]],
                "student_grades",
                dtype={
                    "profile_id": "INT PRIMARY KEY",
                    "grade": "INT", "is_deleted": "INT NOT NULL"
                }
            )
            self.save_current_file_version(path)

    def load_external_system(self, path):
        data = self.read_table_dump(path)
        self.external_system = dict(zip(data["system_code"], data["short_name"]))

    def read_paper_letters_info(self, path):
        letters_status = self.read_table_dump(
            path, dtype={"ИНН": "Int64"},
        )
        return letters_status[["ИНН", "Статус письма"]].dropna(subset=["ИНН"])

    def read_schools_approved_in_november(self, path):
        schools = self.read_table_dump(path, dtype={"inn": "Int64"})
        schools = schools.query("approved == 1").rename({"inn": "ИНН"}, axis=1).dropna()
        return schools[["ИНН"]]

    @staticmethod
    def merge_special_status(paper_letters, approved_in_november):
        approved_status = paper_letters.copy()

        approved_in_november = set(
            approved_in_november["ИНН"]
        )

        activated_in_november_status = "Активировали в ноябре"

        special_status_records = []
        for ind, row in approved_status.iterrows():
            if row["ИНН"] in approved_in_november: continue
            rec = {
                "inn": row["ИНН"],
                "special_status": row["Статус письма"]
            }
            if pd.isna(rec["special_status"]) and rec["inn"] in approved_in_november:
                rec["special_status"] = activated_in_november_status
            special_status_records.append(rec)

        for school in approved_in_november:  # - set(approved_status["ИНН"]):
            rec = {
                "inn": school,
                "special_status": activated_in_november_status
            }
            special_status_records.append(rec)

        special_status = pd.DataFrame.from_records(special_status_records)
        return special_status

    def load_educational_institution(self, path):
        approved_in_november_path = path.parent.joinpath("approved_in_november____.csv")
        letter_schools_path = path.parent.joinpath("schools_paper_letters.csv")

        if self.is_new_version(path) or self.is_new_version(approved_in_november_path) or self.is_new_version(letter_schools_path):
            logging.info("Importing educational institutions")
            path = Path(path)
            data = self.read_table_dump(path, dtype={"inn": "Int64"}) \
                .rename({"id": "educational_institution_id"}, axis=1)

            paper_letters = self.read_paper_letters_info(letter_schools_path)

            approved_in_november = self.read_schools_approved_in_november(
                approved_in_november_path
                # path.parent.joinpath("schools_approved_in_november.xlsx")
            )

            special_status = self.merge_special_status(paper_letters, approved_in_november)

            # paper_letters.merge(special_status, how="outer", left_on="ИНН", right_on="inn").to_csv(
            #     "schools_special_status.csv", index=False)

            merged = data.merge(special_status, how="left", left_on="inn", right_on="inn").drop("inn", axis=1)

            self.convert_ids_to_int(merged, ["educational_institution_id"])

            self.db.replace_records(
                merged[["educational_institution_id", "educational_institution_id_uuid", "special_status"]],
                "educational_institution",
                dtype={
                    "educational_institution_id": "INT PRIMARY KEY",
                    "educational_institution_id_uuid": "TEXT UNIQUE NOT NULL",
                    "special_status": "TEXT"
                }
            )
            self.save_current_file_version(path)
            self.save_current_file_version(approved_in_november_path)
            self.save_current_file_version(letter_schools_path)

    def load_profile_approved_status(self, path):
        if self.is_new_version(path):
            logging.info("Importing profiles")
            data = self.read_table_dump(path, parse_dates=["updated_at", "approval_date"])
            self.convert_ids_to_int(data, ["profile_id"])
            self.convert_ids_to_int(data, ["educational_institution_id"], add_new=False)
            for field in ["profile_id", "educational_institution_id"]:
                self.check_for_nas(data, field, str(path.absolute()) + f"_{self.__class__.__name__}")
            self.normalize_is_deleted_field(data)

            self.db.replace_records(
                data[[
                    "profile_id", "profile_id_uuid", "approved_status", "role",
                    "educational_institution_id",
                    "is_deleted"
                ]], "profile_approved_status",
                dtype={
                    "profile_id": "INT PRIMARY KEY",
                    "profile_id_uuid": "TEXT UNIQUE NOT NULL",
                    "approved_status": "TEXT",
                    "role": "TEXT",
                    "educational_institution_id": "INT NOT NULL",
                    # "educational_institution_id_uuid": "TEXT NOT NULL",
                    "is_deleted": "INT NOT NULL"
                }
            )  # updated_at
            self.save_current_file_version(path)

    def prepare_data_adapters(self, args):
        self.adapters = [
            DataAdapter_United(shared_model=self, args=args),
            DataAdapter_FoxFord(shared_model=self, args=args),
            DataAdapter_MEO(shared_model=self, args=args),
            DataAdapter_Uchi(shared_model=self, args=args)
        ]

    def import_statistics(self):
        for adapter in self.adapters:
            for chunk in adapter:
                self.db.add_records(chunk, "course_statistics_pre")

    def needs_new_report(self):
        file_versions = self.db.query(f"select version from {self.file_version_table_name} where filename != 'report_version'")
        report_version = self.version_store.get("report_version", 0)
        if len(file_versions) == 0:
            raise Exception("Unknown error")
        last_file_version = int(file_versions["version"].max())
        return last_file_version > report_version

    def set_latest_report(self):
        file_versions = self.db.query(f"select version from {self.file_version_table_name} where filename != 'report_version'")
        if len(file_versions) == 0:
            raise Exception("Unknown error")
        last_file_version = int(file_versions["version"].max())
        self.version_store["report_version"] = last_file_version
