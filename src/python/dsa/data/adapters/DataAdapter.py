import logging
from abc import abstractmethod
from math import isnan
from pathlib import Path

import pandas as pd

from dsa.data.adapters.StatisticsCombiner import preprocess, map_partitions_1c_nd, map_partitions_foxford, \
    map_partitions_meo, map_partitions_uchi


class DataAdapter:
    def __init__(
            self, shared_model, args
    ):
        self.statistics_import_chunk_size = 1000000

        self.shared_model = shared_model
        self.has_new_data = False
        self.args = args

        self.load_course_structure(self.get_course_structure_path(args))
        self.preprocess(self.get_course_statistics_path(args))
        self.set_preprocessed_path(args)
        self.load_course_statistics()


    @staticmethod
    def get_course_structure_path(args):
        return Path(args.course_structure)

    @staticmethod
    def get_course_statistics_path(args):
        logging.info("Importing course statistics")
        return Path(args.course_statistics)

    def set_preprocessed_path(self, args):
        self.preprocessed_path = Path(self.get_course_statistics_path(args)).joinpath("preprocessed3d")

    def get_raw_statistics_files(self, path):
        return map(Path, filter(
            lambda x: not x.startswith(".") and x.endswith(".csv.bz2"),
            (str(p.absolute()) for p in path.iterdir())
        ))

    def preprocess(self, path):
        files = self.get_raw_statistics_files(path)
        preprocess(files, map_partitions_1c_nd)

    # @property
    # def db(self):
    #     return self.shared_model.db
    #
    # def convert_ids_to_int(self, *args, **kwargs):
    #     self.shared_model.convert_ids_to_int(*args, **kwargs)
    #
    # def check_for_nas(self, *args, **kwargs):
    #     self.shared_model.convert_ids_to_int(*args, **kwargs)

    def format_course_structure_columns(self, data):
        data = data.rename({"deleted": "is_deleted"}, axis=1)
        data = data.query(
            "system_code == '0b37f22e-c46c-4d53-b0e7-8bdaaf51a8d0' or system_code == '3a4b37c1-1f7d-4cb9-b144-e24c708d9c20' or "
            "system_code == 'd2735d92-6ad6-49c4-9b36-c3b16cee695d' or system_code == '13788b9a-3426-45b2-9ba5-d8cec8c03c0c' or "
            "system_code == '61dbfd85-2f0b-49eb-ad60-343cc5f12a36' or system_code == '1d258153-7d01-4ed7-9035-3f9df9cf578f' or "
            "system_code == 'f1e908c8-7d15-11ec-90d6-0242ac120003' or system_code == '2ca72c8e-8594-11ec-a8a3-0242ac120002'"
        )
        return data

    def validate_structure_id(self, id_, parent_id, structure):
        assert id_ not in structure

    def resolve_structure(self, data):
        fields = data.columns
        structure = {}
        for ind, row in data.iterrows():
            self.validate_structure_id(row["id"], row["parent_id"], structure)
            structure[row["id"]] = {key: row[key] for key in fields}
        self.structure = structure

        mapping = []
        for id_ in structure:
            course_name, provider, is_deleted = self.find_subject(id_)
            mapping.append({
                "educational_course_id": id_,
                "course_name": course_name,
                "provider": provider,
                "is_deleted": is_deleted
            })
        return pd.DataFrame(mapping)

    def prepare_course_ids(self, data, path):
        self.shared_model.merge_provider_with_course_name(data)
        self.shared_model.convert_ids_to_int(data, ["educational_course_id"])
        self.shared_model.convert_ids_to_int(data, ["provider_course_name"], add_new=False)
        self.shared_model.check_for_nas(data, "provider_course_name", Path(str(path.absolute()) + f"_{self.__class__.__name__}"))
        data.rename({"provider_course_name": "course_id"}, axis=1, inplace=True)
        self.shared_model.mappings["educational_course_id2course_id"].update(dict(zip(data["educational_course_id"], data["course_id"])))

        return data

    def load_course_structure(self, path):
        if self.shared_model.is_new_version(path):
            data = self.shared_model.read_table_dump(path)
            data = self.format_course_structure_columns(data)
            if "is_deleted" in data.columns:
                self.shared_model.normalize_is_deleted_field(data)
            else:
                data["is_deleted"] = False
            # data = data.query("is_deleted == False")
            data = self.resolve_structure(data)
            data = self.prepare_course_ids(data, path)
            # provider = next(iter(data["provider"]))
            providers = data["provider"].unique()

            try:
                query_cond = " and ".join(f"provider != '{provider}'" for provider in providers)
                existing = self.shared_model.db.query(
                    f"""
                    SELECT * from course_information where {query_cond}
                    """
                )
            except:
                existing = None

            if existing is None:
                to_write = data
            else:
                to_write = existing.append(data)

            if existing is not None:
                self.shared_model.db.drop_table("course_information_backup")
                self.shared_model.db.execute("create table course_information_backup as select * from course_information")
            self.shared_model.db.drop_table("course_information")

            self.shared_model.db.replace_records(
                to_write[[
                    "educational_course_id", "educational_course_id_uuid",
                    "course_name", "provider", "course_id", "is_deleted"]],
                "course_information",
                dtype={
                    "educational_course_id": "INT PRIMARY KEY",
                    "educational_course_id_uuid": "TEXT UNIQUE NOT NULL",
                    "course_name": "TEXT NOT NULL",
                    "provider": "TEXT NOT NULL",
                    "course_id": "INT NOT NULL",
                    "is_deleted": "INT NOT NULL"
                }
            )
            self.shared_model.save_current_file_version(path)

    def remove_one_id_level(self, course_id):
        return "/".join(course_id.split("/")[:-1])

    def get_course_type(self, type_id):
        return self.shared_model.get_course_type(type_id)

    def find_subject(self, course_id):
        if not isinstance(course_id, float) and "foxford" in course_id and course_id not in self.structure:
            course_id = self.remove_one_id_level(course_id)
        if not isinstance(course_id, float) and course_id not in self.structure:
            if f"Lesson_{course_id}" in self.structure:
                course_id = f"Lesson_{course_id}"
            elif f"Chapter_{course_id}" in self.structure:
                course_id = f"Chapter_{course_id}"
            elif f"Topic_{course_id}" in self.structure:
                course_id = f"Topic_{course_id}"
            elif f"Course_{course_id}" in self.structure:
                course_id = f"Course_{course_id}"
        if course_id not in self.structure:
            return None, None, None
        course = self.structure[course_id]
        # print(course["course_name"], course_types[course["course_type_id"]], course["external_link"], sep="\t")
        parent_id = course["parent_id"]
        # print(parent_id, type(parent_id))
        if isinstance(parent_id, float) and isnan(parent_id):
            course_name = course["course_name"]
            course_type = self.get_course_type(course["course_type_id"])
            provider = self.shared_model.external_system[course["system_code"]]
            self.validate_course(course_name, course_type, provider)
            return course_name, provider, course["is_deleted"]
        return self.find_subject(parent_id)

    def map_course_statistics_columns(self, data):
        pass

    # def preprocess_chunk(self, chunk: pd.DataFrame, path, error_buffer, drop_duplicates=False):
    #     path = Path(path)
    #     self.map_course_statistics_columns(chunk)
    #     column_order = ["profile_id", "educational_course_id", "created_at"]
    #     chunk = chunk[column_order]
    #     # chunk.dropna(axis=0, inplace=True)
    #     for col in column_order:
    #         self.shared_model.check_for_nas(chunk, col, path, error_buffer=error_buffer)
    #         # chunk = chunk[~chunk[col].isnull()]
    #     chunk.eval("created_at_original = created_at", inplace=True)
    #     chunk.eval("created_at = created_at.dt.normalize()", inplace=True)
    #     # chunk["created_at_original"] = chunk["created_at"]
    #     # chunk["created_at"] = chunk["created_at"].dt.normalize()
    #     self.shared_model.convert_ids_to_int(chunk, ["profile_id", "educational_course_id"], add_new=False)
    #     chunk.eval( # this lookup can fail only if convert_ids_to_int fails
    #         "educational_course_id = educational_course_id.map(@resolve_id)",
    #         local_dict={"resolve_id": lambda id_: self.shared_model.mappings["educational_course_id2course_id"].get(id_, pd.NA)},
    #         inplace=True
    #     )
    #     # chunk["educational_course_id"] = chunk["educational_course_id"].apply(
    #     #     lambda id_: self.educational_course_id2course_id.get(id_, pd.NA)
    #     # )
    #     for col in ["profile_id", "educational_course_id"]:
    #         self.check_for_nas(chunk, col, str(path.absolute()) + f"___unresolved", error_buffer=error_buffer)
    #     # self.check_for_nas(chunk, "educational_course_id", path.parent.joinpath("statistics_resolved_course_id"))
    #     if drop_duplicates:
    #         chunk.drop_duplicates(subset=column_order, inplace=True)
    #     chunk.drop(["profile_id_uuid", "educational_course_id_uuid"], axis=1, inplace=True)
    #     return chunk

    def prepare_statistics_table(self):
        pass

    def filter_statistics_files(self, files):
        return filter(lambda filename: filename.endswith(".csv") and not filename.startswith("."), files)

    # @abstractmethod
    # def get_statistics_pre_table_name(self):
    #     pass
    #     # return "course_statistics_pre_unified"

    @abstractmethod
    def get_statistics_table_name(self):
        pass
        # return "course_statistics_unified"

    @abstractmethod
    def get_active_days_count_table_name(self):
        pass
        # return "active_days_count_unified"

    # def compute_active_days(self):
    #     if self.has_new_data:
    #         logging.info("Computing active days")
    #         self.shared_model.db.drop_table(self.get_statistics_table_name())
    #         self.shared_model.db.execute(
    #             f"""
    #             CREATE TABLE {self.get_statistics_table_name()} AS
    #             SELECT
    #             profile_id, educational_course_id, date,
    #             sum(dt) as active_time,
    #             sum(dt) >= 600 as is_active
    #             FROM
    #             {self.get_statistics_pre_table_name()}
    #             GROUP BY
    #             profile_id, educational_course_id, date
    #             """,
    #         )

    # def compute_active_days_count(self):
    #     if self.has_new_data:
    #         logging.info("Computing full report")
    #         for table_name in [self.get_active_days_count_table_name()]:
    #             self.shared_model.db.drop_table(table_name)
    #
    #         self.shared_model.db.execute(
    #             f"""
    #             CREATE TABLE {self.get_active_days_count_table_name()} AS
    #             SELECT
    #             educational_course_id, profile_id,
    #             CAST(COUNT(CASE WHEN is_active = true THEN 1 ELSE NULL END) AS INTEGER) AS "active_days"
    #             FROM
    #             {self.get_statistics_table_name()}
    #             {self.get_freeze_date_filtration_rule()}
    #             GROUP BY educational_course_id, profile_id
    #             """
    #         )

    def get_freeze_date_filtration_rule(self):
        if self.args.freeze_date is not None:
            return f"WHERE course_statistics.date < '{self.args.freeze_date}'"
        else:
            return ""

    def load_course_statistics(self):
        for chunk in self.iterate_preprocessed():
            self.shared_model.db.add_records(chunk, self.get_statistics_table_name())

        # self.compute_active_days()
        # self.compute_active_days_count()

    # def load_course_statistics(self, path, date_field="created_at", **kwargs):
    #     self.prepare_statistics_table()
    #     target_table = "course_statistics" if self.minute_activity is False else "course_statistics_pre"
    #     filename = os.path.basename(path)
    #     if filename not in self.processed_files:
    #         error_buffer = {}
    #         for ind, chunk in enumerate(pd.read_csv(
    #                 path, chunksize=self.statistics_import_chunk_size, parse_dates=[date_field], **kwargs
    #         )):
    #             self.db.add_records(
    #                 self.preprocess_chunk(chunk, path, error_buffer, drop_duplicates=not self.minute_activity),
    #                 target_table,
    #                 # dtype={
    #                 #     "profile_id": "INT NOT NULL",
    #                 #     "educational_course_id": "INT NOT NULL",
    #                 #     "created_at": "TIMESTAMP NOT NULL",
    #                 #     "created_at_original": "TIMESTAMP NOT NULL"
    #                 # }
    #             )
    #         for error_file, content in error_buffer.items():
    #             content.to_csv(error_file, index=False, sep="\t")
    #         self.processed_files.append(filename)
    #         self.has_new_data = True
    #         self.save_state()

    def entry_valid(self, profile_id, statistic_type_id, educational_course_id, created_at):
        profile_id = profile_id  # entry["profile_id"]
        course_id = educational_course_id  # entry["educational_course_id"]
        if isinstance(profile_id, float) and isnan(profile_id):
            return False
        elif isinstance(course_id, float) and isnan(course_id) or pd.isna(course_id):
            return False
        else:
            return True

    def validate_course(self, subject_name, course_type, provider):
        if subject_name is None and course_type is None and provider is None:
            return
        assert course_type == "ЦОМ"
        assert isinstance(provider, str)
        assert isinstance(subject_name, str)

    def get_preprocessed_files(self):
        return (file for file in self.preprocessed_path.iterdir() if file.name.endswith("___preprocessed.csv.bz2"))

    def delete_if_needed(self):
        pass

    def iterate_preprocessed(self):
        for file in self.get_preprocessed_files():
            if self.shared_model.is_new_version(file):
                self.delete_if_needed()
                for chunk in pd.read_csv(
                    file, chunksize=1000000,
                    names=["profile_id", "educational_course_id", "created_at"], header=None,
                    parse_dates=["created_at"], dtype={"educational_course_id": "string"}
                ):
                    def encode_course_id(id_):
                        return self.shared_model.mappings["educational_course_id2course_id"].get(
                            self.shared_model.mappings["educational_course_id"].get(id_, pd.NA),
                            pd.NA
                        )

                    def encode_profile_id(id_):
                        return self.shared_model.mappings["profile_id"].get(id_, pd.NA)

                    chunk["educational_course_id"] = chunk["educational_course_id"].apply(encode_course_id)
                    chunk["profile_id"] = chunk["profile_id"].apply(encode_profile_id)
                    chunk.dropna(inplace=True)
                    yield chunk
                self.shared_model.save_current_file_version(file)
                self.has_new_data = True

    def __iter__(self):
        return self.iterate_preprocessed()



class DataAdapter_United(DataAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def delete_if_needed(self):
        self.shared_model.db.drop_table(self.get_statistics_table_name())

    def get_statistics_pre_table_name(self):
        return "course_statistics_pre_unified"

    def get_statistics_table_name(self):
        return "course_statistics_unified"

    def get_active_days_count_table_name(self):
        return "active_days_count_unified"


class DataAdapter_FoxFord(DataAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_course_structure_path(args):
        logging.info("Importing course statistics for FoxFord")
        return Path(args.course_structure_foxford)

    @staticmethod
    def get_course_statistics_path(args):
        return Path(args.course_statistics_foxford)

    def preprocess(self, path):
        files = self.get_raw_statistics_files(path)
        preprocess(files, map_partitions_foxford)

    def format_course_structure_columns(self, data):
        data.rename({
            "externalId": "id",
            "externalParentId": "parent_id",
            "courseName": "course_name",
            "courseTypeId": "course_type_id",
            "externalLink": "external_link"
        }, axis=1, inplace=True)
        data["system_code"] = "13788b9a-3426-45b2-9ba5-d8cec8c03c0c"
        return data

    def get_course_type(self, type_id):
        if type_id == 0:
            return "ЦОМ"
        elif type_id == 2:
            return "Урок"
        elif type_id == 3:
            return "Задача"
        else:
            raise ValueError()

    def get_statistics_type(self, type_id):
        if type_id == 0:
            return  "login"
        elif type_id == 2:
            return "started_studying"
        else:
            return "logout"

    def validate_structure_id(self, id_, parent_id, structure):
        if id_ in structure:
            assert parent_id == structure[id_]["parent_id"]

    def map_course_statistics_columns(self, data):
        def normalize(id_):
            if pd.isna(id_):
                return id_
            else:
                return "/".join(id_.split("/")[:5])
        data.eval(
            "educational_course_id = educational_course_id.map(@normalize)",
            local_dict={"normalize": normalize},
            inplace=True
        )
        return data

    # def get_statistics_pre_table_name(self):
    #     return "course_statistics_pre_foxford"

    def get_statistics_table_name(self):
        return "course_statistics_foxford"

    def get_active_days_count_table_name(self):
        return "active_days_count_foxford"


class DataAdapter_MEO(DataAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_course_structure_path(args):
        logging.info("Importing course statistics for MEO")
        return Path(args.course_structure_meo)

    @staticmethod
    def get_course_statistics_path(args):
        return Path(args.course_statistics_meo)

    def preprocess(self, path):
        files = self.get_raw_statistics_files(path)
        preprocess(files, map_partitions_meo)

    def format_course_structure_columns(self, data):
        data.rename({"material_id": "educational_course_id"}, axis=1, inplace=True)
        data["system_code"] = "61dbfd85-2f0b-49eb-ad60-343cc5f12a36"
        return data.astype({"educational_course_id": "string"})

    def map_course_statistics_columns(self, data):
        # data.rename({
        #     "Start": "created_at",
        #     "profileId": "profile_id",
        #     "CourseId": "educational_course_id",
        # }, axis=1, inplace=True)
        # data.astype({"educational_course_id": "Int64"}, inplace=True)
        return data

    def resolve_structure(self, data):
        return data

    # def get_statistics_pre_table_name(self):
    #     return "course_statistics_pre_meo"

    def get_statistics_table_name(self):
        return "course_statistics_meo"

    def get_active_days_count_table_name(self):
        return "active_days_count_meo"


class DataAdapter_Uchi(DataAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_course_structure_path(args):
        logging.info("Importing course statistics UCHI")
        return Path(args.course_structure_uchi)

    @staticmethod
    def get_course_statistics_path(args):
        return Path(args.course_statistics_uchi)

    def preprocess(self, path):
        files = self.get_raw_statistics_files(path)
        preprocess(files, map_partitions_uchi)

    def get_statistics_type(self, type_id):
        if type_id == 0:
            return "login"
        elif type_id == 2:
            return "started_studying"
        else:
            return "logout"

    def format_course_structure_columns(self, data):
        data.drop(["id", "parent_id"], axis=1, inplace=True)
        data = data.query(f"system_code == 'd2735d92-6ad6-49c4-9b36-c3b16cee695d'")
        data.rename({
            "external_id": "id",
            "external_parent_id": "parent_id",
            # "courseName": "course_name",
            "courseTypeId": "course_type_id",
            "externalLink": "external_link"
        }, axis=1, inplace=True)
        # data["system_code"] =
        return data  # lesson chapter topic course

    def map_course_statistics_columns(self, data):
        pass
        # data.rename({
        #     "createdAt": "created_at",
        #     "statisticsTypeId": "statistic_type_id",
        #     "userId": "profile_id",
        #     "externalId": "educational_course_id",
        # }, axis=1, inplace=True)
        # return data[["profile_id", "educational_course_id", "created_at"]]

    # def get_statistics_pre_table_name(self):
    #     return "course_statistics_pre_uchi"

    def get_statistics_table_name(self):
        return "course_statistics_uchi"

    def get_active_days_count_table_name(self):
        return "active_days_count_uchi"
