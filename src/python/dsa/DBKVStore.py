class DBKVStore:
    def __init__(
            self, db_conn, table_name, key_column_name, value_column_name, key_type="TEXT", value_type="INTEGER",
            auto_commit=True
    ):
        self.table_name = table_name
        self.key_column_name = key_column_name
        self.value_column_name = value_column_name
        self.conn = db_conn
        self.cur = self.conn.cursor()
        self.cur.execute(
            f"create table {self.table_name} ({self.key_column_name} {key_type} PRIMARY KEY, "
            f"{self.value_column_name} {value_type})"
        )

    def set_auto_commit(self, auto_commit):
        self.auto_commit = auto_commit

    def __setitem__(self, key, value):
        self.cur.execute(f"REPLACE INTO {self.table_name} VALUES (?,?)", (key, value))
        if self.auto_commit:
            self.conn.commit()

    def __getitem__(self, item):
        result = self.cur.execute(
            f"SELECT {self.value_column_name} FROM {self.table_name} where {self.value_column_name} = ?", (item, )
        )
        if len(result) == 0:
            raise KeyError(f"Key {item} not in table")

        return result[0][0]

    def __contains__(self, item):
        try:
            temp = self[item]
            return True
        except KeyError:
            return False

    def get(self, item, default=None):
        try:
            temp = self[item]
            return temp
        except KeyError:
            return default

    def commit(self):
        self.conn.commit()