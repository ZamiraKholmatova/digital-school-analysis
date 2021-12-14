#%%
# from uuid import UUID

import postgresql
import pandas as pd
from sqlalchemy.types import Boolean, Date, String, Integer
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import UUID
#%%
# import psycopg2

# conn = psycopg2.connect("dbname=postgres user=postgres password=example port=6001 host=localhost")
engine = create_engine("postgresql://postgres:example@localhost:6001/postgres")
# conn = engine.raw_connection()
#%%
data = pd.read_csv("/home/ltv/dev/digital-school-analysis/data/db_data/statistic_type_16.11.csv")
#%%
# conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS billing;")
#%%
data.to_sql(
    "statistic_types", engine, index=False, if_exists="replace",
    dtype={
        "id": UUID,
        "type_name": String,
        "type": String,
        "entity_id": Integer,
        "updated_at": Date
    }
)

#%%
conn = engine.raw_connection()
# conn.cursor().execute("ALTER TABLE statistic_types ADD PRIMARY KEY (id);")
conn.cursor().execute("ALTER TABLE statistic_types ADD UNIQUE(type_name);")
#%%
conn.commit()
