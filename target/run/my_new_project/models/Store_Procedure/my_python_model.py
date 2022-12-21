
  
    
import pandas as pd
import snowflake.snowpark.functions as F
import numpy
from snowflake.snowpark import Session
import snowflake.connector
import cursor
# cs = ctx.cursor()
# conn = snowflake.connector.connect(
#                 user="ANJALIN@BOOLEANDATA.COM",
#                 password="Anjali@123",
#                 account="oseazbt-booleandata_partner",
#                 warehouse="BOOLEAN_DEV_WH",
#                 database="DEVELOPER_DB",
#                 schema="ANJALI_SCHEMA"
#                 );
def model(dbt, session):

    my_sql_model_df = dbt.ref("my_first_dbt_model")

    final_df = cursor.execute("call emp_table('Campaign')")  # stuff you can't write in SQL!

    return final_df

# snowuser = 'anilk@booleandata.com'                                                                        

# snowpass = 'Anil@8123'

# snowacct = 'oseazbt-booleandata_partner'



# from snowflake.snowpark import Session



# sess = None



# print('connecting..')

# cnn_params = {

#     "account": snowacct,

#     "user": snowuser,

#     "password": snowpass,

#     "warehouse": "BOOLEAN_DEV_WH",

#     "database": "DEVELOPER_DB",

#     "schema": "ANIL_SCHEMA"

# }


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args,dbt_load_df_function):
    refs = {"my_first_dbt_model": "DEVELOPER_DB.ANJALI_SCHEMA.my_first_dbt_model"}
    key = ".".join(args)
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = ".".join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = 'DEVELOPER_DB'
    schema = 'ANJALI_SCHEMA'
    identifier = 'my_python_model'
    def __repr__(self):
        return 'DEVELOPER_DB.ANJALI_SCHEMA.my_python_model'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------

# To run this in snowsight, you need to select entry point to be main
# And you may have to modify the return type to text to get the result back
# def main(session):
#     dbt = dbtObj(session.table)
#     df = model(dbt, session)
#     return df.collect()

# to run this in local notebook, you need to create a session following examples https://github.com/Snowflake-Labs/sfguide-getting-started-snowpark-python
# then you can do the following to run model
# dbt = dbtObj(session.table)
# df = model(dbt, session)


def materialize(session, df, target_relation):
    # make sure pandas exists
    import importlib.util
    package_name = 'pandas'
    if importlib.util.find_spec(package_name):
        import pandas
        if isinstance(df, pandas.core.frame.DataFrame):
          # session.write_pandas does not have overwrite function
          df = session.createDataFrame(df)
    df.write.mode("overwrite").save_as_table("DEVELOPER_DB.ANJALI_SCHEMA.my_python_model", create_temp_table=False)

def main(session):
    dbt = dbtObj(session.table)
    df = model(dbt, session)
    materialize(session, df, dbt.this)
    return "OK"

  