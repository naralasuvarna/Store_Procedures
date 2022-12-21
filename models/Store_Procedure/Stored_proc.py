def model(dbt,session):

    dbt.config(materialized="incremental")

    return session.sql("call emp_table('ACCOUNT')")