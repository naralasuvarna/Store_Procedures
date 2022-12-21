def model(dbt,session):
    dbt.config(materialized="table")
    return session.sql("call DEVELOPER_DB.QA_FRAMEWORK_SCHEMA.QA_FRAMEWORK('QA_CONFIG_TABLE')")