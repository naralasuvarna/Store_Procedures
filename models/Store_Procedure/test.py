def model(dbt,session):

    dbt.config(materialized="incremental")

    return session.sql(" SET Variable_3 = 100; CALL sv_proc2($Variable_3)")