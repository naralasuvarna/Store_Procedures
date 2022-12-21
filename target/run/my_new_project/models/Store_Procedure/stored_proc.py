begin;
    

        insert into DEVELOPER_DB.ANJALI_SCHEMA.stored_proc ("EMP_TABLE")
        (
            select "EMP_TABLE"
            from DEVELOPER_DB.ANJALI_SCHEMA.stored_proc__dbt_tmp
        );
    commit;