
  create or replace  view DEVELOPER_DB.ANJALI_SCHEMA.store_proc1
  
   as (
    { config(materialized='procedure')}}

create or replace procedure "DEVELOPER_DB"."ANJALI_SCHEMA"."sv_proc2"(PARAMETER_1 FLOAT)
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    AS
    $$
        var rs = snowflake.execute( {sqlText: "SELECT 2 * " + PARAMETER_1} );
        rs.next();
        var MyString = rs.getColumnValue(1);
        return MyString;
    $$
    ;
  );
