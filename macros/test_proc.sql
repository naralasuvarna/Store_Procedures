{% macro test_proc(msg) %}
create or replace procedure "DEVELOPER_DB"."ANJALI_SCHEMA".sv_proc3(PARAMETER_1 FLOAT)
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
{% endmacro %}