{% macro output_message(msg) %}
create or replace procedure output_message(message varchar)
returns varchar not null
language sql
as
begin
  return message;
end;

call output_message(msg);
{% endmacro %}