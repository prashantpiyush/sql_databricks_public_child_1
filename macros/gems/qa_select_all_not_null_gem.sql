{% macro qa_select_all_not_null_gem(model, col) %}

select * from {{model}} where {{col}} is not null
{% endmacro %}

 {% macro combine_multiple_tables(table_1, table_2, table_3, table_4, table_5, col_table_1) %}

select * from {{table_1}} 
    where {{col_table_1}} != (select count(*) from {{table_2}} ) + 
        (select count(*) from {{table_3}} ) + 
            (select count(*) from {{table_4}} ) + 
                (select count(*) from {{table_5}} )
{% endmacro %}

 