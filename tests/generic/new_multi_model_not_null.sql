{% test new_multi_model_not_null(model, model1, column_name_model_1, column_name_model_2, column_name_mode1_1, column_name_mode1_2, value1=-10, value2=100) %}


select * from {{ model }} where {{ column_name_model_1 }} is null or {{ column_name_model_2 }} is null and {{column_name_model_1}} <= (select count(*) from {{model1}} where {{column_name_mode1_1}} is not null ) 
and {{column_name_model_2}} <= (select count(*) from {{model1}} where {{column_name_mode1_2}} is not null ) and {{column_name_model_1}} between {{value1}} and {{value2}}
{% endtest %}

 