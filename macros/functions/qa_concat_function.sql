{% macro qa_concat_function_main(param_1, param_2='hello') %}
concat({{param_1}}, {{param_2}})
{% endmacro %}

 