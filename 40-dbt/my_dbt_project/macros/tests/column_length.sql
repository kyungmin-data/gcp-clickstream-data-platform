{% test column_length(model, column_name, min_length=1, max_length=255) %}

select *
from {{ model }}
where 
    length({{ column_name }}) < {{ min_length }}
    or length({{ column_name }}) > {{ max_length }}

{% endtest %}