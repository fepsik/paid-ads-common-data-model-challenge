# Marketing common data modelling challenge

## How to add new data

    1. Add stg model and change column names / add calculated columns according to MCDM structure
    2. Add new piece of code for this datasource into ads_basic_performance

### Sample code to add into ads_basic_performance

```
{%- set datasource_columns = adapter.get_columns_in_relation(
    ref('src_datasource_model')
) -%}
{%- set datasource_columns_list = [] -%}
{%- for column in datasource_columns -%}
    {%- do datasource_columns_list.append(column.name) -%}
{%- endfor -%} 
...
UNION ALL
SELECT 
            {%- for col in mcdm_columns -%}
            {% if col[0] in datasource_columns_list %}
            CAST({{ col[0] }} as {{col[1]}}) as {{col[0]}},
            {% else %}
            CAST(
                {% if col[1]=='int64' %}
                0
                {% else %}
                ''
                {% endif%}
                as {{col[1]}}) as {{ col[0] }},
            {% endif %}
            {% endfor %}
FROM        {{ ref('stg_datasource_model') }}
...
```

## How it works

It's checking columns in source/staging model and if field is presented in mcdm_structure then field will be taken and if not presented it will be filled with '' or 0.
This way you can not to worry about column order in source/staging model to union it.