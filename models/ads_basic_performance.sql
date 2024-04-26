{%- set bing_columns = adapter.get_columns_in_relation(
    ref('stg_ads_bing')
) -%}
{%- set bing_columns_list = [] -%}
{%- for column in bing_columns -%}
    {%- do bing_columns_list.append(column.name) -%}
{%- endfor -%}

{%- set facebook_columns = adapter.get_columns_in_relation(
    ref('stg_ads_creative_facebook')
) -%}
{%- set facebook_columns_list = [] -%}
{%- for column in facebook_columns -%}
    {%- do facebook_columns_list.append(column.name) -%}
{%- endfor -%}

{%- set tiktok_columns = adapter.get_columns_in_relation(
    ref('stg_ads_tiktok')
) -%}
{%- set tiktok_columns_list = [] -%}
{%- for column in tiktok_columns -%}
    {%- do tiktok_columns_list.append(column.name) -%}
{%- endfor -%}

{%- set twitter_columns = adapter.get_columns_in_relation(
    ref('src_promoted_tweets_twitter_all_data')
) -%}
{%- set twitter_columns_list = [] -%}
{%- for column in twitter_columns -%}
    {%- do twitter_columns_list.append(column.name) -%}
{%- endfor -%}



{%
    set mcdm_columns = run_query('select mcdm_field_name, mcdm_field_value_type from '~ref("mcdm_paid_ads_basic_performance_structure"))
%}

SELECT 
            {%- for col in mcdm_columns -%}
            {% if col[0] in bing_columns_list %}
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
FROM        {{ ref('stg_ads_bing') }}
UNION ALL
SELECT 
            {%- for col in mcdm_columns -%}
            {% if col[0] in facebook_columns_list %}
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
FROM        {{ ref('stg_ads_creative_facebook') }}
UNION ALL
SELECT 
            {%- for col in mcdm_columns -%}
            {% if col[0] in tiktok_columns_list %}
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
FROM        {{ ref('stg_ads_tiktok') }}
UNION ALL
SELECT 
            {%- for col in mcdm_columns -%}
            {% if col[0] in twitter_columns_list %}
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
FROM        {{ ref('src_promoted_tweets_twitter_all_data') }}

