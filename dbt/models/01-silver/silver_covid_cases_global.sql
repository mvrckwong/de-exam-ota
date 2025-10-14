{{
    config(
        materialized='table'
    )
}}

-- Dynamically union all bronze tables that start with "global_"
{% set relations = dbt_utils.get_relations_by_pattern(
    schema_pattern='bronze',
    table_pattern='global_%'
) %}

with unioned_data as (
    {{ dbt_utils.union_relations(
        relations=relations,
        source_column_name='_source'
    ) }}
),

final as (
    select
        {% set columns = adapter.get_columns_in_relation(relations[0]) %}
        {% for column in columns %}
        {{ column.name }} as {{ column.name | lower }}{{ "," if not loop.last }}
        {% endfor %},
        _source
    from unioned_data
)

select * from final