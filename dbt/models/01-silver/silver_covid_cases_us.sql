{{
    config(
        materialized='table'
    )
}}

-- Dynamically union all bronze tables that start with "us_"
{% set relations = dbt_utils.get_relations_by_pattern(
    schema_pattern='bronze',
    table_pattern='us_%'
) %}

with unioned_data as (
    {{ dbt_utils.union_relations(
        relations=relations,
        source_column_name='_source'
    ) }}
)
SELECT * FROM unioned_data