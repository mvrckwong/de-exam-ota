{{
    config(
        materialized='table'
    )
}}

-- Dynamically union all bronze tables (seeds) without listing them individually
{% set relations = dbt_utils.get_relations_by_pattern(
    schema_pattern='bronze',
    table_pattern='%'
) %}

{{ dbt_utils.union_relations(
    relations=relations,
    source_column_name='_source_table'
) }}