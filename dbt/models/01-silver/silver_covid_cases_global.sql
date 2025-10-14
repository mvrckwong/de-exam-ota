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

{{ dbt_utils.union_relations(
    relations=relations,
    source_column_name='_source_table'
) }}