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

-- Get all unique column names from all relations
{% set all_columns = [] %}
{% for relation in relations %}
    {% set relation_columns = adapter.get_columns_in_relation(relation) %}
    {% for column in relation_columns %}
        {% if column.name not in all_columns %}
            {% do all_columns.append(column.name) %}
        {% endif %}
    {% endfor %}
{% endfor %}


with source as (
    {{ dbt_utils.union_relations(
        relations=relations,
        source_column_name='_source',
        column_override=none
    ) }}
),

transformed as (
    select
        {% for column in all_columns %}
        "{{ column }}" as "{{ column | lower }}"{{ "," if not loop.last else "" }}
        {% endfor %}
        {% if all_columns %},{% endif %}
        _source
    from source
)

SELECT
    -- Geographic Identifiers
    -- FIPS is numeric, NULL handling is automatic
    "fips" AS fips_code,
    
    -- Text fields: convert empty strings to NULL, trim whitespace
    NULLIF(TRIM("admin2"), '') AS admin2,
    NULLIF(TRIM("province_state"), '') AS province_state,
    COALESCE(NULLIF(TRIM("country_region"), ''), 'Unknown') AS country_region,
    NULLIF(TRIM("combined_key"), '') AS location_key,
    
    -- Geographic Coordinates
    -- Use NULLIF to handle 0 coordinates that might be placeholders
    NULLIF("lat", 0) AS latitude,
    NULLIF("long_", 0) AS longitude,
    
    -- Case Metrics
    -- Ensure non-negative values, coalesce to 0 for NULL
    COALESCE("confirmed", 0) AS confirmed_cases,
    COALESCE("deaths", 0) AS total_deaths,
    COALESCE("recovered", 0) AS total_recovered,
    COALESCE("active", 0) AS active_cases,
    
    -- Rate Metrics
    -- Allow NULL for missing/invalid rates
    CASE 
        WHEN "incident_rate" < 0 THEN NULL 
        ELSE "incident_rate" 
    END AS incident_rate_per_100k,
    
    CASE 
        WHEN "case_fatality_ratio" < 0 OR "case_fatality_ratio" > 100 THEN NULL 
        ELSE "case_fatality_ratio" 
    END AS case_fatality_ratio_pct,
    
    -- Timestamp Fields
    -- Add UTC timezone to last_update
    CASE 
        WHEN "last_update" IS NOT NULL 
        THEN ("last_update" AT TIME ZONE 'UTC')
        ELSE NULL 
    END AS last_updated_at,
    
    -- Lineage and Metadata
    CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS _staged_at,
    REPLACE(REPLACE(_source, '""', '"'), '"warehouse"', 'warehouse') AS _source_table
    
FROM 
    transformed