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
    "fips" AS fips_code,
    NULLIF(TRIM("province_state"), '') AS province_state,
    COALESCE(NULLIF(TRIM("country_region"), ''), 'US') AS country_region,
    
    -- Unique Identifiers
    "uid" AS uid,
    NULLIF(TRIM("iso3"), '') AS iso3_code,
    
    -- Geographic Coordinates
    NULLIF("lat", 0) AS latitude,
    NULLIF("long_", 0) AS longitude,
    
    -- Case Metrics
    COALESCE("confirmed", 0) AS confirmed_cases,
    COALESCE("deaths", 0) AS total_deaths,
    COALESCE("recovered", 0) AS total_recovered,
    COALESCE("active", 0) AS active_cases,
    
    -- Testing Metrics
    COALESCE("total_test_results", 0) AS total_test_results,
    COALESCE("people_tested", 0) AS people_tested,
    
    -- Hospitalization Metrics
    COALESCE("people_hospitalized", 0) AS people_hospitalized,
    
    -- Rate Metrics (validate ranges)
    CASE 
        WHEN "incident_rate" < 0 THEN NULL 
        ELSE "incident_rate" 
    END AS incident_rate_per_100k,
    
    CASE 
        WHEN "case_fatality_ratio" < 0 OR "case_fatality_ratio" > 100 THEN NULL 
        ELSE "case_fatality_ratio" 
    END AS case_fatality_ratio_pct,
    
    CASE 
        WHEN "testing_rate" < 0 THEN NULL 
        ELSE "testing_rate" 
    END AS testing_rate_per_100k,
    
    CASE 
        WHEN "hospitalization_rate" < 0 OR "hospitalization_rate" > 100 THEN NULL 
        ELSE "hospitalization_rate" 
    END AS hospitalization_rate_pct,
    
    CASE 
        WHEN "mortality_rate" < 0 OR "mortality_rate" > 100 THEN NULL 
        ELSE "mortality_rate" 
    END AS mortality_rate_pct,
    
    -- Timestamp Fields
    CASE 
        WHEN "last_update" IS NOT NULL 
        THEN ("last_update" AT TIME ZONE 'UTC')
        ELSE NULL 
    END AS last_updated_at,
    
    -- Date field (for daily snapshot)
    "date"::DATE AS snapshot_date,
    
    -- Lineage and Metadata
    CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS _staged_at,
    REPLACE(REPLACE(REPLACE(_source, '"warehouse".', ''), '""', '"'), '"', '') AS _source_table
    
FROM 
    transformed