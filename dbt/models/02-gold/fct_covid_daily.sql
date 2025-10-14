{{
    config(
        materialized='table',
        tags=['gold', 'fact', 'core']
    )
}}

/*
    Fact Table: Daily COVID-19 Metrics
    
    Purpose: Unified fact table combining US and global COVID-19 data at daily grain.
    This table serves as the single source of truth for analytical queries and supports
    all three analytical questions:
    
    1. Top values analysis - Aggregatable by country/state
    2. Time series analysis - Daily grain with snapshot dates
    3. Correlation analysis - Contains all key metrics for statistical analysis
    
    Grain: One row per location per day (where applicable)
    - US data: state/province + date
    - Global data: country/province + latest snapshot
*/

with us_data as (
    select
        -- Dimensional attributes
        'US' as data_source,
        country_region,
        province_state,
        null::text as admin2,
        fips_code,
        iso3_code,
        uid,
        snapshot_date as report_date,
        
        -- Geographic coordinates
        latitude,
        longitude,
        
        -- Case metrics
        confirmed_cases,
        total_deaths,
        total_recovered,
        active_cases,
        
        -- US-specific metrics
        total_test_results,
        people_tested,
        people_hospitalized,
        
        -- Rate metrics
        incident_rate_per_100k,
        case_fatality_ratio_pct,
        testing_rate_per_100k,
        hospitalization_rate_pct,
        mortality_rate_pct,
        
        -- Metadata
        last_updated_at,
        _staged_at,
        _source_table
    from {{ ref('silver_covid_cases_us') }}
    where snapshot_date is not null
),

global_data as (
    select
        -- Dimensional attributes
        'Global' as data_source,
        country_region,
        province_state,
        admin2,
        fips_code,
        null::text as iso3_code,
        null::bigint as uid,
        -- For global data without explicit dates, use last_updated_at date
        date(last_updated_at) as report_date,
        
        -- Geographic coordinates
        latitude,
        longitude,
        
        -- Case metrics
        confirmed_cases,
        total_deaths,
        total_recovered,
        active_cases,
        
        -- US-specific metrics (NULL for global data)
        null::bigint as total_test_results,
        null::bigint as people_tested,
        null::bigint as people_hospitalized,
        
        -- Rate metrics
        incident_rate_per_100k,
        case_fatality_ratio_pct,
        null::double precision as testing_rate_per_100k,
        null::double precision as hospitalization_rate_pct,
        null::double precision as mortality_rate_pct,
        
        -- Metadata
        last_updated_at,
        _staged_at,
        _source_table
    from {{ ref('silver_covid_cases_global') }}
    -- Exclude US data from global to avoid duplication
    where country_region != 'US'
        and country_region is not null
        and country_region != 'Unknown'
),

combined_data as (
    select * from us_data
    union all
    select * from global_data
),

with_derived_metrics as (
    select
        -- Generate surrogate key for unique identification
        {{ dbt_utils.generate_surrogate_key([
            'data_source',
            'country_region',
            'coalesce(province_state, \'\')',
            'coalesce(admin2, \'\')',
            'report_date'
        ]) }} as fact_key,
        
        -- Dimensional attributes
        data_source,
        country_region,
        province_state,
        admin2,
        coalesce(
            province_state || ', ' || country_region,
            country_region
        ) as location_name,
        fips_code,
        iso3_code,
        uid,
        report_date,
        extract(year from report_date) as report_year,
        extract(month from report_date) as report_month,
        extract(day from report_date) as report_day,
        to_char(report_date, 'Day') as day_of_week,
        extract(week from report_date) as week_of_year,
        
        -- Geographic coordinates
        latitude,
        longitude,
        
        -- Core case metrics
        confirmed_cases,
        total_deaths,
        total_recovered,
        active_cases,
        
        -- Testing metrics
        total_test_results,
        people_tested,
        people_hospitalized,
        
        -- Rate metrics (per 100k population)
        incident_rate_per_100k,
        testing_rate_per_100k,
        
        -- Percentage metrics
        case_fatality_ratio_pct,
        hospitalization_rate_pct,
        mortality_rate_pct,
        
        -- Derived metrics: Calculate additional KPIs
        case
            when confirmed_cases > 0 then
                round((100.0 * total_deaths / confirmed_cases)::numeric, 2)
            else null
        end as calculated_cfr_pct,
        
        case
            when confirmed_cases > 0 then
                round((100.0 * total_recovered / confirmed_cases)::numeric, 2)
            else null
        end as recovery_rate_pct,
        
        case
            when people_tested > 0 then
                round((100.0 * confirmed_cases / people_tested)::numeric, 2)
            else null
        end as test_positivity_rate_pct,
        
        case
            when confirmed_cases > 0 then
                round((100.0 * people_hospitalized / confirmed_cases)::numeric, 2)
            else null
        end as hospitalization_to_case_ratio_pct,
        
        -- Day-over-day calculations (for time series analysis)
        lag(confirmed_cases) over (
            partition by data_source, country_region, province_state, admin2
            order by report_date
        ) as prev_day_confirmed_cases,
        
        lag(total_deaths) over (
            partition by data_source, country_region, province_state, admin2
            order by report_date
        ) as prev_day_deaths,
        
        lag(people_tested) over (
            partition by data_source, country_region, province_state, admin2
            order by report_date
        ) as prev_day_people_tested,
        
        -- Rolling averages (for trend analysis)
        avg(confirmed_cases) over (
            partition by data_source, country_region, province_state, admin2
            order by report_date
            rows between 6 preceding and current row
        ) as confirmed_cases_7day_avg,
        
        avg(total_deaths) over (
            partition by data_source, country_region, province_state, admin2
            order by report_date
            rows between 6 preceding and current row
        ) as deaths_7day_avg,
        
        -- Metadata
        last_updated_at,
        _staged_at,
        _source_table,
        current_timestamp as _fact_created_at
    from combined_data
),

final as (
    select
        fact_key,
        
        -- Dimensional attributes
        data_source,
        country_region,
        province_state,
        admin2,
        location_name,
        fips_code,
        iso3_code,
        uid,
        
        -- Date dimensions
        report_date,
        report_year,
        report_month,
        report_day,
        day_of_week,
        week_of_year,
        
        -- Geographic coordinates
        latitude,
        longitude,
        
        -- Core metrics (additive facts)
        confirmed_cases,
        total_deaths,
        total_recovered,
        active_cases,
        total_test_results,
        people_tested,
        people_hospitalized,
        
        -- Daily changes (for trend analysis)
        coalesce(confirmed_cases - prev_day_confirmed_cases, 0) as daily_new_cases,
        coalesce(total_deaths - prev_day_deaths, 0) as daily_new_deaths,
        coalesce(people_tested - prev_day_people_tested, 0) as daily_new_tests,
        
        -- Percentage change metrics
        case
            when prev_day_confirmed_cases > 0 then
                round((100.0 * (confirmed_cases - prev_day_confirmed_cases) / prev_day_confirmed_cases)::numeric, 2)
            else null
        end as pct_change_cases,
        
        case
            when prev_day_deaths > 0 then
                round((100.0 * (total_deaths - prev_day_deaths) / prev_day_deaths)::numeric, 2)
            else null
        end as pct_change_deaths,
        
        -- Rolling averages (semi-additive facts)
        round(confirmed_cases_7day_avg::numeric, 2) as confirmed_cases_7day_avg,
        round(deaths_7day_avg::numeric, 2) as deaths_7day_avg,
        
        -- Rate metrics (non-additive facts)
        incident_rate_per_100k,
        testing_rate_per_100k,
        case_fatality_ratio_pct,
        hospitalization_rate_pct,
        mortality_rate_pct,
        
        -- Derived percentage metrics
        calculated_cfr_pct,
        recovery_rate_pct,
        test_positivity_rate_pct,
        hospitalization_to_case_ratio_pct,
        
        -- Flags and indicators
        case when coalesce(confirmed_cases - prev_day_confirmed_cases, 0) > 0 then true else false end as has_new_cases,
        case when coalesce(total_deaths - prev_day_deaths, 0) > 0 then true else false end as has_new_deaths,
        case
            when confirmed_cases > prev_day_confirmed_cases then 'Increasing'
            when confirmed_cases < prev_day_confirmed_cases then 'Decreasing'
            when confirmed_cases = prev_day_confirmed_cases then 'Stable'
            else 'Unknown'
        end as case_trend,
        
        -- Severity classification (for top values analysis)
        case
            when confirmed_cases >= 1000000 then 'Critical'
            when confirmed_cases >= 500000 then 'Very High'
            when confirmed_cases >= 100000 then 'High'
            when confirmed_cases >= 10000 then 'Moderate'
            when confirmed_cases >= 1000 then 'Low'
            else 'Very Low'
        end as severity_category,
        
        -- Metadata
        last_updated_at,
        _staged_at,
        _source_table,
        _fact_created_at
        
    from with_derived_metrics
)

select * from final

