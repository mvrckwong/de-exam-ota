{{
    config(
        materialized='table',
        tags=['gold', 'aggregation', 'country']
    )
}}

/*
    Aggregation Model: Country-Level Summary Statistics
    
    Purpose: Provides comprehensive country-level aggregated metrics from
    global data for international comparative analysis.
*/

with country_totals as (
    select
        country_region,
        count(*) as total_records,
        count(distinct province_state) as provinces_reporting,
        count(distinct location_key) as unique_locations,
        sum(confirmed_cases) as total_confirmed_cases,
        sum(total_deaths) as total_deaths,
        sum(total_recovered) as total_recovered,
        sum(active_cases) as total_active_cases,
        avg(incident_rate_per_100k) as avg_incident_rate_per_100k,
        avg(case_fatality_ratio_pct) as avg_case_fatality_ratio_pct,
        max(confirmed_cases) as max_single_location_cases,
        max(total_deaths) as max_single_location_deaths
    from {{ ref('silver_covid_cases_global') }}
    where country_region is not null
        and country_region != 'Unknown'
    group by country_region
),

ranked_countries as (
    select
        country_region,
        total_records,
        provinces_reporting,
        unique_locations,
        total_confirmed_cases,
        total_deaths,
        total_recovered,
        total_active_cases,
        avg_incident_rate_per_100k,
        avg_case_fatality_ratio_pct,
        max_single_location_cases,
        max_single_location_deaths,
        
        -- Global rankings
        row_number() over (order by total_confirmed_cases desc) as global_rank_cases,
        row_number() over (order by total_deaths desc) as global_rank_deaths,
        row_number() over (order by total_recovered desc) as global_rank_recovered,
        
        -- Calculate share of global totals
        round(
            (100.0 * total_confirmed_cases / sum(total_confirmed_cases) over ())::numeric,
            2
        ) as pct_of_global_cases,
        
        round(
            (100.0 * total_deaths / sum(total_deaths) over ())::numeric,
            2
        ) as pct_of_global_deaths
    from country_totals
)

select
    country_region,
    total_records as data_points,
    provinces_reporting,
    unique_locations,
    
    -- Total metrics
    total_confirmed_cases,
    total_deaths,
    total_recovered,
    total_active_cases,
    
    -- Global rankings
    global_rank_cases,
    global_rank_deaths,
    global_rank_recovered,
    
    -- Global share
    pct_of_global_cases,
    pct_of_global_deaths,
    
    -- Average rates
    round(avg_incident_rate_per_100k::numeric, 2) as avg_incident_rate_per_100k,
    round(avg_case_fatality_ratio_pct::numeric, 2) as avg_case_fatality_ratio_pct,
    
    -- Derived metrics
    case
        when total_confirmed_cases > 0 then
            round((100.0 * total_deaths / total_confirmed_cases)::numeric, 2)
        else null
    end as overall_case_fatality_ratio_pct,
    
    case
        when total_confirmed_cases > 0 then
            round((100.0 * total_recovered / total_confirmed_cases)::numeric, 2)
        else null
    end as recovery_rate_pct,
    
    -- Peak values
    max_single_location_cases,
    max_single_location_deaths,
    
    -- Impact classification
    case
        when global_rank_cases <= 10 then 'Top 10 Globally'
        when global_rank_cases <= 25 then 'Top 25 Globally'
        when global_rank_cases <= 50 then 'Top 50 Globally'
        else 'Other'
    end as global_impact_tier,
    
    -- Severity based on case fatality ratio
    case
        when avg_case_fatality_ratio_pct >= 5.0 then 'Very High Severity'
        when avg_case_fatality_ratio_pct >= 3.0 then 'High Severity'
        when avg_case_fatality_ratio_pct >= 2.0 then 'Moderate Severity'
        when avg_case_fatality_ratio_pct >= 1.0 then 'Low Severity'
        else 'Very Low Severity'
    end as severity_classification

from ranked_countries
order by total_confirmed_cases desc

