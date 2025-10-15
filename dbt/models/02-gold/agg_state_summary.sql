{{
    config(
        materialized='table',
        tags=['agg']
    )
}}

/*
    Aggregation Model: State-Level Summary Statistics
    
    Purpose: Provides comprehensive state-level aggregated metrics for
    comparative analysis across US states.
    
    Built from: fct_covid_daily (fact table)
    This ensures consistency with all other gold models and leverages pre-calculated metrics.
*/

with state_totals as (
    select
        province_state,
        count(distinct report_date) as days_reporting,
        max(report_date) as latest_report_date,
        max(confirmed_cases) as total_confirmed_cases,
        max(total_deaths) as total_deaths,
        max(total_recovered) as total_recovered,
        max(people_tested) as total_people_tested,
        max(people_hospitalized) as total_people_hospitalized,
        avg(incident_rate_per_100k) as avg_incident_rate_per_100k,
        avg(case_fatality_ratio_pct) as avg_case_fatality_ratio_pct,
        avg(testing_rate_per_100k) as avg_testing_rate_per_100k,
        avg(hospitalization_rate_pct) as avg_hospitalization_rate_pct,
        avg(mortality_rate_pct) as avg_mortality_rate_pct,
        
        -- Calculate peak values
        max(confirmed_cases) as peak_confirmed_cases,
        max(total_deaths) as peak_deaths
    from {{ ref('fct_covid_daily') }}
    where data_source = 'US'
        and province_state is not null
    group by province_state
),

ranked_states as (
    select
        province_state,
        days_reporting,
        latest_report_date,
        total_confirmed_cases,
        total_deaths,
        total_recovered,
        total_people_tested,
        total_people_hospitalized,
        avg_incident_rate_per_100k,
        avg_case_fatality_ratio_pct,
        avg_testing_rate_per_100k,
        avg_hospitalization_rate_pct,
        avg_mortality_rate_pct,
        peak_confirmed_cases,
        peak_deaths,
        
        -- Rankings
        row_number() over (order by total_confirmed_cases desc) as rank_by_cases,
        row_number() over (order by total_deaths desc) as rank_by_deaths,
        row_number() over (order by avg_incident_rate_per_100k desc) as rank_by_incident_rate,
        
        -- Calculate percentiles
        percent_rank() over (order by total_confirmed_cases) as cases_percentile,
        percent_rank() over (order by total_deaths) as deaths_percentile
    from state_totals
)

select
    province_state,
    days_reporting,
    latest_report_date,
    
    -- Total metrics
    total_confirmed_cases,
    total_deaths,
    total_recovered,
    total_people_tested,
    total_people_hospitalized,
    
    -- Rankings
    rank_by_cases as national_rank_cases,
    rank_by_deaths as national_rank_deaths,
    rank_by_incident_rate as national_rank_incident_rate,
    
    -- Percentiles (0-1 scale)
    round(cases_percentile::numeric, 4) as cases_percentile,
    round(deaths_percentile::numeric, 4) as deaths_percentile,
    
    -- Average rates
    round(avg_incident_rate_per_100k::numeric, 2) as avg_incident_rate_per_100k,
    round(avg_case_fatality_ratio_pct::numeric, 2) as avg_case_fatality_ratio_pct,
    round(avg_testing_rate_per_100k::numeric, 2) as avg_testing_rate_per_100k,
    round(avg_hospitalization_rate_pct::numeric, 2) as avg_hospitalization_rate_pct,
    round(avg_mortality_rate_pct::numeric, 2) as avg_mortality_rate_pct,
    
    -- Derived metrics
    case
        when total_confirmed_cases > 0 then
            round((100.0 * total_deaths / total_confirmed_cases)::numeric, 2)
        else null
    end as overall_case_fatality_ratio_pct,
    
    case
        when total_people_tested > 0 then
            round((100.0 * total_confirmed_cases / total_people_tested)::numeric, 2)
        else null
    end as test_positivity_rate_pct,
    
    -- Peak metrics
    peak_confirmed_cases,
    peak_deaths,
    
    -- Severity classification
    case
        when rank_by_cases <= 5 then 'Top 5 - Critical'
        when rank_by_cases <= 10 then 'Top 10 - Severe'
        when rank_by_cases <= 20 then 'Top 20 - High'
        when rank_by_cases <= 30 then 'Moderate'
        else 'Lower Impact'
    end as impact_category

from ranked_states
order by total_confirmed_cases desc

