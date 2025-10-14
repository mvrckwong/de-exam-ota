{{
    config(
        materialized='table',
        tags=['gold', 'aggregation', 'daily', 'from_fact']
    )
}}

/*
    Aggregation Model: Daily Summary Statistics
    
    Purpose: Provides high-level daily aggregated metrics across all US states
    for trend analysis and executive reporting.
    
    Built from: fct_covid_daily (fact table)
    This ensures consistency with all other gold models and leverages pre-calculated metrics.
*/

with daily_us_totals as (
    select
        report_date as snapshot_date,
        sum(confirmed_cases) as total_confirmed_cases,
        sum(total_deaths) as total_deaths,
        sum(total_recovered) as total_recovered,
        sum(active_cases) as total_active_cases,
        sum(people_tested) as total_people_tested,
        sum(people_hospitalized) as total_people_hospitalized,
        avg(incident_rate_per_100k) as avg_incident_rate_per_100k,
        avg(case_fatality_ratio_pct) as avg_case_fatality_ratio_pct,
        avg(testing_rate_per_100k) as avg_testing_rate_per_100k,
        avg(hospitalization_rate_pct) as avg_hospitalization_rate_pct,
        avg(mortality_rate_pct) as avg_mortality_rate_pct,
        count(distinct province_state) as states_reporting
    from {{ ref('fct_covid_daily') }}
    where data_source = 'US'
        and report_date is not null
    group by report_date
),

with_derived_metrics as (
    select
        snapshot_date,
        total_confirmed_cases,
        total_deaths,
        total_recovered,
        total_active_cases,
        total_people_tested,
        total_people_hospitalized,
        avg_incident_rate_per_100k,
        avg_case_fatality_ratio_pct,
        avg_testing_rate_per_100k,
        avg_hospitalization_rate_pct,
        avg_mortality_rate_pct,
        states_reporting,
        
        -- Calculate daily new cases and deaths
        lag(total_confirmed_cases) over (order by snapshot_date) as prev_day_total_cases,
        lag(total_deaths) over (order by snapshot_date) as prev_day_total_deaths,
        
        -- Calculate 7-day rolling averages
        avg(total_confirmed_cases) over (
            order by snapshot_date 
            rows between 6 preceding and current row
        ) as cases_7day_rolling_avg,
        
        avg(total_deaths) over (
            order by snapshot_date 
            rows between 6 preceding and current row
        ) as deaths_7day_rolling_avg
    from daily_us_totals
)

select
    snapshot_date,
    
    -- Total cumulative metrics
    total_confirmed_cases,
    total_deaths,
    total_recovered,
    total_active_cases,
    total_people_tested,
    total_people_hospitalized,
    
    -- Daily new metrics
    coalesce(total_confirmed_cases - prev_day_total_cases, 0) as new_cases_today,
    coalesce(total_deaths - prev_day_total_deaths, 0) as new_deaths_today,
    
    -- Rolling averages
    round(cases_7day_rolling_avg::numeric, 2) as cases_7day_rolling_avg,
    round(deaths_7day_rolling_avg::numeric, 2) as deaths_7day_rolling_avg,
    
    -- Average rates across states
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
    
    states_reporting,
    
    -- Trend indicators
    case
        when total_confirmed_cases - prev_day_total_cases > 0 then 'Rising'
        when total_confirmed_cases - prev_day_total_cases < 0 then 'Falling'
        else 'Stable'
    end as case_trend

from with_derived_metrics
order by snapshot_date desc

