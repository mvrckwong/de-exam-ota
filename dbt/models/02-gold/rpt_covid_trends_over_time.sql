{{
    config(
        materialized='table',
        tags=['gold', 'report', 'time_series', 'from_fact']
    )
}}

/*
    Report Model: COVID-19 Trends Over Time
    
    Purpose: Answers the question - "How does a particular metric change 
    over time within the dataset?"
    
    This model tracks the progression of COVID-19 metrics over time at the daily level.
    Leverages pre-calculated metrics from fct_covid_daily for better performance.
    
    Built from: fct_covid_daily (fact table)
    This ensures consistency with all other gold models and leverages pre-calculated metrics.
    For exploration and custom analysis with complex transformations.
*/

with daily_metrics as (
    select
        report_date as snapshot_date,
        province_state,
        confirmed_cases,
        total_deaths,
        people_tested,
        people_hospitalized,
        testing_rate_per_100k,
        hospitalization_rate_pct,
        mortality_rate_pct,
        incident_rate_per_100k,
        -- Use pre-calculated metrics from fact table
        daily_new_cases,
        daily_new_deaths,
        pct_change_cases,
        pct_change_deaths,
        confirmed_cases_7day_avg,
        deaths_7day_avg,
        case_trend
    from {{ ref('fct_covid_daily') }}
    where data_source = 'US'
        and report_date is not null
        and province_state is not null
),

daily_aggregated as (
    select
        snapshot_date,
        province_state,
        sum(confirmed_cases) as daily_confirmed_cases,
        sum(total_deaths) as daily_deaths,
        sum(people_tested) as daily_people_tested,
        sum(people_hospitalized) as daily_people_hospitalized,
        avg(testing_rate_per_100k) as avg_testing_rate_per_100k,
        avg(hospitalization_rate_pct) as avg_hospitalization_rate_pct,
        avg(mortality_rate_pct) as avg_mortality_rate_pct,
        avg(incident_rate_per_100k) as avg_incident_rate_per_100k,
        -- Use pre-calculated metrics from fact table
        sum(daily_new_cases) as new_cases_vs_prev_day,
        sum(daily_new_deaths) as new_deaths_vs_prev_day,
        avg(pct_change_cases) as pct_change_cases,
        avg(pct_change_deaths) as pct_change_deaths,
        avg(confirmed_cases_7day_avg) as cases_7day_avg,
        avg(deaths_7day_avg) as deaths_7day_avg,
        -- Use trend from fact table
        max(case_trend) as trend_direction
    from daily_metrics
    group by snapshot_date, province_state
)

select
    snapshot_date,
    province_state,
    daily_confirmed_cases,
    daily_deaths,
    daily_people_tested,
    daily_people_hospitalized,
    
    -- Pre-calculated day-over-day changes from fact table
    new_cases_vs_prev_day,
    new_deaths_vs_prev_day,
    
    -- Pre-calculated percentage changes from fact table
    round(pct_change_cases::numeric, 2) as pct_change_cases,
    round(pct_change_deaths::numeric, 2) as pct_change_deaths,
    
    -- Pre-calculated smoothed metrics from fact table
    round(cases_7day_avg::numeric, 2) as cases_7day_moving_avg,
    round(deaths_7day_avg::numeric, 2) as deaths_7day_moving_avg,
    
    -- Rates
    round(avg_testing_rate_per_100k::numeric, 2) as avg_testing_rate_per_100k,
    round(avg_hospitalization_rate_pct::numeric, 2) as avg_hospitalization_rate_pct,
    round(avg_mortality_rate_pct::numeric, 2) as avg_mortality_rate_pct,
    round(avg_incident_rate_per_100k::numeric, 2) as avg_incident_rate_per_100k,
    
    -- Pre-calculated trend indicator from fact table
    trend_direction
    
from daily_aggregated
order by province_state, snapshot_date