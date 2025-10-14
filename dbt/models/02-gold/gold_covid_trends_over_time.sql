{{
    config(
        materialized='table',
        tags=['gold', 'analytics', 'time_series']
    )
}}

/*
    Gold Model: COVID-19 Trends Over Time
    
    Purpose: Answers the question - "How does a particular metric change 
    over time within the dataset?"
    
    This model tracks the progression of key COVID-19 metrics (confirmed cases,
    deaths, hospitalization, testing) over time at the daily level for US states.
*/

with daily_metrics as (
    select
        snapshot_date,
        province_state,
        confirmed_cases,
        total_deaths,
        people_tested,
        people_hospitalized,
        testing_rate_per_100k,
        hospitalization_rate_pct,
        mortality_rate_pct,
        incident_rate_per_100k
    from {{ ref('silver_covid_cases_us') }}
    where snapshot_date is not null
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
        avg(incident_rate_per_100k) as avg_incident_rate_per_100k
    from daily_metrics
    group by snapshot_date, province_state
),

with_changes as (
    select
        snapshot_date,
        province_state,
        daily_confirmed_cases,
        daily_deaths,
        daily_people_tested,
        daily_people_hospitalized,
        avg_testing_rate_per_100k,
        avg_hospitalization_rate_pct,
        avg_mortality_rate_pct,
        avg_incident_rate_per_100k,
        
        -- Calculate day-over-day changes
        lag(daily_confirmed_cases) over (
            partition by province_state 
            order by snapshot_date
        ) as prev_day_cases,
        
        lag(daily_deaths) over (
            partition by province_state 
            order by snapshot_date
        ) as prev_day_deaths,
        
        -- Calculate 7-day moving averages for smoothing
        avg(daily_confirmed_cases) over (
            partition by province_state 
            order by snapshot_date 
            rows between 6 preceding and current row
        ) as cases_7day_avg,
        
        avg(daily_deaths) over (
            partition by province_state 
            order by snapshot_date 
            rows between 6 preceding and current row
        ) as deaths_7day_avg
    from daily_aggregated
)

select
    snapshot_date,
    province_state,
    daily_confirmed_cases,
    daily_deaths,
    daily_people_tested,
    daily_people_hospitalized,
    
    -- Day-over-day changes
    coalesce(daily_confirmed_cases - prev_day_cases, 0) as new_cases_vs_prev_day,
    coalesce(daily_deaths - prev_day_deaths, 0) as new_deaths_vs_prev_day,
    
    -- Percentage changes
    case
        when prev_day_cases > 0 then
            round((100.0 * (daily_confirmed_cases - prev_day_cases) / prev_day_cases)::numeric, 2)
        else null
    end as pct_change_cases,
    
    case
        when prev_day_deaths > 0 then
            round((100.0 * (daily_deaths - prev_day_deaths) / prev_day_deaths)::numeric, 2)
        else null
    end as pct_change_deaths,
    
    -- Smoothed metrics
    round(cases_7day_avg::numeric, 2) as cases_7day_moving_avg,
    round(deaths_7day_avg::numeric, 2) as deaths_7day_moving_avg,
    
    -- Rates
    round(avg_testing_rate_per_100k::numeric, 2) as avg_testing_rate_per_100k,
    round(avg_hospitalization_rate_pct::numeric, 2) as avg_hospitalization_rate_pct,
    round(avg_mortality_rate_pct::numeric, 2) as avg_mortality_rate_pct,
    round(avg_incident_rate_per_100k::numeric, 2) as avg_incident_rate_per_100k,
    
    -- Trend indicators
    case
        when daily_confirmed_cases > prev_day_cases then 'Increasing'
        when daily_confirmed_cases < prev_day_cases then 'Decreasing'
        when daily_confirmed_cases = prev_day_cases then 'Stable'
        else 'Unknown'
    end as trend_direction
    
from with_changes
order by province_state, snapshot_date

