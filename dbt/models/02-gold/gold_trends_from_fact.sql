{{
    config(
        materialized='table',
        tags=['gold', 'analytics', 'time_series', 'fact_based']
    )
}}

/*
    Gold Model: COVID-19 Trends Over Time (Fact-Based Version)
    
    Purpose: Demonstrates how to use fct_covid_daily for time series analysis.
    This is a simplified version that leverages pre-calculated metrics in the fact table.
    
    Answers: "How does a particular metric change over time within the dataset?"
*/

with daily_trends as (
    select
        report_date,
        data_source,
        country_region,
        province_state,
        location_name,
        
        -- Core metrics (already calculated in fact table)
        confirmed_cases,
        total_deaths,
        daily_new_cases,
        daily_new_deaths,
        
        -- Percentage changes (already calculated)
        pct_change_cases,
        pct_change_deaths,
        
        -- Rolling averages (already calculated)
        confirmed_cases_7day_avg,
        deaths_7day_avg,
        
        -- Trend indicator (already calculated)
        case_trend,
        
        -- Additional metrics
        test_positivity_rate_pct,
        case_fatality_ratio_pct,
        severity_category,
        
        -- Date dimensions
        report_year,
        report_month,
        day_of_week,
        week_of_year
        
    from {{ ref('fct_covid_daily') }}
    where data_source = 'US'  -- Focus on US for detailed tracking
        and province_state is not null
),

with_weekly_aggregates as (
    select
        *,
        -- Calculate weekly aggregates for additional context
        avg(daily_new_cases) over (
            partition by province_state, report_year, week_of_year
        ) as weekly_avg_new_cases,
        
        max(daily_new_cases) over (
            partition by province_state, report_year, week_of_year
        ) as weekly_peak_new_cases,
        
        sum(daily_new_cases) over (
            partition by province_state, report_year, week_of_year
        ) as weekly_total_new_cases
    from daily_trends
)

select
    report_date,
    report_year,
    report_month,
    week_of_year,
    day_of_week,
    province_state,
    location_name,
    
    -- Core metrics
    confirmed_cases as cumulative_confirmed_cases,
    total_deaths as cumulative_deaths,
    daily_new_cases,
    daily_new_deaths,
    
    -- Change metrics
    pct_change_cases,
    pct_change_deaths,
    
    -- Smoothed trends
    round(confirmed_cases_7day_avg::numeric, 2) as cases_7day_moving_avg,
    round(deaths_7day_avg::numeric, 2) as deaths_7day_moving_avg,
    
    -- Weekly context
    round(weekly_avg_new_cases::numeric, 2) as weekly_avg_new_cases,
    weekly_peak_new_cases,
    weekly_total_new_cases,
    
    -- Rates and ratios
    round(test_positivity_rate_pct::numeric, 2) as test_positivity_rate_pct,
    round(case_fatality_ratio_pct::numeric, 2) as case_fatality_ratio_pct,
    
    -- Classifications
    case_trend,
    severity_category,
    
    -- Trend strength indicator
    case
        when abs(pct_change_cases) >= 20 then 'Sharp'
        when abs(pct_change_cases) >= 10 then 'Moderate'
        when abs(pct_change_cases) >= 5 then 'Gradual'
        else 'Minimal'
    end as trend_strength

from with_weekly_aggregates
order by province_state, report_date

