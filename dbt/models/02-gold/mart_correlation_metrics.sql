{{
    config(
        materialized='table',
        tags=['mart']
    )
}}

/*
    Data Mart: Correlation Analysis
    
    Purpose: Optimized data mart for correlation analysis.
    Uses centralized metrics from fct_covid_daily ensuring consistency.
    This is the production-ready version - use this over rpt_correlation_analysis
    for dashboards and regular reporting.
    
    Answers: "Is there a correlation between two specific columns? 
    Explain your findings."
*/

with us_metrics as (
    select
        confirmed_cases,
        total_deaths,
        people_tested,
        people_hospitalized,
        testing_rate_per_100k,
        hospitalization_rate_pct,
        mortality_rate_pct,
        incident_rate_per_100k,
        case_fatality_ratio_pct,
        test_positivity_rate_pct,
        daily_new_cases,
        daily_new_deaths
    from {{ ref('fct_covid_daily') }}
    where data_source = 'US'
        and confirmed_cases > 0
        and report_date is not null
),

-- Correlation 1: Confirmed Cases vs Deaths
cases_deaths_corr as (
    select
        'Confirmed Cases vs Deaths' as metric_pair,
        corr(confirmed_cases, total_deaths) as correlation_coefficient,
        count(*) as sample_size,
        avg(confirmed_cases) as metric_1_avg,
        avg(total_deaths) as metric_2_avg,
        stddev(confirmed_cases) as metric_1_stddev,
        stddev(total_deaths) as metric_2_stddev
    from us_metrics
    where total_deaths is not null
),

-- Correlation 2: Testing Rate vs Incident Rate
testing_incident_corr as (
    select
        'Testing Rate vs Incident Rate' as metric_pair,
        corr(testing_rate_per_100k, incident_rate_per_100k) as correlation_coefficient,
        count(*) as sample_size,
        avg(testing_rate_per_100k) as metric_1_avg,
        avg(incident_rate_per_100k) as metric_2_avg,
        stddev(testing_rate_per_100k) as metric_1_stddev,
        stddev(incident_rate_per_100k) as metric_2_stddev
    from us_metrics
    where testing_rate_per_100k is not null
        and incident_rate_per_100k is not null
),

-- Correlation 3: Hospitalization Rate vs Mortality Rate
hosp_mortality_corr as (
    select
        'Hospitalization Rate vs Mortality Rate' as metric_pair,
        corr(hospitalization_rate_pct, mortality_rate_pct) as correlation_coefficient,
        count(*) as sample_size,
        avg(hospitalization_rate_pct) as metric_1_avg,
        avg(mortality_rate_pct) as metric_2_avg,
        stddev(hospitalization_rate_pct) as metric_1_stddev,
        stddev(mortality_rate_pct) as metric_2_stddev
    from us_metrics
    where hospitalization_rate_pct is not null
        and mortality_rate_pct is not null
),

-- Correlation 4: Daily New Cases vs Test Positivity
new_cases_positivity_corr as (
    select
        'Daily New Cases vs Test Positivity Rate' as metric_pair,
        corr(daily_new_cases, test_positivity_rate_pct) as correlation_coefficient,
        count(*) as sample_size,
        avg(daily_new_cases) as metric_1_avg,
        avg(test_positivity_rate_pct) as metric_2_avg,
        stddev(daily_new_cases) as metric_1_stddev,
        stddev(test_positivity_rate_pct) as metric_2_stddev
    from us_metrics
    where test_positivity_rate_pct is not null
        and daily_new_cases is not null
),

-- Correlation 5: Cases vs Case Fatality Ratio
cases_cfr_corr as (
    select
        'Confirmed Cases vs Case Fatality Ratio' as metric_pair,
        corr(confirmed_cases, case_fatality_ratio_pct) as correlation_coefficient,
        count(*) as sample_size,
        avg(confirmed_cases) as metric_1_avg,
        avg(case_fatality_ratio_pct) as metric_2_avg,
        stddev(confirmed_cases) as metric_1_stddev,
        stddev(case_fatality_ratio_pct) as metric_2_stddev
    from us_metrics
    where case_fatality_ratio_pct is not null
),

all_correlations as (
    select * from cases_deaths_corr
    union all
    select * from testing_incident_corr
    union all
    select * from hosp_mortality_corr
    union all
    select * from new_cases_positivity_corr
    union all
    select * from cases_cfr_corr
)

select
    metric_pair,
    round(correlation_coefficient::numeric, 4) as correlation_coefficient,
    sample_size,
    round(metric_1_avg::numeric, 2) as metric_1_avg,
    round(metric_2_avg::numeric, 2) as metric_2_avg,
    round(metric_1_stddev::numeric, 2) as metric_1_stddev,
    round(metric_2_stddev::numeric, 2) as metric_2_stddev,
    
    -- Interpretation of correlation strength
    case
        when abs(correlation_coefficient) >= 0.9 then 'Very Strong'
        when abs(correlation_coefficient) >= 0.7 then 'Strong'
        when abs(correlation_coefficient) >= 0.5 then 'Moderate'
        when abs(correlation_coefficient) >= 0.3 then 'Weak'
        else 'Very Weak or None'
    end as correlation_strength,
    
    -- Direction of correlation
    case
        when correlation_coefficient > 0 then 'Positive'
        when correlation_coefficient < 0 then 'Negative'
        else 'None'
    end as correlation_direction,
    
    -- Statistical significance
    case
        when sample_size >= 1000 then 'Very High confidence'
        when sample_size >= 100 then 'High confidence'
        when sample_size >= 30 then 'Medium confidence'
        else 'Low confidence'
    end as statistical_confidence,
    
    -- R-squared (coefficient of determination)
    round(power(correlation_coefficient, 2)::numeric, 4) as r_squared,
    
    -- Interpretation
    case
        when metric_pair = 'Confirmed Cases vs Deaths' then
            'Strong positive correlation expected: More cases lead to more deaths, validating disease severity.'
        when metric_pair = 'Testing Rate vs Incident Rate' then
            'Positive correlation: Higher testing rates detect more cases, increasing incident rates.'
        when metric_pair = 'Hospitalization Rate vs Mortality Rate' then
            'Positive correlation: Higher hospitalization often indicates more severe cases and outcomes.'
        when metric_pair = 'Daily New Cases vs Test Positivity Rate' then
            'Positive correlation: Surges in new cases often accompanied by higher positivity rates.'
        when metric_pair = 'Confirmed Cases vs Case Fatality Ratio' then
            'Negative correlation possible: Better testing (more cases) may reveal lower true CFR.'
        else 'No interpretation available'
    end as interpretation

from all_correlations
order by abs(correlation_coefficient) desc

