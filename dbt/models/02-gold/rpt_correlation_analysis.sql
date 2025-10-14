{{
    config(
        materialized='table',
        tags=['gold', 'report', 'correlation', 'from_silver']
    )
}}

/*
    Report Model: Correlation Analysis Between Key Metrics
    
    Purpose: Answers the question - "Is there a correlation between two 
    specific columns? Explain your findings."
    
    This model calculates correlations between:
    1. Confirmed Cases vs Deaths (expected strong positive correlation)
    2. Testing Rate vs Incident Rate (expected negative correlation)
    3. Hospitalization Rate vs Mortality Rate (expected positive correlation)
    4. Confirmed Cases vs Case Fatality Ratio (inverse relationship expected)
*/

with us_metrics as (
    select
        province_state,
        confirmed_cases,
        total_deaths,
        people_tested,
        people_hospitalized,
        testing_rate_per_100k,
        hospitalization_rate_pct,
        mortality_rate_pct,
        incident_rate_per_100k,
        case_fatality_ratio_pct
    from {{ ref('silver_covid_cases_us') }}
    where confirmed_cases > 0
        and total_deaths >= 0
        and snapshot_date is not null
),

-- Calculate correlation between confirmed cases and deaths
cases_deaths_correlation as (
    select
        'Confirmed Cases vs Deaths' as metric_pair,
        corr(confirmed_cases, total_deaths) as correlation_coefficient,
        count(*) as sample_size,
        avg(confirmed_cases) as avg_cases,
        avg(total_deaths) as avg_deaths,
        stddev(confirmed_cases) as stddev_cases,
        stddev(total_deaths) as stddev_deaths
    from us_metrics
    where total_deaths is not null
),

-- Calculate correlation between testing rate and incident rate
testing_incident_correlation as (
    select
        'Testing Rate vs Incident Rate' as metric_pair,
        corr(testing_rate_per_100k, incident_rate_per_100k) as correlation_coefficient,
        count(*) as sample_size,
        avg(testing_rate_per_100k) as avg_testing_rate,
        avg(incident_rate_per_100k) as avg_incident_rate,
        stddev(testing_rate_per_100k) as stddev_testing_rate,
        stddev(incident_rate_per_100k) as stddev_incident_rate
    from us_metrics
    where testing_rate_per_100k is not null
        and incident_rate_per_100k is not null
),

-- Calculate correlation between hospitalization rate and mortality rate
hosp_mortality_correlation as (
    select
        'Hospitalization Rate vs Mortality Rate' as metric_pair,
        corr(hospitalization_rate_pct, mortality_rate_pct) as correlation_coefficient,
        count(*) as sample_size,
        avg(hospitalization_rate_pct) as avg_hospitalization_rate,
        avg(mortality_rate_pct) as avg_mortality_rate,
        stddev(hospitalization_rate_pct) as stddev_hospitalization_rate,
        stddev(mortality_rate_pct) as stddev_mortality_rate
    from us_metrics
    where hospitalization_rate_pct is not null
        and mortality_rate_pct is not null
),

-- Calculate correlation between confirmed cases and case fatality ratio
cases_cfr_correlation as (
    select
        'Confirmed Cases vs Case Fatality Ratio' as metric_pair,
        corr(confirmed_cases, case_fatality_ratio_pct) as correlation_coefficient,
        count(*) as sample_size,
        avg(confirmed_cases) as avg_cases_cfr,
        avg(case_fatality_ratio_pct) as avg_case_fatality_ratio,
        stddev(confirmed_cases) as stddev_cases_cfr,
        stddev(case_fatality_ratio_pct) as stddev_case_fatality_ratio
    from us_metrics
    where case_fatality_ratio_pct is not null
),

combined_correlations as (
    select 
        metric_pair,
        correlation_coefficient,
        sample_size,
        avg_cases as metric_1_avg,
        avg_deaths as metric_2_avg,
        stddev_cases as metric_1_stddev,
        stddev_deaths as metric_2_stddev
    from cases_deaths_correlation
    
    union all
    
    select 
        metric_pair,
        correlation_coefficient,
        sample_size,
        avg_testing_rate as metric_1_avg,
        avg_incident_rate as metric_2_avg,
        stddev_testing_rate as metric_1_stddev,
        stddev_incident_rate as metric_2_stddev
    from testing_incident_correlation
    
    union all
    
    select 
        metric_pair,
        correlation_coefficient,
        sample_size,
        avg_hospitalization_rate as metric_1_avg,
        avg_mortality_rate as metric_2_avg,
        stddev_hospitalization_rate as metric_1_stddev,
        stddev_mortality_rate as metric_2_stddev
    from hosp_mortality_correlation
    
    union all
    
    select 
        metric_pair,
        correlation_coefficient,
        sample_size,
        avg_cases_cfr as metric_1_avg,
        avg_case_fatality_ratio as metric_2_avg,
        stddev_cases_cfr as metric_1_stddev,
        stddev_case_fatality_ratio as metric_2_stddev
    from cases_cfr_correlation
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
    
    -- Statistical significance indicator (rule of thumb: larger sample = more reliable)
    case
        when sample_size >= 100 then 'High confidence'
        when sample_size >= 30 then 'Medium confidence'
        else 'Low confidence'
    end as statistical_confidence,
    
    -- Interpretation
    case
        when metric_pair = 'Confirmed Cases vs Deaths' then
            'Strong positive correlation expected: As cases increase, deaths typically increase proportionally.'
        when metric_pair = 'Testing Rate vs Incident Rate' then
            'Positive correlation: Higher testing rates often detect more cases, increasing incident rates.'
        when metric_pair = 'Hospitalization Rate vs Mortality Rate' then
            'Positive correlation: Higher hospitalization rates often correlate with more severe outcomes.'
        when metric_pair = 'Confirmed Cases vs Case Fatality Ratio' then
            'Negative correlation possible: More cases (better testing) may show lower fatality ratios.'
        else 'No interpretation available'
    end as interpretation

from combined_correlations
order by abs(correlation_coefficient) desc

