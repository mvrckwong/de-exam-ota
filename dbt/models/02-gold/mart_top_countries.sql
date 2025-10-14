{{
    config(
        materialized='table',
        tags=['gold', 'mart', 'top_values', 'from_fact']
    )
}}

/*
    Data Mart: Top 5 Countries by Cases
    
    Purpose: Optimized data mart for top countries analysis.
    Queries from fct_covid_daily for better performance and consistency.
    This is the production-ready version - use this over rpt_top_countries_by_cases
    for dashboards and regular reporting.
    
    Answers: "What are the top 5 most common values in a particular column, 
    and what is their frequency?"
*/

with latest_by_country as (
    -- Get the most recent data for each country
    select
        country_region,
        max(report_date) as latest_report_date
    from {{ ref('fct_covid_daily') }}
    group by country_region
),

country_latest_metrics as (
    select
        f.country_region,
        count(distinct f.province_state) as provinces_reporting,
        count(distinct f.report_date) as days_reporting,
        sum(f.confirmed_cases) as total_confirmed_cases,
        sum(f.total_deaths) as total_deaths,
        sum(f.total_recovered) as total_recovered,
        avg(f.case_fatality_ratio_pct) as avg_case_fatality_ratio_pct,
        max(f.severity_category) as peak_severity_category
    from {{ ref('fct_covid_daily') }} f
    inner join latest_by_country l
        on f.country_region = l.country_region
        and f.report_date = l.latest_report_date
    group by f.country_region
),

ranked_countries as (
    select
        country_region,
        days_reporting as occurrence_frequency,
        total_confirmed_cases,
        total_deaths,
        total_recovered,
        provinces_reporting,
        avg_case_fatality_ratio_pct,
        peak_severity_category,
        row_number() over (order by total_confirmed_cases desc) as rank_by_cases,
        round(
            (100.0 * total_confirmed_cases / sum(total_confirmed_cases) over ())::numeric,
            2
        ) as pct_of_global_cases
    from country_latest_metrics
)

select
    rank_by_cases,
    country_region,
    occurrence_frequency,
    total_confirmed_cases,
    total_deaths,
    total_recovered,
    provinces_reporting,
    round(avg_case_fatality_ratio_pct::numeric, 2) as avg_case_fatality_ratio_pct,
    pct_of_global_cases,
    peak_severity_category,
    case
        when rank_by_cases = 1 then 'Highest'
        when rank_by_cases <= 3 then 'Very High'
        when rank_by_cases <= 5 then 'High'
        else 'Other'
    end as impact_tier
from ranked_countries
where rank_by_cases <= 5
order by rank_by_cases

