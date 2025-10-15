{{
    config(
        materialized='table',
        tags=['report']
    )
}}

/*
    Report Model: Top 5 Countries by Confirmed Cases
    
    Purpose: Answers the question - "What are the top 5 most common values 
    in a particular column, and what is their frequency?"
    
    This model identifies the countries with the highest confirmed COVID-19 cases
    and their frequency of occurrence across different regions/provinces.
    
    Built from: fct_covid_daily (fact table)
    This ensures consistency with all other gold models and leverages pre-calculated metrics.
    For exploration and custom analysis with complex transformations.
*/

with latest_by_country as (
    -- Get the most recent data for each country
    select
        country_region,
        max(report_date) as latest_report_date
    from {{ ref('fct_covid_daily') }}
    group by country_region
),

country_metrics as (
    select
        f.country_region,
        count(distinct f.report_date) as record_count,
        sum(f.confirmed_cases) as total_confirmed_cases,
        sum(f.total_deaths) as total_deaths,
        sum(f.total_recovered) as total_recovered,
        avg(f.case_fatality_ratio_pct) as avg_case_fatality_ratio,
        count(distinct f.province_state) as unique_provinces
    from {{ ref('fct_covid_daily') }} f
    inner join latest_by_country l
        on f.country_region = l.country_region
        and f.report_date = l.latest_report_date
    where f.country_region is not null
        and f.country_region != 'Unknown'
    group by f.country_region
),

ranked_countries as (
    select
        country_region,
        record_count as frequency,
        total_confirmed_cases,
        total_deaths,
        total_recovered,
        avg_case_fatality_ratio,
        unique_provinces,
        row_number() over (order by total_confirmed_cases desc) as rank_by_cases,
        round(
            (100.0 * total_confirmed_cases / sum(total_confirmed_cases) over ())::numeric,
            2
        ) as pct_of_global_cases
    from country_metrics
)

select
    rank_by_cases,
    country_region,
    frequency as occurrence_frequency,
    total_confirmed_cases,
    total_deaths,
    total_recovered,
    round(avg_case_fatality_ratio::numeric, 2) as avg_case_fatality_ratio_pct,
    unique_provinces,
    pct_of_global_cases,
    case
        when rank_by_cases = 1 then 'Highest'
        when rank_by_cases <= 3 then 'Very High'
        when rank_by_cases <= 5 then 'High'
        else 'Other'
    end as severity_tier
from ranked_countries
where rank_by_cases <= 5
order by rank_by_cases

