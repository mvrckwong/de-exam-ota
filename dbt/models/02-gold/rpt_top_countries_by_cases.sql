{{
    config(
        materialized='table',
        tags=['gold', 'report', 'top_values', 'from_silver']
    )
}}

/*
    Report Model: Top 5 Countries by Confirmed Cases
    
    Purpose: Answers the question - "What are the top 5 most common values 
    in a particular column, and what is their frequency?"
    
    This model identifies the countries with the highest confirmed COVID-19 cases
    and their frequency of occurrence across different regions/provinces.
*/

with country_metrics as (
    select
        country_region,
        count(*) as record_count,
        sum(confirmed_cases) as total_confirmed_cases,
        sum(total_deaths) as total_deaths,
        sum(total_recovered) as total_recovered,
        avg(case_fatality_ratio_pct) as avg_case_fatality_ratio,
        count(distinct province_state) as unique_provinces
    from {{ ref('silver_covid_cases_global') }}
    where country_region is not null
        and country_region != 'Unknown'
    group by country_region
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

