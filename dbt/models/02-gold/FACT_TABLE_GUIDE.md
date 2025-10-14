# Fact Table Implementation Guide

## Quick Start

### What is `fct_covid_daily`?

`fct_covid_daily` is the **core fact table** that combines all COVID-19 data from both US and global sources into a single, unified table optimized for analytics. Think of it as your "one-stop shop" for all COVID-19 metrics.

### Why Do We Need It?

**Problem Without Fact Table:**
- Query US data from `silver_covid_cases_us`
- Query global data from `silver_covid_cases_global`
- Calculate metrics repeatedly in each model
- Risk of inconsistent calculations
- Slower performance due to repeated joins and calculations

**Solution With Fact Table:**
- Query everything from one table: `fct_covid_daily`
- Metrics pre-calculated once
- Guaranteed consistency across all reports
- Faster queries (no repeated calculations)
- Unified structure for US + global data

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                           │
├─────────────────────────────────────────────────────────────┤
│  silver_covid_cases_us         silver_covid_cases_global    │
│  (State-level, daily)          (Country-level)              │
└──────────────────┬──────────────────────┬───────────────────┘
                   │                      │
                   └──────────┬───────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  FACT TABLE (CORE)                          │
├─────────────────────────────────────────────────────────────┤
│                   fct_covid_daily                           │
│                                                              │
│  • Unified US + Global data                                 │
│  • Daily grain                                              │
│  • Pre-calculated metrics                                   │
│  • Surrogate key                                            │
│  • Date dimensions                                          │
│  • Geographic attributes                                    │
│  • Trend indicators                                         │
└──────────────────┬──────────────────────────────────────────┘
                   │
    ┌──────────────┼──────────────┐
    │              │              │
    ▼              ▼              ▼
┌─────────┐  ┌─────────┐  ┌─────────────┐
│Analytics│  │  Aggs   │  │ Fact-Based  │
│ Models  │  │ Models  │  │   Models    │
└─────────┘  └─────────┘  └─────────────┘
```

## What's Inside the Fact Table?

### Key Columns

#### 🔑 Primary Key
- `fact_key` - Unique identifier for each row

#### 📍 Dimensions (Who, What, Where)
- `data_source` - 'US' or 'Global'
- `country_region` - Country name
- `province_state` - State/province (if applicable)
- `location_name` - Full location identifier
- `latitude`, `longitude` - Geographic coordinates

#### 📅 Date Dimensions (When)
- `report_date` - Date of report
- `report_year`, `report_month`, `report_day`
- `day_of_week` - Monday, Tuesday, etc.
- `week_of_year` - Week number (1-52)

#### 📊 Core Metrics (Additive Facts)
These can be summed across dimensions:
- `confirmed_cases` - Cumulative confirmed cases
- `total_deaths` - Cumulative deaths
- `total_recovered` - Cumulative recovered
- `people_tested` - Total people tested
- `people_hospitalized` - Total hospitalized

#### 📈 Daily Change Metrics (Pre-calculated!)
- `daily_new_cases` - New cases today vs yesterday
- `daily_new_deaths` - New deaths today vs yesterday
- `pct_change_cases` - Percentage change in cases
- `pct_change_deaths` - Percentage change in deaths

#### 📉 Trend Metrics (Pre-calculated!)
- `confirmed_cases_7day_avg` - 7-day rolling average
- `deaths_7day_avg` - 7-day rolling average of deaths
- `case_trend` - 'Increasing', 'Decreasing', 'Stable'

#### 🎯 Rate Metrics (Non-Additive Facts)
These are percentages/ratios - don't sum them!
- `incident_rate_per_100k`
- `case_fatality_ratio_pct`
- `testing_rate_per_100k`
- `test_positivity_rate_pct`
- `recovery_rate_pct`

#### 🏷️ Classifications
- `severity_category` - 'Critical', 'Very High', 'High', 'Moderate', 'Low', 'Very Low'
- `has_new_cases` - Boolean flag
- `has_new_deaths` - Boolean flag

## How Does It Answer the 3 Analytical Questions?

### Question 1: Top 5 Most Common Values & Frequency

**Query Example:**
```sql
SELECT 
    country_region,
    COUNT(DISTINCT report_date) as frequency,
    SUM(confirmed_cases) as total_cases
FROM {{ ref('fct_covid_daily') }}
GROUP BY country_region
ORDER BY total_cases DESC
LIMIT 5;
```

**Why It Works:**
- Single table has all country data
- Pre-aggregated, just need GROUP BY
- No complex joins needed

### Question 2: How Metrics Change Over Time

**Query Example:**
```sql
SELECT 
    report_date,
    province_state,
    confirmed_cases,
    daily_new_cases,              -- ✅ Already calculated!
    confirmed_cases_7day_avg,     -- ✅ Already calculated!
    case_trend                     -- ✅ Already classified!
FROM {{ ref('fct_covid_daily') }}
WHERE province_state = 'California'
ORDER BY report_date;
```

**Why It Works:**
- Time series metrics pre-calculated
- No need for LAG() or window functions
- Trend indicators ready to use

### Question 3: Correlation Between Columns

**Query Example:**
```sql
SELECT 
    CORR(confirmed_cases, total_deaths) as cases_deaths_correlation,
    CORR(testing_rate_per_100k, incident_rate_per_100k) as testing_incident_correlation
FROM {{ ref('fct_covid_daily') }}
WHERE data_source = 'US';
```

**Why It Works:**
- All metrics in one table
- Consistent data source
- No risk of mismatched joins

## Comparison: Before vs After

### Before Fact Table (Querying Silver Direct)

```sql
-- Complex! Multiple CTEs needed
WITH us_daily AS (
    SELECT 
        province_state,
        snapshot_date,
        confirmed_cases,
        LAG(confirmed_cases) OVER (...) as prev_cases,  -- Calculate every time!
        ...
    FROM silver_covid_cases_us
),
with_changes AS (
    SELECT 
        *,
        confirmed_cases - prev_cases as daily_new,      -- Calculate every time!
        AVG(confirmed_cases) OVER (...)                 -- Calculate every time!
    FROM us_daily
)
SELECT * FROM with_changes;
```

### After Fact Table

```sql
-- Simple! Pre-calculated
SELECT 
    report_date,
    province_state,
    confirmed_cases,
    daily_new_cases,              -- Already done!
    confirmed_cases_7day_avg      -- Already done!
FROM {{ ref('fct_covid_daily') }}
WHERE data_source = 'US';
```

## Model Options: Which Should You Use?

### Option 1: Report Models (rpt_*)
**Files:** 
- `rpt_top_countries_by_cases.sql`
- `rpt_covid_trends_over_time.sql`
- `rpt_correlation_analysis.sql`

**When to use:**
- Initial development and exploration
- Need specific transformations not in fact table
- Custom analysis and ad-hoc queries
- Testing new analytical patterns

**Pros:**
- More flexible
- Don't depend on fact table
- Direct access to silver data
- Can implement custom logic

**Cons:**
- More complex SQL
- Repeated calculations
- Potential inconsistency
- Slower performance

### Option 2: Data Mart Models (mart_*)
**Files:**
- `mart_top_countries.sql`
- `mart_covid_trends.sql`
- `mart_correlation_metrics.sql`

**When to use:**
- Production analytics
- Standard reporting and dashboards
- Performance-critical queries
- Regular business use cases

**Pros:**
- Simpler SQL
- Faster performance
- Guaranteed consistency
- Easier to maintain
- Leverages pre-calculated metrics

**Cons:**
- Depends on fact table
- Limited to pre-calculated metrics
- Less flexible

### Our Recommendation

**For Production:** Use data mart models (Option 2) ⭐
- Better performance
- Easier maintenance
- Consistent results
- Optimized for dashboards

**For Development/Exploration:** Use report models (Option 1)
- More flexibility
- Can test new metrics
- Custom transformations
- Don't need to rebuild fact table

## Usage Examples

### Example 1: Get Latest Snapshot

```sql
-- All locations, most recent data
SELECT 
    country_region,
    province_state,
    confirmed_cases,
    total_deaths,
    severity_category
FROM {{ ref('fct_covid_daily') }}
WHERE report_date = (
    SELECT MAX(report_date) 
    FROM {{ ref('fct_covid_daily') }}
)
ORDER BY confirmed_cases DESC;
```

### Example 2: Time Series for Specific Location

```sql
-- Track California over time
SELECT 
    report_date,
    daily_new_cases,
    confirmed_cases_7day_avg,
    case_trend,
    test_positivity_rate_pct
FROM {{ ref('fct_covid_daily') }}
WHERE province_state = 'California'
    AND data_source = 'US'
ORDER BY report_date;
```

### Example 3: Compare Countries

```sql
-- Compare top 5 countries
WITH top_countries AS (
    SELECT DISTINCT country_region
    FROM {{ ref('fct_covid_daily') }}
    ORDER BY confirmed_cases DESC
    LIMIT 5
)
SELECT 
    f.report_date,
    f.country_region,
    f.daily_new_cases,
    f.case_trend
FROM {{ ref('fct_covid_daily') }} f
INNER JOIN top_countries t ON f.country_region = t.country_region
ORDER BY f.report_date, f.country_region;
```

### Example 4: Weekly Aggregates

```sql
-- Weekly summaries
SELECT 
    report_year,
    week_of_year,
    data_source,
    SUM(daily_new_cases) as weekly_new_cases,
    AVG(test_positivity_rate_pct) as avg_positivity,
    COUNT(DISTINCT location_name) as locations_reporting
FROM {{ ref('fct_covid_daily') }}
GROUP BY report_year, week_of_year, data_source
ORDER BY report_year, week_of_year;
```

## Building the Models

### Step 1: Build Fact Table First

```bash
# Build just the fact table
dbt run --select fct_covid_daily

# Or use tag
dbt run --select tag:fact
```

### Step 2: Build Dependent Models

```bash
# Build all fact-based models
dbt run --select tag:fact_based

# Or build all gold models
dbt run --select tag:gold
```

### Step 3: Test Everything

```bash
# Test fact table
dbt test --select fct_covid_daily

# Test all gold models
dbt test --select tag:gold
```

## Fact Types Explained

### Additive Facts (Can Sum Across All Dimensions)
✅ `confirmed_cases` - Sum by country, date, any dimension  
✅ `total_deaths` - Sum by country, date, any dimension  
✅ `daily_new_cases` - Sum by country, date, any dimension

**Example:**
```sql
-- This makes sense
SELECT SUM(confirmed_cases) FROM fct_covid_daily;
```

### Semi-Additive Facts (Sum Across Some Dimensions)
⚠️ `confirmed_cases_7day_avg` - Sum by location, NOT by date  
⚠️ `deaths_7day_avg` - Sum by location, NOT by date

**Example:**
```sql
-- ✅ This makes sense (sum across locations for one date)
SELECT SUM(confirmed_cases_7day_avg) 
FROM fct_covid_daily 
WHERE report_date = '2021-12-31';

-- ❌ This does NOT make sense (sum across dates)
SELECT SUM(confirmed_cases_7day_avg) 
FROM fct_covid_daily 
WHERE province_state = 'California';
```

### Non-Additive Facts (Cannot Sum)
❌ `case_fatality_ratio_pct` - It's a percentage/ratio  
❌ `test_positivity_rate_pct` - It's a percentage/ratio  
❌ `incident_rate_per_100k` - It's a rate

**Example:**
```sql
-- ❌ WRONG - Don't sum percentages!
SELECT SUM(case_fatality_ratio_pct) FROM fct_covid_daily;

-- ✅ RIGHT - Use AVG for percentages
SELECT AVG(case_fatality_ratio_pct) FROM fct_covid_daily;
```

## Performance Tips

1. **Always filter on indexed columns:**
   - `report_date`
   - `country_region`
   - `province_state`
   - `data_source`

2. **Use pre-calculated metrics** instead of recalculating

3. **Aggregate at the right level** - don't retrieve too much data

4. **Be aware of fact types** - don't sum non-additive facts

## Summary

✅ **Use `fct_covid_daily` as your primary data source**  
✅ **Leverage pre-calculated metrics** (daily changes, rolling averages)  
✅ **Build fact-based models for production**  
✅ **Understand fact types** (additive, semi-additive, non-additive)  
✅ **Query performance is excellent** (pre-aggregated, indexed)

The fact table is your friend! It makes analytics simpler, faster, and more consistent. 🚀


