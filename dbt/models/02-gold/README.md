# Gold Layer Models

## Overview

This directory contains gold layer models that provide business-ready, analytical insights from COVID-19 data. These models are designed to answer specific analytical questions and provide aggregated metrics for reporting and visualization.

The gold layer is built on a dimensional modeling architecture with **`fct_covid_daily`** serving as the core fact table that combines all silver layer data into a unified, analytics-ready structure.

## Architecture

### 🏗️ Fact Table (Core Foundation)

#### `fct_covid_daily`
**The Single Source of Truth for COVID-19 Analytics**

- **Purpose:** Unified fact table combining US and global COVID-19 data at daily grain
- **Grain:** One row per location per day
- **Data Sources:**
  - US data: State-level daily snapshots
  - Global data: Country/province-level snapshots
  
**Key Features:**
- ✅ Surrogate key for unique identification (`fact_key`)
- ✅ Pre-calculated day-over-day changes
- ✅ 7-day rolling averages for trend smoothing
- ✅ Derived metrics (CFR, recovery rate, test positivity)
- ✅ Date dimensions (year, month, week, day of week)
- ✅ Geographic attributes (country, state, coordinates)
- ✅ Severity classifications and trend indicators
- ✅ Fact type classification (additive, semi-additive, non-additive)

**Why Use the Fact Table?**
1. **Performance:** Pre-calculated metrics avoid repeated computations
2. **Consistency:** Single version of truth for all analytics
3. **Simplicity:** Unified structure for both US and global data
4. **Flexibility:** Supports all analytical query patterns
5. **Maintainability:** Changes propagate to all downstream models

**Fact Types in the Table:**
- **Additive Facts:** Can be summed across all dimensions (e.g., `confirmed_cases`, `total_deaths`)
- **Semi-Additive Facts:** Can be summed across some dimensions (e.g., `confirmed_cases_7day_avg`)
- **Non-Additive Facts:** Cannot be summed (e.g., `case_fatality_ratio_pct`, ratios and percentages)

## Model Categories

### 📊 Analytical Models (Gold)

These models answer specific analytical questions with clean, documented insights.

#### 1. `gold_top_countries_by_cases`
**Question Answered:** *"What are the top 5 most common values in a particular column, and what is their frequency?"*

- **Purpose:** Identifies the top 5 countries with the highest confirmed COVID-19 cases
- **Key Metrics:**
  - Occurrence frequency (number of regional records)
  - Total confirmed cases
  - Percentage of global cases
  - Severity tier classification
- **Use Cases:**
  - Executive dashboards showing global hotspots
  - Comparative country analysis
  - Resource allocation planning

#### 2. `gold_covid_trends_over_time`
**Question Answered:** *"How does a particular metric change over time within the dataset?"*

- **Purpose:** Tracks the progression of COVID-19 metrics over time at the daily level
- **Key Metrics:**
  - Daily confirmed cases and deaths
  - Day-over-day changes and percentage changes
  - 7-day moving averages for trend smoothing
  - Trend direction indicators (Increasing/Decreasing/Stable)
- **Use Cases:**
  - Time series visualization
  - Trend analysis and forecasting
  - Policy impact assessment

#### 3. `gold_correlation_analysis`
**Question Answered:** *"Is there a correlation between two specific columns? Explain your findings."*

- **Purpose:** Calculates statistical correlations between key COVID-19 metrics
- **Correlations Analyzed:**
  1. **Confirmed Cases vs Deaths** - Expected strong positive correlation
  2. **Testing Rate vs Incident Rate** - Shows relationship between testing and case detection
  3. **Hospitalization Rate vs Mortality Rate** - Links severity to outcomes
  4. **Confirmed Cases vs Case Fatality Ratio** - Examines testing impact on CFR
  
- **Key Features:**
  - Pearson correlation coefficients
  - Correlation strength classification (Very Strong to Very Weak)
  - Direction indicators (Positive/Negative)
  - Statistical confidence levels based on sample size
  - Detailed interpretations of findings

- **Use Cases:**
  - Understanding relationships between metrics
  - Validating hypotheses about disease spread
  - Research and epidemiological analysis

**Findings Interpretation:**
- **Strong Positive Correlation (Cases vs Deaths):** As confirmed cases increase, deaths typically increase proportionally, validating the severity of the pandemic.
- **Positive Correlation (Testing Rate vs Incident Rate):** Higher testing rates often detect more cases, increasing reported incident rates. This suggests that testing capacity affects case detection.
- **Positive Correlation (Hospitalization vs Mortality):** Higher hospitalization rates correlate with more severe outcomes, indicating the importance of healthcare capacity.
- **Negative Correlation Possible (Cases vs CFR):** More widespread testing (more cases) may show lower fatality ratios as mild cases are detected.

### 🔄 Fact-Based Analytical Models

Simplified versions of analytical models that demonstrate querying from the fact table:

#### 7. `gold_top_countries_from_fact`
- **Based on:** `fct_covid_daily`
- **Demonstrates:** How to query the fact table for top values analysis
- **Benefits:** Simpler SQL, leverages pre-calculated metrics

#### 8. `gold_trends_from_fact`
- **Based on:** `fct_covid_daily`
- **Demonstrates:** Time series analysis using fact table
- **Benefits:** No need to recalculate rolling averages or day-over-day changes

#### 9. `gold_correlation_from_fact`
- **Based on:** `fct_covid_daily`
- **Demonstrates:** Statistical correlation using centralized metrics
- **Benefits:** Consistent data source, additional correlation pairs possible

### 📈 Aggregation Models

Pre-computed aggregations optimized for fast query performance and reporting.

#### 4. `agg_daily_summary`
- **Granularity:** Daily, national (US) level
- **Purpose:** High-level daily aggregated metrics across all US states
- **Key Metrics:**
  - Cumulative totals (cases, deaths, tests, hospitalizations)
  - Daily new cases and deaths
  - 7-day rolling averages
  - National average rates
  - Test positivity rate
  - Trend indicators
- **Use Cases:**
  - Executive dashboards
  - National trend monitoring
  - Public health reporting

#### 5. `agg_state_summary`
- **Granularity:** State level (US)
- **Purpose:** Comprehensive state-level aggregated metrics for comparison
- **Key Metrics:**
  - Total cumulative metrics per state
  - National rankings (by cases, deaths, incident rate)
  - Percentile distributions
  - Case fatality and test positivity rates
  - Impact category classification
- **Use Cases:**
  - State-by-state comparisons
  - Regional resource allocation
  - Performance benchmarking

#### 6. `agg_country_summary`
- **Granularity:** Country level (Global)
- **Purpose:** International comparative analysis with global rankings
- **Key Metrics:**
  - Total cumulative metrics per country
  - Global rankings
  - Percentage share of global cases/deaths
  - Recovery rates
  - Severity classifications
  - Global impact tiers
- **Use Cases:**
  - International comparisons
  - Global trend analysis
  - Policy benchmarking across countries

## Data Quality & Testing

All models include comprehensive data quality tests defined in `schema.yml`:
- **Uniqueness tests** on primary keys
- **Not null tests** on critical fields
- **Accepted values tests** for categorical fields
- **Relationship tests** to ensure referential integrity

## Best Practices Implemented

### 1. **Clear Documentation**
- Every model includes detailed header comments explaining purpose and usage
- Column-level documentation in schema.yml
- README with comprehensive explanations

### 2. **Performance Optimization**
- Materialized as tables for fast query performance
- Pre-aggregated metrics to avoid repeated calculations
- Efficient CTEs for readability and optimization

### 3. **Code Organization**
- Logical CTEs with clear naming
- Separation of concerns (raw aggregation → derived metrics → final output)
- Consistent formatting and style

### 4. **Analytics Best Practices**
- Moving averages for trend smoothing
- Percentile rankings for relative comparisons
- Statistical measures (correlation, confidence levels)
- Derived metrics calculated consistently

### 5. **Business Context**
- Classifications and tiers for easy interpretation
- Percentage calculations for relative comparisons
- Trend indicators for quick insights
- Human-readable descriptions and interpretations

## Data Lineage

```
Silver Layer
├── silver_covid_cases_us       ─┐
└── silver_covid_cases_global   ─┤
                                 │
                                 ▼
                        fct_covid_daily (CORE FACT TABLE)
                                 │
        ┌────────────────────────┼────────────────────────┐
        ▼                        ▼                        ▼
   Analytical Models      Aggregation Models      Fact-Based Models
   ├── gold_top_...      ├── agg_daily_...       ├── gold_top_..._from_fact
   ├── gold_trends_...   ├── agg_state_...       ├── gold_trends_from_fact
   └── gold_correl_...   └── agg_country_...     └── gold_correl_from_fact
```

## Usage Examples

### Using the Fact Table Directly

```sql
-- Get latest metrics for all locations
SELECT 
    country_region,
    province_state,
    report_date,
    confirmed_cases,
    daily_new_cases,
    case_trend,
    severity_category
FROM {{ ref('fct_covid_daily') }}
WHERE report_date = (SELECT MAX(report_date) FROM {{ ref('fct_covid_daily') }})
ORDER BY confirmed_cases DESC;

-- Time series for specific location
SELECT 
    report_date,
    confirmed_cases,
    daily_new_cases,
    confirmed_cases_7day_avg,
    case_trend
FROM {{ ref('fct_covid_daily') }}
WHERE province_state = 'California'
    AND data_source = 'US'
ORDER BY report_date;

-- Aggregate by country
SELECT 
    country_region,
    SUM(confirmed_cases) as total_cases,
    SUM(total_deaths) as total_deaths,
    AVG(case_fatality_ratio_pct) as avg_cfr
FROM {{ ref('fct_covid_daily') }}
WHERE report_date = '2021-12-31'
GROUP BY country_region
ORDER BY total_cases DESC;
```

### Query Top Countries
```sql
SELECT 
    rank_by_cases,
    country_region,
    total_confirmed_cases,
    pct_of_global_cases,
    severity_tier
FROM {{ ref('gold_top_countries_by_cases') }}
ORDER BY rank_by_cases;
```

### Track State Trends Over Time
```sql
SELECT 
    snapshot_date,
    province_state,
    new_cases_vs_prev_day,
    cases_7day_moving_avg,
    trend_direction
FROM {{ ref('gold_covid_trends_over_time') }}
WHERE province_state = 'California'
ORDER BY snapshot_date DESC;
```

### Analyze Correlations
```sql
SELECT 
    metric_pair,
    correlation_coefficient,
    correlation_strength,
    correlation_direction,
    interpretation
FROM {{ ref('gold_correlation_analysis') }}
ORDER BY abs(correlation_coefficient) DESC;
```

### Daily National Summary
```sql
SELECT 
    snapshot_date,
    new_cases_today,
    cases_7day_rolling_avg,
    test_positivity_rate_pct,
    case_trend
FROM {{ ref('agg_daily_summary') }}
ORDER BY snapshot_date DESC
LIMIT 30;
```

## Model Comparison: Direct Silver vs Fact-Based

| Aspect | Direct Silver Models | Fact-Based Models |
|--------|---------------------|-------------------|
| **Complexity** | More complex, joins multiple sources | Simpler, single source |
| **Performance** | Repeated calculations | Pre-calculated metrics |
| **Consistency** | May have calculation differences | Guaranteed consistency |
| **Maintenance** | Changes needed in multiple places | Changes propagate from fact table |
| **Use Case** | Initial development, specific needs | Production, standard analytics |

**Recommendation:** Use fact-based models for production analytics and direct silver models when you need specific transformations not available in the fact table.

## Maintenance

- **Dependencies:** 
  - Fact table depends on silver layer models
  - Analytical and aggregation models depend on either silver layer or fact table
- **Refresh Frequency:** Should be refreshed when silver layer is updated
- **Build Order:**
  1. Silver layer models
  2. `fct_covid_daily` (core fact table)
  3. All gold layer analytical and aggregation models
- **Testing:** Run `dbt test --select 02-gold` to validate data quality

## Tags

Models are tagged for easy selection:
- `gold` - All gold layer models
- `fact` - Fact table models
- `core` - Core/foundational models
- `analytics` - Analytical insight models
- `aggregation` - Pre-computed aggregation models
- `fact_based` - Models that query from fact table
- `time_series` - Time-based analysis models
- `correlation` - Statistical correlation models
- `top_values` - Frequency/ranking analysis
- `daily` - Daily grain aggregations
- `state` - State-level aggregations
- `country` - Country-level aggregations

Run specific tags:
```bash
# Build everything in gold layer
dbt run --select tag:gold

# Build only the fact table
dbt run --select tag:fact

# Build fact-based analytical models
dbt run --select tag:fact_based

# Build aggregations
dbt run --select tag:aggregation

# Test all analytics
dbt test --select tag:analytics
```

## Performance Tips

1. **Start with the Fact Table:** Always build `fct_covid_daily` first as it's the foundation
2. **Use Fact-Based Models:** They're optimized and leverage pre-calculated metrics
3. **Materialize as Tables:** Gold models are materialized as tables for fast query performance
4. **Incremental Builds:** Consider incremental materialization for very large fact tables
5. **Indexing:** Add indexes on frequently filtered columns (report_date, country_region, province_state)

## Best Practices Implemented

✅ **Dimensional Modeling** - Fact table with clear grain and dimensions  
✅ **Single Source of Truth** - Centralized fact table for all analytics  
✅ **Pre-calculated Metrics** - Avoid repeated computation  
✅ **Clear Documentation** - Every model and column documented  
✅ **Comprehensive Testing** - Data quality tests on all models  
✅ **Performance Optimization** - Table materialization, efficient CTEs  
✅ **Separation of Concerns** - Fact table separate from analytical logic  
✅ **Flexibility** - Support both direct and fact-based querying patterns

