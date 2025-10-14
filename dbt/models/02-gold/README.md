# Gold Layer Models

## Overview

This directory contains gold layer models that provide business-ready, analytical insights from COVID-19 data. These models are designed to answer specific analytical questions and provide aggregated metrics for reporting and visualization.

The gold layer is built on a dimensional modeling architecture with **`fct_covid_daily`** serving as the core fact table that combines all silver layer data into a unified, analytics-ready structure.

## Naming Conventions (dbt Best Practices)

Following dbt naming conventions, models are prefixed by their purpose:

| Prefix | Purpose | When to Use |
|--------|---------|-------------|
| `fct_` | **Fact Tables** - Granular, event-level data | Core data with defined grain |
| `agg_` | **Aggregations** - Pre-computed summaries | Performance-optimized summaries |
| `rpt_` | **Reports** - Answer specific business questions | Complex analysis from silver layer |
| `mart_` | **Data Marts** - Business-ready, optimized marts | Simplified queries from fact table |
| `dim_` | **Dimensions** - Descriptive reference data | Lookup tables (future) |

**Note:** No "gold" prefix needed - the folder structure (`02-gold`) defines the layer.

## Architecture

### 🏗️ **CONSISTENT ARCHITECTURE ACHIEVED**

**All gold models now follow the same architectural pattern:**

#### **Single Source of Truth**
- **Fact Table**: `fct_covid_daily` - Central hub for all analytical queries
- **All Models**: Source from `fct_covid_daily` for consistency and performance
- **Pre-calculated Metrics**: Leverage existing calculations for better performance

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

### 🏗️ **CONSISTENT ARCHITECTURE ACHIEVED**

**All gold models now follow the same architectural pattern:**

#### **Single Source of Truth**
- **Fact Table**: `fct_covid_daily` - Central hub for all analytical queries
- **All Models**: Source from `fct_covid_daily` for consistency and performance
- **Pre-calculated Metrics**: Leverage existing calculations for better performance

### 📊 Model Categories

#### **1. Core Fact Table**
- **`fct_covid_daily`** - Single source of truth combining US and global data
  - **Purpose**: Central hub for all analytical queries
  - **Grain**: One row per location per day
  - **Features**: Pre-calculated metrics, surrogate keys, derived measures

#### **2. Aggregation Models** (from fact table)
- **`agg_daily_summary`** - Daily national-level aggregated metrics
- **`agg_state_summary`** - State-level comprehensive summaries  
- **`agg_country_summary`** - Country-level global analysis

#### **3. Data Mart Models** (from fact table)
- **`mart_top_countries`** - Top countries analysis for dashboards
- **`mart_covid_trends`** - Time series trends for dashboards
- **`mart_correlation_metrics`** - Correlation analysis for dashboards

#### **4. Report Models** (from fact table)
- **`rpt_top_countries_by_cases`** - Top countries analysis for exploration
- **`rpt_covid_trends_over_time`** - Time series analysis for exploration
- **`rpt_correlation_analysis`** - Statistical analysis for exploration

#### 1. `rpt_top_countries_by_cases`
**Question Answered:** *"What are the top 5 most common values in a particular column, and what is their frequency?"*

- **Purpose:** Identifies the top 5 countries with the highest confirmed COVID-19 cases
- **Key Metrics:**
  - Occurrence frequency (number of regional records)
  - Total confirmed cases
  - Percentage of global cases
  - Severity tier classification
- **Source:** Built from `silver_covid_cases_global`
- **Use Cases:**
  - Executive dashboards showing global hotspots
  - Comparative country analysis
  - Resource allocation planning

#### 2. `rpt_covid_trends_over_time`
**Question Answered:** *"How does a particular metric change over time within the dataset?"*

- **Purpose:** Tracks the progression of COVID-19 metrics over time at the daily level
- **Key Metrics:**
  - Daily confirmed cases and deaths
  - Day-over-day changes and percentage changes
  - 7-day moving averages for trend smoothing
  - Trend direction indicators (Increasing/Decreasing/Stable)
- **Source:** Built from `silver_covid_cases_us`
- **Use Cases:**
  - Time series visualization
  - Trend analysis and forecasting
  - Policy impact assessment

#### 3. `rpt_correlation_analysis`
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

- **Source:** Built from `silver_covid_cases_us`
- **Use Cases:**
  - Understanding relationships between metrics
  - Validating hypotheses about disease spread
  - Research and epidemiological analysis

**Findings Interpretation:**
- **Strong Positive Correlation (Cases vs Deaths):** As confirmed cases increase, deaths typically increase proportionally, validating the severity of the pandemic.
- **Positive Correlation (Testing Rate vs Incident Rate):** Higher testing rates often detect more cases, increasing reported incident rates. This suggests that testing capacity affects case detection.
- **Positive Correlation (Hospitalization vs Mortality):** Higher hospitalization rates correlate with more severe outcomes, indicating the importance of healthcare capacity.
- **Negative Correlation Possible (Cases vs CFR):** More widespread testing (more cases) may show lower fatality ratios as mild cases are detected.

### 🎯 Data Mart Models (mart_*)

Optimized business marts built from the fact table. Use these for:
- Production dashboards and reporting
- Regular business use cases
- Best performance and consistency

#### 4. `mart_top_countries`
- **Based on:** `fct_covid_daily`
- **Purpose:** Production-ready top countries analysis
- **Benefits:** Simpler SQL, leverages pre-calculated metrics, better performance
- **Recommendation:** Use this over `rpt_top_countries_by_cases` for production

#### 5. `mart_covid_trends`
- **Based on:** `fct_covid_daily`
- **Purpose:** Production-ready time series analysis
- **Benefits:** No need to recalculate rolling averages or day-over-day changes
- **Recommendation:** Use this over `rpt_covid_trends_over_time` for production

#### 6. `mart_correlation_metrics`
- **Based on:** `fct_covid_daily`
- **Purpose:** Production-ready statistical correlation analysis
- **Benefits:** Consistent data source, additional correlation pairs possible
- **Recommendation:** Use this over `rpt_correlation_analysis` for production

### 📈 Aggregation Models (agg_*)

Pre-computed aggregations optimized for fast query performance and reporting.

#### 7. `agg_daily_summary`
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

#### 8. `agg_state_summary`
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

#### 9. `agg_country_summary`
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

### 1. **Clear Naming Conventions**
- Prefixes indicate model purpose (`fct_`, `agg_`, `rpt_`, `mart_`)
- No redundant layer prefixes (folder structure defines layer)
- Descriptive names that explain business purpose

### 2. **Clear Documentation**
- Every model includes detailed header comments explaining purpose and usage
- Column-level documentation in schema.yml
- README with comprehensive explanations

### 3. **Performance Optimization**
- Materialized as tables for fast query performance
- Pre-aggregated metrics to avoid repeated calculations
- Efficient CTEs for readability and optimization

### 4. **Code Organization**
- Logical CTEs with clear naming
- Separation of concerns (raw aggregation → derived metrics → final output)
- Consistent formatting and style

### 5. **Analytics Best Practices**
- Moving averages for trend smoothing
- Percentile rankings for relative comparisons
- Statistical measures (correlation, confidence levels)
- Derived metrics calculated consistently

### 6. **Business Context**
- Classifications and tiers for easy interpretation
- Percentage calculations for relative comparisons
- Trend indicators for quick insights
- Human-readable descriptions and interpretations

## Data Lineage

```
Silver Layer
├── silver_covid_cases_us       ─┬─────────────────────┐
└── silver_covid_cases_global   ─┘                     │
                                 │                      │ (exploration only)
                                 ▼                      ▼
                        fct_covid_daily         Report Models (rpt_*)
                     (SINGLE SOURCE OF TRUTH)   ├── rpt_top_countries...
                                 │               ├── rpt_covid_trends...
                                 │               └── rpt_correlation...
        ┌────────────────────────┼────────────────────────┐
        ▼                        ▼                        ▼
  Aggregation Models      Data Marts (mart_*)    (Future dimensions)
  ├── agg_daily_...       ├── mart_top_countries
  ├── agg_state_...       ├── mart_covid_trends
  └── agg_country_...     └── mart_correlation_...
  
  ALL PRODUCTION MODELS USE FACT TABLE ✅
```

**Architecture Principle:** The fact table is the **single source of truth** for all production models (aggregations and marts). Report models can access silver directly for exploration and custom analysis, but production dashboards should use models built from the fact table.

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

### Query Reports

```sql
-- Top countries report
SELECT 
    rank_by_cases,
    country_region,
    total_confirmed_cases,
    pct_of_global_cases,
    severity_tier
FROM {{ ref('rpt_top_countries_by_cases') }}
ORDER BY rank_by_cases;
```

### Query Data Marts

```sql
-- Trends data mart (optimized)
SELECT 
    report_date,
    province_state,
    cases_7day_moving_avg,
    case_trend,
    severity_category
FROM {{ ref('mart_covid_trends') }}
WHERE province_state = 'California'
ORDER BY report_date DESC;
```

### Query Aggregations

```sql
-- Daily national summary
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

## Model Comparison: Reports vs Data Marts

| Aspect | Report Models (rpt_*) | Data Mart Models (mart_*) |
|--------|----------------------|---------------------------|
| **Source** | Silver layer (direct) | Fact table |
| **Complexity** | More complex transformations | Simpler, leverages pre-calculated metrics |
| **Performance** | Repeated calculations | Pre-calculated metrics |
| **Consistency** | May have calculation differences | Guaranteed consistency |
| **Maintenance** | Changes needed in multiple places | Changes propagate from fact table |
| **Use Case** | Exploration, custom analysis | Production dashboards, regular reporting |
| **Recommendation** | Development and ad-hoc | **Production** ⭐ |

**Production Guidance:** Use data marts (`mart_*`) for dashboards and regular reporting. Use reports (`rpt_*`) for exploration and custom analysis.

## Maintenance

- **Dependencies:** 
  - **Fact table** depends on silver layer models
  - **Report models** depend on silver layer (for exploration)
  - **Data mart models** depend on fact table ✅
  - **Aggregation models** depend on fact table ✅
  - **All production models** use fact table as single source of truth
- **Refresh Frequency:** Should be refreshed when silver layer is updated
- **Build Order:**
  1. Silver layer models
  2. `fct_covid_daily` (core fact table)
  3. All gold layer models (reports, marts, aggregations)
- **Testing:** Run `dbt test --select 02-gold` to validate data quality

## Tags

Models are tagged for easy selection:

| Tag | Purpose | Models |
|-----|---------|--------|
| `gold` | All gold layer models | All |
| `fact` | Fact table models | `fct_covid_daily` |
| `core` | Core/foundational models | `fct_covid_daily` |
| `report` | Report models | `rpt_*` |
| `mart` | Data mart models | `mart_*` |
| `aggregation` | Pre-computed aggregations | `agg_*` |
| `from_silver` | Built directly from silver | `rpt_*` |
| `from_fact` | Built from fact table | `mart_*` |
| `time_series` | Time-based analysis | Trend models |
| `correlation` | Statistical correlation | Correlation models |
| `top_values` | Frequency/ranking analysis | Top countries models |
| `daily` | Daily grain aggregations | `agg_daily_summary` |
| `state` | State-level aggregations | `agg_state_summary` |
| `country` | Country-level aggregations | `agg_country_summary` |

Run specific tags:
```bash
# Build everything in gold layer
dbt run --select tag:gold

# Build only the fact table
dbt run --select tag:fact

# Build data marts (production models)
dbt run --select tag:mart

# Build reports (exploration models)
dbt run --select tag:report

# Build aggregations
dbt run --select tag:aggregation

# Test all models
dbt test --select tag:gold
```

## Performance Tips

1. **Start with the Fact Table:** Always build `fct_covid_daily` first as it's the foundation
2. **Use Data Marts for Production:** They're optimized and leverage pre-calculated metrics
3. **Use Reports for Exploration:** More flexible for custom analysis
4. **Materialize as Tables:** Gold models are materialized as tables for fast query performance
5. **Incremental Builds:** Consider incremental materialization for very large fact tables
6. **Indexing:** Add indexes on frequently filtered columns (report_date, country_region, province_state)

## Best Practices Summary

✅ **Dimensional Modeling** - Fact table with clear grain and dimensions  
✅ **Single Source of Truth** - Centralized fact table for all analytics  
✅ **Clear Naming** - Prefixes indicate model purpose (`fct_`, `agg_`, `rpt_`, `mart_`)  
✅ **Pre-calculated Metrics** - Avoid repeated computation  
✅ **Clear Documentation** - Every model and column documented  
✅ **Comprehensive Testing** - Data quality tests on all models  
✅ **Performance Optimization** - Table materialization, efficient CTEs  
✅ **Separation of Concerns** - Fact table separate from analytical logic  
✅ **Flexibility** - Reports for exploration, marts for production
