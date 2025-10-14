# Gold Layer Architecture

## ✅ Improved Architecture (Current)

Following your excellent observation, **all production models now flow through the fact table** as the single source of truth!

### Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER                            │
│  ┌──────────────────────────┐  ┌──────────────────────────┐   │
│  │ silver_covid_cases_us    │  │ silver_covid_cases_global│   │
│  │ (State-level, daily)     │  │ (Country-level)          │   │
│  └──────────┬───────────────┘  └──────────┬───────────────┘   │
└─────────────┼──────────────────────────────┼───────────────────┘
              │                              │
              └──────────────┬───────────────┘
                             │
              ┌──────────────▼──────────────┐
              │                             │  (exploration only)
              │    fct_covid_daily          │◄────────────────┐
              │  SINGLE SOURCE OF TRUTH     │                 │
              │  ✅ All Production Models   │                 │
              │  • Pre-calculated metrics   │                 │
              │  • 816,923 rows            │                 │
              └──────────────┬──────────────┘                 │
                             │                                │
        ┌────────────────────┼────────────────────────────┐  │
        │                    │                            │  │
        ▼                    ▼                            ▼  │
┌──────────────┐   ┌──────────────────┐    ┌──────────────────┐
│Aggregations  │   │  Data Marts      │    │  Report Models   │
│(agg_*)       │   │  (mart_*)        │    │  (rpt_*)         │
│FROM FACT ✅  │   │  FROM FACT ✅    │    │  FROM SILVER     │
│              │   │                  │    │  (exploration)   │
│• Daily       │   │• Top Countries   │    │• Custom Analysis │
│• State       │   │• COVID Trends    │    │• Ad-hoc Queries  │
│• Country     │   │• Correlation     │    │• Testing New     │
│              │   │                  │    │  Patterns        │
│892 rows      │   │51,754 rows       │    │51,754 rows       │
│59 rows       │   │5 rows            │    │5 rows            │
│202 rows      │   │5 rows            │    │4 rows            │
└──────────────┘   └──────────────────┘    └──────────────────┘
   
   PRODUCTION         PRODUCTION            EXPLORATION
   ⭐⭐⭐            ⭐⭐⭐               🔬
```

## Model Categories by Source

### 🏗️ Fact Table (Core Foundation)

| Model | Source | Purpose |
|-------|--------|---------|
| `fct_covid_daily` | Silver | Single source of truth, 816K rows |

### ✅ Models Built FROM FACT TABLE (Production)

| Model | Type | Rows | Purpose |
|-------|------|------|---------|
| `agg_daily_summary` | Aggregation | 892 | Daily US national totals |
| `agg_state_summary` | Aggregation | 59 | State-level summaries |
| `agg_country_summary` | Aggregation | 202 | Country-level summaries |
| `mart_top_countries` | Data Mart | 5 | Top 5 countries analysis |
| `mart_covid_trends` | Data Mart | 51,754 | Time series trends |
| `mart_correlation_metrics` | Data Mart | 5 | Statistical correlations |

**Total: 6 production models ✅**

### 🔬 Models Built FROM SILVER (Exploration)

| Model | Type | Rows | Purpose |
|-------|------|------|---------|
| `rpt_top_countries_by_cases` | Report | 5 | Custom top countries |
| `rpt_covid_trends_over_time` | Report | 51,754 | Custom time series |
| `rpt_correlation_analysis` | Report | 4 | Custom correlations |

**Total: 3 exploration models**

## Benefits of This Architecture

### ✅ Single Source of Truth
- **All production models** use `fct_covid_daily`
- Guaranteed data consistency across dashboards
- No discrepancies between different reports

### ✅ Performance
- Pre-calculated metrics in fact table
- No repeated calculations across models
- Faster query times for production dashboards

### ✅ Maintainability
- One place to update business logic
- Changes propagate to all downstream models
- Easier debugging and testing

### ✅ Flexibility
- Report models still available for exploration
- Can test new metrics without rebuilding fact table
- Balance between consistency and flexibility

## When to Use Each Model Type

### Use Aggregations (`agg_*`)
- ✅ Executive dashboards
- ✅ High-level KPIs
- ✅ National/State/Country summaries
- ✅ Fast, pre-computed totals

### Use Data Marts (`mart_*`)
- ✅ Production dashboards
- ✅ Regular business reporting
- ✅ Performance-critical queries
- ✅ Standard analytical patterns

### Use Reports (`rpt_*`)
- 🔬 Exploration and analysis
- 🔬 Testing new hypotheses
- 🔬 Custom transformations
- 🔬 One-off analyses

## Tags for Easy Selection

```bash
# Build ALL production models (recommended)
dbt run --select tag:from_fact

# Build just aggregations
dbt run --select tag:aggregation

# Build just data marts
dbt run --select tag:mart

# Build exploration reports
dbt run --select tag:from_silver
```

## Dependency Graph

```
silver_covid_cases_us ─┬─→ fct_covid_daily ─→ agg_daily_summary
                       │                    ├─→ agg_state_summary
silver_covid_cases_global ─┘                ├─→ agg_country_summary
                                            ├─→ mart_top_countries
                                            ├─→ mart_covid_trends
                                            └─→ mart_correlation_metrics

silver_covid_cases_us ─→ rpt_covid_trends_over_time
                      ├─→ rpt_correlation_analysis
                      
silver_covid_cases_global ─→ rpt_top_countries_by_cases
```

## Build Order

1. **Silver Layer** - Source data cleaned and standardized
2. **Fact Table** - `fct_covid_daily` (816K rows)
3. **Production Models** - All `agg_*` and `mart_*` models (from fact)
4. **Exploration Models** - All `rpt_*` models (from silver)

```bash
# Recommended build sequence
dbt run --select silver  # Step 1
dbt run --select fct_covid_daily  # Step 2
dbt run --select tag:from_fact  # Step 3 (production)
dbt run --select tag:from_silver  # Step 4 (exploration)
```

## Key Metrics

| Metric | Value |
|--------|-------|
| **Fact Table Rows** | 816,923 |
| **Production Models** | 6 (all from fact) ✅ |
| **Exploration Models** | 3 (from silver) |
| **Total Gold Models** | 10 (1 fact + 6 prod + 3 explore) |

## Architecture Principles

1. ✅ **Fact table is single source of truth** for production
2. ✅ **All aggregations use fact table** for consistency
3. ✅ **All data marts use fact table** for performance
4. 🔬 **Report models can use silver** for exploration
5. ✅ **Tag-based organization** for easy selection
6. ✅ **Clear naming conventions** indicate purpose and source

This architecture ensures **data consistency, performance, and flexibility** for both production dashboards and exploratory analysis! 🎉

