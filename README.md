# 🌍 CrisisLens — Humanitarian Funding Gap Intelligence Platform

> **Databricks Geo-Insight Challenge Entry**
> *"Which Crises Are Most Overlooked?"*

[![Databricks](https://img.shields.io/badge/Built%20with-Databricks-FF3621?logo=databricks)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-003366)](https://delta.io)
[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 🎯 The Problem

Every year, billions of dollars flow into humanitarian response — but the distribution is profoundly unequal. Some crises receive more than 100% of their requested funding while others, with millions of people in desperate need, receive less than 20 cents of every dollar required.

**The UN OCHA needs a systematic, repeatable, data-driven way to detect these mismatches and alert decision-makers before funding windows close.**

CrisisLens answers three questions:
1. **Which active crises are most overlooked?** (high need, low coverage)
2. **Where are CBPF/Pooled Funds failing to fill the gap?** 
3. **Which humanitarian projects have unusual beneficiary-to-budget ratios?** (efficiency outliers)

---

## 🏗️ Architecture: Databricks Medallion

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES (External)                       │
│  HUMDATA HNO │ HUMDATA HRP │ OCHA FTS │ CBPF Hub │ COD Pop     │
└──────────────────────────┬──────────────────────────────────────┘
                           │ (API + CSV ingestion)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    🥉 BRONZE LAYER (Delta Lake)                  │
│  Raw data as-is, versioned, append-only audit trail             │
│  bronze_requirements_funding │ bronze_hrp_projects              │
│  bronze_population │ bronze_cbpf_allocations                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │ (clean + enrich)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    🥈 SILVER LAYER (Delta Lake)                  │
│  Cleaned, standardized, joined, validated                       │
│  silver_funding_analysis │ silver_hrp_projects                  │
│  silver_country_profiles                                        │
└──────────────────────────┬──────────────────────────────────────┘
                           │ (aggregate + score)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    🥇 GOLD LAYER (Delta Lake)                    │
│  Business-ready: rankings, scores, benchmarks                   │
│  gold_crisis_rankings │ gold_project_benchmarks                 │
│  gold_regional_summary │ gold_cluster_summary                   │
│  gold_cbpf_mismatch                                             │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              📊 DATABRICKS SQL DASHBOARD                         │
│  Interactive Map │ Gap Rankings │ Outlier Flags │ Benchmarks    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📁 Repository Structure

```
crisislens/
├── README.md                          ← You are here
├── requirements.txt                   ← Python dependencies
├── LICENSE
│
├── notebooks/
│   ├── 00_setup_and_config.py        ← Cluster config, database setup
│   ├── 01_data_ingestion.py          ← Download + Bronze Delta tables
│   ├── 02_data_cleaning.py           ← Silver: clean, join, validate
│   ├── 03_gap_scoring.py             ← Gold: Overlook Score algorithm
│   ├── 04_outlier_benchmarking.py    ← ML: outlier detection + KMeans
│   ├── 05_visualization.py           ← Charts, maps, final dashboard
│   └── 06_databricks_sql_queries.sql ← Copy-paste SQL for dashboard
│
├── data/
│   └── sample_data.csv               ← Small sample for quick testing
│
├── docs/
│   ├── METHODOLOGY.md                ← How the Overlook Score works
│   └── DATA_DICTIONARY.md            ← Every column explained
│
└── .github/
    └── workflows/
        └── validate.yml              ← Auto-validate notebooks on push
```

---

## 🔬 The Overlook Score — Methodology

The **Overlook Score** (0–100) is CrisisLens's core metric. It combines:

```
OverlookScore = (PeopleInNeed_M × Severity²) / CoverageRate

Where:
  PeopleInNeed_M  = Total people needing humanitarian assistance (millions)
  Severity        = OCHA severity classification (1-4, where 4 = worst)
  CoverageRate    = FundingReceived / FundingRequired (0.0 to 1.0+)

Then normalized to 0-100 across all active crises.
Score of 100 = Most overlooked crisis globally
Score of 0   = Best funded relative to need
```

Additionally we compute:
- **CBPF Mismatch Score**: Is the pooled fund going where the gap is largest?
- **Beneficiary Efficiency Z-Score**: Statistical outlier detection per cluster
- **Benchmark Similarity Score**: KMeans cosine similarity for project comparison

---

## 🚀 Quick Start

### Prerequisites
- Databricks workspace (any tier, including Community Edition)
- Cluster with DBR 13.3 LTS or higher

### Run in Order
```
Notebook 00 → 01 → 02 → 03 → 04 → 05
```
Each notebook prints a completion message and tells you what to run next.

---

## 📡 Data Sources

| Dataset | Source URL | Used For |
|---------|-----------|----------|
| Humanitarian Needs Overview | [HUMDATA HNO](https://data.humdata.org/dataset/global-hpc-hno) | People in Need per country |
| Humanitarian Response Plans | [HUMDATA HRP](https://data.humdata.org/dataset/humanitarian-response-plans) | Project-level budgets & targets |
| Global Requirements & Funding | [HUMDATA FTS](https://data.humdata.org/dataset/global-requirements-and-funding-data) | Requirement vs. actual funding |
| CBPF Pooled Funds | [CBPF Hub](https://cbpf.data.unocha.org/) | Pooled fund allocations |
| Population Data | [HUMDATA COD](https://data.humdata.org/dataset/cod-ps-global) | Per-capita normalization |

---

## 💡 Key Findings (2023 Data)

> **Sudan** has 18M people in need but received only 22.7% of its required funding — the worst severity-adjusted score among all active crises. CBPF allocations cover less than 6% of its funding gap.

> **Myanmar** and **CAR** (Central African Republic) are chronically overlooked: both have severity-4 or severity-3 ratings with coverage rates under 35%, yet receive minimal pooled fund support.

> **24% of humanitarian projects** flagged as efficiency outliers — either reaching far more or far fewer beneficiaries per dollar than comparable projects in the same cluster, suggesting scale-up or investigation opportunities.

---

## 👥 Who Benefits

This tool is designed for direct use by:
- **UN OCHA** — to prioritize donor outreach and pooled fund allocation decisions
- **Humanitarian coordinators** — to benchmark their projects and identify efficiency gaps
- **Donors** — to see where their money has the highest marginal impact
- **Researchers** — as a reproducible, open dataset for humanitarian financing analysis

---

## 🏆 Hackathon Submission

**Challenge**: Databricks Geo-Insight Challenge — "Which Crises Are Most Overlooked?"
**Team**: CrisisLens
**Tech Stack**: Databricks, Delta Lake, PySpark, Spark ML, Databricks SQL, Matplotlib, Folium

*All project code, data, and findings will be shared directly with UN OCHA teams.*
