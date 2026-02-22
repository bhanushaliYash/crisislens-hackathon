# 📖 CrisisLens — Data Dictionary

All tables live in the `crisislens` database in Databricks.

---

## Bronze Tables (Raw Data)

### `bronze_requirements_funding`
| Column | Type | Source | Description |
|--------|------|--------|-------------|
| country | string | OCHA FTS | Country name |
| iso3 | string | Standard | ISO 3-letter country code |
| year | int | OCHA FTS | Year of data (2023) |
| people_in_need_millions | double | HNO | Estimated people requiring humanitarian assistance |
| required_funding_musd | double | HRP | Total funding requested in HRP ($M) |
| funding_received_musd | double | OCHA FTS | Actual funding received ($M) |
| cbpf_allocation_musd | double | CBPF Hub | CBPF/Pooled fund allocations ($M) |
| severity | int | OCHA | Crisis severity: 1=minimal, 2=stressed, 3=crisis, 4=emergency/catastrophe |
| primary_cluster | string | HRP | Lead humanitarian sector |
| region | string | CrisisLens | Geographic region classification |
| latitude | double | GIS | Latitude for map rendering |
| longitude | double | GIS | Longitude for map rendering |
| population_millions | double | COD | Total country population |

### `bronze_hrp_projects`
| Column | Type | Source | Description |
|--------|------|--------|-------------|
| project_code | string | HRP | Unique project identifier |
| country | string | HRP | Country of implementation |
| iso3 | string | Standard | ISO 3-letter code |
| cluster | string | HRP | Humanitarian cluster/sector |
| organization | string | HRP | Implementing organization (WFP, UNICEF, IRC, etc.) |
| year | int | HRP | Year |
| budget_usd | long | HRP | Total project budget in USD |
| target_beneficiaries | long | HRP | Number of people the project aims to assist |
| funding_received_usd | long | HRP | Actual funding received for this project |
| beneficiaries_per_1000_usd | double | Derived | Efficiency: beneficiaries per $1,000 budget |
| funding_coverage_pct | double | Derived | Funding received / budget × 100 |

### `bronze_cbpf_allocations`
| Column | Type | Source | Description |
|--------|------|--------|-------------|
| country | string | CBPF Hub | Country |
| iso3 | string | Standard | ISO 3-letter code |
| cluster | string | CBPF Hub | Sector receiving allocation |
| allocation_kusd | double | CBPF Hub | Allocation in thousands USD |
| allocation_musd | double | Derived | Allocation in millions USD |
| year | int | CBPF Hub | Year |
| allocation_type | string | CBPF Hub | Standard grant or Rapid Response |

---

## Silver Tables (Cleaned + Enriched)

### `silver_funding_analysis`
All Bronze columns plus:
| Column | Type | Formula | Description |
|--------|------|---------|-------------|
| funding_gap_musd | double | required - received | Absolute funding gap ($M) |
| coverage_rate | double | received / required | Coverage as decimal (0-1.5) |
| coverage_rate_pct | double | × 100 | Coverage as percentage |
| funding_per_person_in_need_usd | double | received / PIN | USD per person in need |
| need_score | double | PIN × severity² | Composite need index |
| overlook_score_raw | double | need_score / coverage | Raw overlook score |
| overlook_score | double | min-max normalized | Final score (0-100) |
| cbpf_need_coverage_pct | double | cbpf / required × 100 | CBPF as % of total requirement |
| cbpf_gap_coverage_pct | double | cbpf / gap × 100 | CBPF as % of funding gap |
| is_overlooked | boolean | coverage<40% AND severity≥3 | Overlooked crisis flag |
| funding_category | string | rule-based | Human-readable funding level |
| estimated_unfunded_people_millions | double | PIN × (1-coverage) | People without adequate support |

### `silver_hrp_projects`
All Bronze HRP columns plus:
| Column | Type | Description |
|--------|------|-------------|
| budget_per_beneficiary_usd | double | USD per targeted beneficiary |
| project_funding_gap_usd | double | Budget minus funding received |
| ben_per_1000_usd | double | Beneficiaries per $1,000 budget |
| cluster_avg_ben_ratio | double | Window avg within cluster |
| cluster_std_ben_ratio | double | Window std dev within cluster |
| ben_ratio_zscore | double | Z-score vs cluster (outlier signal) |
| is_efficiency_outlier | boolean | |z| > 1.5 |
| outlier_classification | string | Labeled finding |
| funding_gap_flag | string | Project-level funding label |

---

## Gold Tables (Analytics-Ready)

### `gold_crisis_rankings` — Primary output table
| Column | Description |
|--------|-------------|
| global_overlook_rank | Rank 1-N (1 = most overlooked globally) |
| regional_overlook_rank | Rank within geographic region |
| attention_priority | 🔴/🟠/🟡/🟢 priority label |
| cbpf_adequacy | Assessment of pooled fund engagement |
| key_insight | Auto-generated one-line finding |
| + all Silver columns | |

### `gold_regional_summary`
Aggregated by region: totals and averages across all crises.

### `gold_cluster_summary`
Aggregated by humanitarian cluster/sector.

### `gold_project_benchmarks`
Silver HRP data plus:
| Column | Description |
|--------|-------------|
| benchmark_group | KMeans cluster assignment (0-5) |
| benchmark_label | Human-readable group label |

### `gold_cbpf_mismatch`
| Column | Description |
|--------|-------------|
| mismatch_score | overlook_score × (1 - cbpf_gap_coverage%) |
| recommended_additional_cbpf_musd | Estimated additional needed to reach 15% gap coverage |
