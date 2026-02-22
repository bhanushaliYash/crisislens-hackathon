-- ============================================================
-- CrisisLens — Databricks SQL Dashboard Queries
-- Copy each query into a separate panel in the Databricks SQL Dashboard
-- ============================================================
-- HOW TO USE:
--   1. In Databricks left sidebar → SQL → SQL Editor
--   2. Create a new query for each section below
--   3. Run it, then click "Add to Dashboard"
--   4. Create a new dashboard called "CrisisLens Humanitarian Intelligence"
-- ============================================================


-- ============================================================
-- PANEL 1: Key Metric Counter — Total People in Need
-- Visualization: Counter widget
-- ============================================================
SELECT
    ROUND(SUM(people_in_need_millions), 0) AS `Total People in Need (Millions)`,
    COUNT(*) AS `Active Crises Analyzed`,
    ROUND(AVG(coverage_rate_pct), 1) AS `Global Avg Funding Coverage (%)`,
    SUM(CASE WHEN is_overlooked THEN 1 ELSE 0 END) AS `Overlooked Crises`,
    ROUND(SUM(CASE WHEN funding_gap_musd > 0 THEN funding_gap_musd ELSE 0 END), 0) AS `Total Funding Gap ($M)`
FROM crisislens.gold_crisis_rankings;


-- ============================================================
-- PANEL 2: Top 15 Most Overlooked Crises (Main Table)
-- Visualization: Table (with color-coded rows)
-- ============================================================
SELECT
    global_overlook_rank                            AS `Rank`,
    country                                         AS `Country`,
    region                                          AS `Region`,
    primary_cluster                                 AS `Primary Sector`,
    ROUND(people_in_need_millions, 1)               AS `People in Need (M)`,
    ROUND(required_funding_musd, 0)                 AS `Required ($M)`,
    ROUND(funding_received_musd, 0)                 AS `Received ($M)`,
    ROUND(funding_gap_musd, 0)                      AS `Gap ($M)`,
    coverage_rate_pct                               AS `Coverage %`,
    ROUND(cbpf_allocation_musd, 1)                  AS `CBPF ($M)`,
    ROUND(overlook_score, 1)                        AS `Overlook Score`,
    attention_priority                              AS `Priority`,
    severity                                        AS `Severity`
FROM crisislens.gold_crisis_rankings
ORDER BY global_overlook_rank ASC
LIMIT 15;


-- ============================================================
-- PANEL 3: Regional Summary
-- Visualization: Bar chart (region vs avg_coverage_pct)
-- ============================================================
SELECT
    region                                          AS `Region`,
    num_crises                                      AS `Active Crises`,
    ROUND(total_people_in_need_M, 0)                AS `People in Need (M)`,
    ROUND(total_required_MUSD, 0)                   AS `Required ($M)`,
    ROUND(total_received_MUSD, 0)                   AS `Received ($M)`,
    ROUND(total_gap_MUSD, 0)                        AS `Total Gap ($M)`,
    avg_coverage_pct                                AS `Avg Coverage (%)`,
    ROUND(gap_per_person_in_need_usd, 2)            AS `Gap Per Person ($)`,
    overlooked_crisis_count                         AS `Overlooked Crises`
FROM crisislens.gold_regional_summary
ORDER BY avg_coverage_pct ASC;


-- ============================================================
-- PANEL 4: Sector (Cluster) Funding Gap
-- Visualization: Horizontal bar chart (cluster vs avg_coverage_pct)
-- ============================================================
SELECT
    cluster                                         AS `Humanitarian Sector`,
    num_crises                                      AS `Crises`,
    ROUND(total_pin_millions, 1)                    AS `People in Need (M)`,
    avg_coverage_pct                                AS `Avg Coverage (%)`,
    ROUND(total_gap_musd, 0)                        AS `Total Gap ($M)`,
    avg_cbpf_coverage_pct                           AS `Avg CBPF Coverage (%)`,
    ROUND(avg_overlook_score, 1)                    AS `Avg Overlook Score`
FROM crisislens.gold_cluster_summary
ORDER BY avg_overlook_score DESC;


-- ============================================================
-- PANEL 5: CBPF Mismatch — Where Pooled Funds Should Go
-- Visualization: Scatter plot (overlook_score vs mismatch_score)
-- ============================================================
SELECT
    country                                         AS `Country`,
    global_overlook_rank                            AS `Overlook Rank`,
    ROUND(overlook_score, 1)                        AS `Overlook Score`,
    coverage_rate_pct                               AS `Coverage %`,
    ROUND(funding_gap_musd, 0)                      AS `Funding Gap ($M)`,
    ROUND(country_cbpf_total_musd, 1)               AS `CBPF Received ($M)`,
    ROUND(cbpf_gap_coverage_pct, 1)                 AS `CBPF Gap Coverage %`,
    cbpf_adequacy                                   AS `CBPF Adequacy`,
    ROUND(mismatch_score, 1)                        AS `Mismatch Score`,
    ROUND(recommended_additional_cbpf_musd, 1)      AS `Recommended Additional CBPF ($M)`,
    attention_priority                              AS `Priority`
FROM crisislens.gold_cbpf_mismatch
WHERE mismatch_score > 0
ORDER BY mismatch_score DESC
LIMIT 15;


-- ============================================================
-- PANEL 6: Project Efficiency Outliers
-- Visualization: Table
-- ============================================================
SELECT
    project_code                                    AS `Project Code`,
    country                                         AS `Country`,
    cluster                                         AS `Sector`,
    organization                                    AS `Organization`,
    ROUND(budget_usd / 1000000.0, 2)               AS `Budget ($M)`,
    target_beneficiaries                            AS `Target Beneficiaries`,
    ROUND(ben_per_1000_usd, 1)                      AS `Efficiency (Ben/$1K)`,
    ROUND(cluster_avg_ben_ratio, 1)                 AS `Cluster Avg (Ben/$1K)`,
    ROUND(ben_ratio_zscore, 2)                      AS `Z-Score`,
    outlier_classification                          AS `Finding`,
    ROUND(funding_coverage_pct, 1)                  AS `Funding Coverage %`
FROM crisislens.gold_project_benchmarks
WHERE is_efficiency_outlier = true
ORDER BY ABS(ben_ratio_zscore) DESC
LIMIT 20;


-- ============================================================
-- PANEL 7: Benchmark Group Summary (for project comparison)
-- Visualization: Grouped bar chart
-- ============================================================
SELECT
    benchmark_label                                 AS `Benchmark Group`,
    COUNT(*)                                        AS `Projects in Group`,
    ROUND(AVG(budget_usd) / 1000000, 2)            AS `Avg Budget ($M)`,
    ROUND(AVG(target_beneficiaries), 0)             AS `Avg Beneficiaries`,
    ROUND(AVG(ben_per_1000_usd), 1)                 AS `Avg Efficiency (Ben/$1K)`,
    ROUND(AVG(funding_coverage_pct), 1)             AS `Avg Coverage %`,
    SUM(CASE WHEN is_efficiency_outlier THEN 1 ELSE 0 END) AS `Outlier Projects`
FROM crisislens.gold_project_benchmarks
GROUP BY benchmark_label
ORDER BY `Avg Budget ($M)` ASC;


-- ============================================================
-- PANEL 8: Severity 4 Crises — Emergency Watch List
-- Visualization: Table (highlight with red)
-- ============================================================
SELECT
    global_overlook_rank                            AS `Rank`,
    country                                         AS `Country`,
    ROUND(people_in_need_millions, 1)               AS `People in Need (M)`,
    ROUND(estimated_unfunded_people_millions, 1)    AS `Unfunded People (M)`,
    coverage_rate_pct                               AS `Coverage %`,
    ROUND(funding_gap_musd, 0)                      AS `Gap ($M)`,
    ROUND(funding_per_person_in_need_usd, 0)        AS `$/Person`,
    cbpf_adequacy                                   AS `Pooled Fund Status`,
    key_insight                                     AS `Key Finding`
FROM crisislens.gold_crisis_rankings
WHERE severity = 4
ORDER BY overlook_score DESC;
