# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 CrisisLens — Notebook 02: Data Cleaning & Silver Layer
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Reads raw Bronze tables
# MAGIC - Cleans and validates the data (remove nulls, fix types, handle edge cases)
# MAGIC - Enriches with calculated fields (derived metrics that power our analysis)
# MAGIC - Joins related tables together
# MAGIC - Saves as Silver Delta tables (clean, analysis-ready)
# MAGIC
# MAGIC **Output tables:**
# MAGIC - `crisislens.silver_funding_analysis` — Enriched country-level funding data
# MAGIC - `crisislens.silver_hrp_projects` — Enriched project data with z-scores
# MAGIC - `crisislens.silver_cbpf_enriched` — CBPF data joined with funding gaps

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, StringType
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

spark.sql("USE crisislens")
print("✅ Connected to crisislens database")
print("🔄 Starting Silver layer processing...\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Clean Dataset 1: Requirements & Funding
# MAGIC
# MAGIC Key enrichments we add:
# MAGIC 1. **Funding Gap** — absolute dollars missing
# MAGIC 2. **Coverage Rate %** — what % of required funding was received
# MAGIC 3. **Funding per Person in Need** — USD per PIN (low = underfunded per capita)
# MAGIC 4. **Need Score** — composite: People × Severity (how bad is this crisis?)
# MAGIC 5. **Overlook Score (Raw)** — Need Score / Coverage Rate (the key metric)
# MAGIC 6. **CBPF Gap Coverage %** — what % of the gap do pooled funds fill?
# MAGIC 7. **Funding Category** — human-readable label for coverage level

# COMMAND ----------

# Load Bronze table
bronze_funding = spark.table("crisislens.bronze_requirements_funding")
print(f"📥 Loaded bronze_requirements_funding: {bronze_funding.count()} rows")

# ============================================================
# DATA QUALITY CHECKS
# ============================================================
print("\n🔍 Data Quality Checks:")

# Check for nulls in critical columns
critical_cols = ["country", "people_in_need_millions", "required_funding_musd", "funding_received_musd"]
for col in critical_cols:
    null_count = bronze_funding.filter(F.col(col).isNull()).count()
    status = "✅" if null_count == 0 else f"⚠️  {null_count} nulls"
    print(f"  {status} {col}")

# Check for logical impossibilities
impossible = bronze_funding.filter(
    (F.col("required_funding_musd") <= 0) |
    (F.col("people_in_need_millions") <= 0) |
    (F.col("severity") < 1) |
    (F.col("severity") > 4)
).count()
print(f"  {'✅' if impossible == 0 else '⚠️  ' + str(impossible)} logical validation checks")

# COMMAND ----------

# ============================================================
# ENRICHMENT — Add all derived metrics
# ============================================================

silver_funding = bronze_funding.withColumn(
    # ----- METRIC 1: Funding Gap (absolute dollars) -----
    # How much money is MISSING from what was requested?
    # Positive = underfunded, Negative = overfunded (rare)
    "funding_gap_musd",
    F.round(
        F.col("required_funding_musd") - F.col("funding_received_musd"),
        2
    )

).withColumn(
    # ----- METRIC 2: Coverage Rate -----
    # What fraction of required funding was actually received?
    # 1.0 = 100% covered, 0.3 = only 30 cents per dollar received
    # We cap at 1.5 (150%) to handle rare over-funding cases
    "coverage_rate",
    F.round(
        F.least(
            F.col("funding_received_musd") / F.greatest(F.col("required_funding_musd"), F.lit(0.001)),
            F.lit(1.5)   # Cap at 150% to prevent extreme outliers skewing the math
        ), 4
    )

).withColumn(
    # ----- METRIC 3: Coverage Rate as Percentage -----
    # Human-readable version of coverage_rate
    "coverage_rate_pct",
    F.round(F.col("coverage_rate") * 100, 1)

).withColumn(
    # ----- METRIC 4: Funding per Person in Need -----
    # USD per person who needs assistance
    # Context: OCHA estimates ~$100-500 per person per year for basic needs
    # Much lower than this = extremely underfunded
    "funding_per_person_in_need_usd",
    F.round(
        (F.col("funding_received_musd") * 1_000_000) /
        F.greatest(F.col("people_in_need_millions") * 1_000_000, F.lit(1)),
        2
    )

).withColumn(
    # ----- METRIC 5: Need Score (composite severity index) -----
    # Combines scale (people) with severity (how bad)
    # Severity is squared to give more weight to worst crises
    # A severity-4 crisis with 10M PIN = 4² × 10 = 160
    # A severity-2 crisis with 10M PIN = 2² × 10 = 40
    "need_score",
    F.round(
        F.col("people_in_need_millions") * (F.col("severity") * F.col("severity")),
        3
    )

).withColumn(
    # ----- METRIC 6: Raw Overlook Score -----
    # THE CORE METRIC: How "overlooked" is this crisis?
    # Formula: Need Score / Coverage Rate
    # High need + low coverage = HIGH score = more overlooked
    # We add a small epsilon to prevent division by zero on fully-funded crises
    "overlook_score_raw",
    F.round(
        F.col("need_score") /
        F.greatest(F.col("coverage_rate"), F.lit(0.01)),
        4
    )

).withColumn(
    # ----- METRIC 7: CBPF Pooled Fund Need Coverage % -----
    # What % of the TOTAL REQUIRED funding comes from pooled funds?
    # Low = pooled funds not engaged with this crisis
    "cbpf_need_coverage_pct",
    F.round(
        (F.col("cbpf_allocation_musd") / F.greatest(F.col("required_funding_musd"), F.lit(0.001))) * 100,
        2
    )

).withColumn(
    # ----- METRIC 8: CBPF Gap Coverage % -----
    # What % of the FUNDING GAP do pooled funds fill?
    # If gap = $500M and CBPF = $50M → 10% gap coverage
    # Very low = pooled funds not helping where help is most needed
    "cbpf_gap_coverage_pct",
    F.round(
        (F.col("cbpf_allocation_musd") /
         F.greatest(
             F.col("required_funding_musd") - F.col("funding_received_musd"),
             F.lit(0.001)
         )) * 100,
        2
    )

).withColumn(
    # ----- METRIC 9: Is Overlooked? (binary flag) -----
    # Simple boolean: does this crisis meet our "overlooked" definition?
    # Definition: coverage < 40% AND severity >= 3
    "is_overlooked",
    F.when(
        (F.col("coverage_rate_pct") < 40) & (F.col("severity") >= 3),
        True
    ).otherwise(False)

).withColumn(
    # ----- METRIC 10: Funding Category -----
    # Human-readable label for use in dashboard and reports
    "funding_category",
    F.when(F.col("coverage_rate_pct") >= 100, "Fully Funded (≥100%)")
     .when(F.col("coverage_rate_pct") >= 70,  "Mostly Funded (70–99%)")
     .when(F.col("coverage_rate_pct") >= 40,  "Partially Funded (40–69%)")
     .when(F.col("coverage_rate_pct") >= 20,  "Critically Underfunded (20–39%)")
     .otherwise("Severely Underfunded (<20%)")

).withColumn(
    # ----- METRIC 11: Unfunded People Estimate -----
    # Rough estimate of how many people aren't being reached due to funding gap
    # = People in Need × (1 - coverage_rate)
    "estimated_unfunded_people_millions",
    F.round(
        F.col("people_in_need_millions") * F.greatest(1 - F.col("coverage_rate"), F.lit(0)),
        2
    )
)

# ============================================================
# NORMALIZE OVERLOOK SCORE to 0-100 scale
# ============================================================
# We do min-max normalization so scores are comparable across crises
# Score of 100 = most overlooked, Score of 0 = best funded relative to need

max_raw = silver_funding.agg(F.max("overlook_score_raw").alias("m")).collect()[0].m
min_raw = silver_funding.agg(F.min("overlook_score_raw").alias("m")).collect()[0].m

print(f"\n📊 Overlook Score range: {min_raw:.1f} to {max_raw:.1f} (will normalize to 0-100)")

silver_funding = silver_funding.withColumn(
    "overlook_score",   # Final normalized score (0-100)
    F.round(
        ((F.col("overlook_score_raw") - min_raw) / (max_raw - min_raw)) * 100,
        2
    )
)

# Add ranking
window_global_rank = Window.orderBy(F.col("overlook_score").desc())
silver_funding = silver_funding.withColumn(
    "global_overlook_rank",
    F.rank().over(window_global_rank)
)

# COMMAND ----------

# Save Silver funding table
(silver_funding
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("crisislens.silver_funding_analysis"))

print(f"\n✅ Saved silver_funding_analysis: {silver_funding.count()} rows")
print(f"\n📊 Top 5 Most Overlooked Crises:")
display(silver_funding.select(
    "global_overlook_rank", "country", "severity", "people_in_need_millions",
    "coverage_rate_pct", "funding_gap_musd", "overlook_score", "funding_category"
).orderBy("global_overlook_rank").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Clean Dataset 2: HRP Project-Level Data
# MAGIC
# MAGIC Key enrichments:
# MAGIC 1. **Budget per Beneficiary** — cost efficiency metric
# MAGIC 2. **Z-Score within Cluster** — statistical outlier detection
# MAGIC 3. **Is Outlier** — flag projects that deviate significantly from cluster norms
# MAGIC 4. **Outlier Direction** — high efficiency (scale-up?) or low efficiency (investigate?)

# COMMAND ----------

bronze_hrp = spark.table("crisislens.bronze_hrp_projects")
print(f"📥 Loaded bronze_hrp_projects: {bronze_hrp.count()} rows")

# ---- First, add basic derived columns ----
silver_hrp = bronze_hrp.withColumn(
    # Budget per beneficiary (USD) — core efficiency metric
    # Lower = reaches more people per dollar (more efficient)
    # But very low could also mean underreporting beneficiaries
    "budget_per_beneficiary_usd",
    F.round(
        F.col("budget_usd") / F.greatest(F.col("target_beneficiaries"), F.lit(1)),
        2
    )
).withColumn(
    # Funding gap for this specific project
    "project_funding_gap_usd",
    F.greatest(F.col("budget_usd") - F.col("funding_received_usd"), F.lit(0))
).withColumn(
    # Make sure beneficiaries_per_1000_usd exists (calculate if missing)
    "ben_per_1000_usd",
    F.when(
        F.col("beneficiaries_per_1000_usd").isNotNull(),
        F.col("beneficiaries_per_1000_usd")
    ).otherwise(
        F.round(
            F.col("target_beneficiaries") / (F.col("budget_usd") / 1000),
            3
        )
    )
)

# ---- Z-Score calculation using Window functions ----
# We calculate z-score WITHIN each cluster (compare like with like)
# A food security project shouldn't be compared with a logistics project

window_cluster = Window.partitionBy("cluster")

silver_hrp = silver_hrp.withColumn(
    # Average ben_per_1000 for all projects in this cluster
    "cluster_avg_ben_ratio",
    F.round(F.avg("ben_per_1000_usd").over(window_cluster), 3)

).withColumn(
    # Standard deviation for this cluster
    "cluster_std_ben_ratio",
    F.round(F.stddev("ben_per_1000_usd").over(window_cluster), 3)

).withColumn(
    # Z-score: how many standard deviations from the cluster average?
    # Z = (value - mean) / std_dev
    # Positive Z = more beneficiaries per dollar than average (high efficiency)
    # Negative Z = fewer beneficiaries per dollar than average (low efficiency)
    "ben_ratio_zscore",
    F.round(
        (F.col("ben_per_1000_usd") - F.col("cluster_avg_ben_ratio")) /
        F.greatest(F.col("cluster_std_ben_ratio"), F.lit(0.001)),
        3
    )

).withColumn(
    # Flag as outlier if |z-score| > 1.5 (deviates significantly from cluster norm)
    # 1.5 standard deviations covers ~87% of normal distribution
    # Projects outside this range deserve investigation
    "is_efficiency_outlier",
    F.abs(F.col("ben_ratio_zscore")) > 1.5

).withColumn(
    # Classify the outlier type — helps prioritize action
    "outlier_classification",
    F.when(
        F.col("ben_ratio_zscore") > 2.0,
        "🟢 Highly Efficient — Investigate for scale-up"
    ).when(
        F.col("ben_ratio_zscore") > 1.5,
        "🟡 Above Average Efficiency — Monitor"
    ).when(
        F.col("ben_ratio_zscore") < -2.0,
        "🔴 Very Low Efficiency — Investigate bottleneck"
    ).when(
        F.col("ben_ratio_zscore") < -1.5,
        "🟠 Below Average Efficiency — Review methodology"
    ).otherwise(
        "✅ Within Normal Range"
    )

).withColumn(
    # Country-level funding context (how well-funded is this crisis overall?)
    # This will be enriched via join in the Gold layer
    "funding_gap_flag",
    F.when(F.col("funding_coverage_pct") < 30, "Critically Underfunded Project")
     .when(F.col("funding_coverage_pct") < 60, "Underfunded Project")
     .when(F.col("funding_coverage_pct") < 90, "Mostly Funded Project")
     .otherwise("Fully Funded Project")
)

# Save Silver HRP table
(silver_hrp
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("crisislens.silver_hrp_projects"))

outlier_count = silver_hrp.filter(F.col("is_efficiency_outlier") == True).count()
print(f"\n✅ Saved silver_hrp_projects: {silver_hrp.count()} rows")
print(f"   🔍 Efficiency outliers detected: {outlier_count} ({outlier_count/silver_hrp.count()*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Clean Dataset 3: CBPF Enriched with Gap Context
# MAGIC
# MAGIC Join CBPF allocations with funding gap data to answer:
# MAGIC "Is pooled fund money going to the most underfunded crises?"

# COMMAND ----------

bronze_cbpf = spark.table("crisislens.bronze_cbpf_allocations")

# Join with silver funding to get the funding gap context for each CBPF allocation
silver_cbpf = bronze_cbpf.join(
    silver_funding.select(
        "iso3", "overlook_score", "coverage_rate_pct",
        "funding_gap_musd", "people_in_need_millions", "funding_category"
    ),
    on="iso3",
    how="left"   # Keep all CBPF rows even if country not in funding table
).withColumn(
    # Allocation efficiency: how much CBPF per person in need?
    "cbpf_per_person_in_need_usd",
    F.round(
        (F.col("allocation_musd") * 1_000_000) /
        F.greatest(F.col("people_in_need_millions") * 1_000_000, F.lit(1)),
        2
    )
).withColumn(
    # Is this CBPF allocation going to an overlooked crisis?
    "allocated_to_overlooked_crisis",
    F.when(F.col("coverage_rate_pct") < 40, True).otherwise(False)
).withColumn(
    # Mismatch flag: high overlook score but getting CBPF support?
    # Low overlook score but getting large CBPF? That's a mismatch
    "cbpf_mismatch_type",
    F.when(
        (F.col("overlook_score") > 60) & (F.col("allocation_musd") < 20),
        "🔴 Overlooked Crisis — UNDERFUNDED by Pooled Funds"
    ).when(
        (F.col("overlook_score") < 30) & (F.col("allocation_musd") > 30),
        "⚠️  Well-Funded Crisis — Receiving Pooled Funds"
    ).otherwise(
        "✅ Reasonable Allocation"
    )
)

(silver_cbpf
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("crisislens.silver_cbpf_enriched"))

print(f"✅ Saved silver_cbpf_enriched: {silver_cbpf.count()} rows")
mismatch_count = silver_cbpf.filter(F.col("cbpf_mismatch_type").contains("Overlooked")).count()
print(f"   ⚠️  CBPF Mismatches found (overlooked but underfunded by pooled funds): {mismatch_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Silver Layer Complete

# COMMAND ----------

print("=" * 65)
print("🥈 SILVER LAYER SUMMARY")
print("=" * 65)

silver_tables = [
    ("silver_funding_analysis", "Country-level funding gaps + Overlook Scores"),
    ("silver_hrp_projects", "Project-level efficiency analysis + z-scores"),
    ("silver_cbpf_enriched", "CBPF allocations with gap context + mismatch flags")
]

for table_name, description in silver_tables:
    count = spark.sql(f"SELECT COUNT(*) as c FROM crisislens.{table_name}").collect()[0].c
    cols = len(spark.table(f"crisislens.{table_name}").columns)
    print(f"\n  ✅ {table_name}")
    print(f"     {description}")
    print(f"     {count:,} rows | {cols} columns")

print("\n🎉 Notebook 02 Complete! ➡️ Run Notebook 03 next")
