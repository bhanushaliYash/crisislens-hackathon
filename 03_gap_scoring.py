# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 CrisisLens — Notebook 03: Gap Scoring Engine (Gold Layer)
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Produces the final Gold tables — business-ready, executive-facing analytics
# MAGIC - Generates the definitive crisis rankings (most to least overlooked)
# MAGIC - Creates regional and sector summaries
# MAGIC - Produces the CBPF mismatch analysis
# MAGIC
# MAGIC **Output tables:**
# MAGIC - `crisislens.gold_crisis_rankings` — Full ranked list of crises
# MAGIC - `crisislens.gold_regional_summary` — Aggregated by geographic region
# MAGIC - `crisislens.gold_cluster_summary` — Aggregated by humanitarian sector
# MAGIC - `crisislens.gold_cbpf_mismatch` — Where pooled funds are misallocated
# MAGIC
# MAGIC **This is what judges will see in the dashboard.**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE crisislens")
print("✅ Connected to crisislens database")
print("🏆 Building Gold layer analytics...\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏅 Gold Table 1: Crisis Rankings
# MAGIC
# MAGIC The main output: a ranked table of all active crises from most to least overlooked.
# MAGIC Each row tells a complete story about one country's humanitarian situation.

# COMMAND ----------

silver = spark.table("crisislens.silver_funding_analysis")

gold_rankings = silver.withColumn(
    # ---- GLOBAL OVERLOOK RANK ----
    # Rank 1 = most overlooked crisis in the world
    "global_overlook_rank",
    F.rank().over(Window.orderBy(F.col("overlook_score").desc()))

).withColumn(
    # ---- REGIONAL RANK ----
    # Rank within their geographic region (useful for regional coordinators)
    "regional_overlook_rank",
    F.rank().over(
        Window.partitionBy("region").orderBy(F.col("overlook_score").desc())
    )

).withColumn(
    # ---- ATTENTION PRIORITY LABEL ----
    # Clear, actionable label for dashboard display
    "attention_priority",
    F.when(F.col("global_overlook_rank") <= 3,  "🔴 CRITICAL — Immediate attention required")
     .when(F.col("global_overlook_rank") <= 7,  "🟠 HIGH — Significant funding gap")
     .when(F.col("global_overlook_rank") <= 12, "🟡 MEDIUM — Monitor closely")
     .otherwise("🟢 LOW — Relatively well funded")

).withColumn(
    # ---- CBPF ADEQUACY ----
    # Simple assessment: is pooled fund support adequate?
    "cbpf_adequacy",
    F.when(F.col("cbpf_gap_coverage_pct") < 5,  "🔴 No Pooled Fund Engagement")
     .when(F.col("cbpf_gap_coverage_pct") < 15, "🟠 Minimal Pooled Fund Support")
     .when(F.col("cbpf_gap_coverage_pct") < 30, "🟡 Some Pooled Fund Support")
     .otherwise("✅ Adequate Pooled Fund Support")

).withColumn(
    # ---- KEY INSIGHT (generated automatically for each crisis) ----
    # This creates a one-line finding that could appear in a UN brief
    "key_insight",
    F.concat_ws(" ",
        F.col("country"),
        F.lit("has"),
        F.round(F.col("people_in_need_millions"), 1).cast("string"),
        F.lit("million people in need but only"),
        F.col("coverage_rate_pct").cast("string"),
        F.lit("% funded —"),
        F.round(F.col("estimated_unfunded_people_millions"), 1).cast("string"),
        F.lit("million people without adequate support.")
    )
)

# Select and order final columns
gold_rankings = gold_rankings.select(
    "global_overlook_rank",
    "regional_overlook_rank",
    "country",
    "iso3",
    "region",
    "primary_cluster",
    "latitude",
    "longitude",
    "year",
    "severity",
    "people_in_need_millions",
    "population_millions",
    "required_funding_musd",
    "funding_received_musd",
    "cbpf_allocation_musd",
    "funding_gap_musd",
    "coverage_rate_pct",
    "cbpf_need_coverage_pct",
    "cbpf_gap_coverage_pct",
    "funding_per_person_in_need_usd",
    "need_score",
    "overlook_score",
    "estimated_unfunded_people_millions",
    "is_overlooked",
    "funding_category",
    "attention_priority",
    "cbpf_adequacy",
    "key_insight"
).orderBy("global_overlook_rank")

(gold_rankings
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("crisislens.gold_crisis_rankings"))

print(f"✅ Saved gold_crisis_rankings: {gold_rankings.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌍 Print the Key Findings

# COMMAND ----------

print("=" * 70)
print("🌍 CRISISLENS — TOP 10 MOST OVERLOOKED HUMANITARIAN CRISES (2023)")
print("=" * 70)
print(f"{'Rank':<5} {'Country':<15} {'PIN(M)':<8} {'Required':<12} {'Received':<12} {'Coverage':<10} {'Score'}")
print("-" * 70)

top_crises = spark.sql("""
    SELECT global_overlook_rank, country, people_in_need_millions,
           required_funding_musd, funding_received_musd,
           coverage_rate_pct, overlook_score, attention_priority
    FROM crisislens.gold_crisis_rankings
    ORDER BY global_overlook_rank
    LIMIT 10
""").collect()

for row in top_crises:
    print(f"#{row.global_overlook_rank:<4} {row.country:<15} {row.people_in_need_millions:<8.1f} "
          f"${row.required_funding_musd:<10.0f}M ${row.funding_received_musd:<10.0f}M "
          f"{row.coverage_rate_pct:<9.1f}% {row.overlook_score:.1f}")

display(gold_rankings.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📍 Gold Table 2: Regional Summary

# COMMAND ----------

gold_regional = spark.sql("""
    SELECT
        region,
        COUNT(*) as num_crises,                                           -- How many crises in this region
        ROUND(SUM(people_in_need_millions), 1) as total_people_in_need_M, -- Total scale of need
        ROUND(SUM(required_funding_musd), 0) as total_required_MUSD,
        ROUND(SUM(funding_received_musd), 0) as total_received_MUSD,
        ROUND(SUM(funding_gap_musd), 0) as total_gap_MUSD,               -- Total unfunded need
        ROUND(AVG(coverage_rate_pct), 1) as avg_coverage_pct,            -- Average funding coverage
        ROUND(MIN(coverage_rate_pct), 1) as min_coverage_pct,            -- Worst-funded crisis in region
        ROUND(SUM(cbpf_allocation_musd), 1) as total_cbpf_MUSD,
        ROUND(AVG(overlook_score), 1) as avg_overlook_score,
        -- What's the $ gap per person in need for this whole region?
        ROUND(
            SUM(funding_gap_musd) * 1000000 / (SUM(people_in_need_millions) * 1000000),
            2
        ) as gap_per_person_in_need_usd,
        -- Count of overlooked crises
        SUM(CASE WHEN is_overlooked THEN 1 ELSE 0 END) as overlooked_crisis_count
    FROM crisislens.gold_crisis_rankings
    GROUP BY region
    ORDER BY avg_overlook_score DESC   -- Worst-performing region first
""")

(gold_regional
 .write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("crisislens.gold_regional_summary"))

print("\n📍 Regional Summary (ordered worst → best funded):")
display(gold_regional)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏥 Gold Table 3: Cluster (Sector) Summary

# COMMAND ----------

gold_cluster = spark.sql("""
    SELECT
        primary_cluster as cluster,
        COUNT(*) as num_crises,
        ROUND(SUM(people_in_need_millions), 1) as total_pin_millions,
        ROUND(AVG(coverage_rate_pct), 1) as avg_coverage_pct,
        ROUND(SUM(funding_gap_musd), 0) as total_gap_musd,
        ROUND(AVG(cbpf_need_coverage_pct), 1) as avg_cbpf_coverage_pct,
        -- Which cluster is most overlooked?
        ROUND(AVG(overlook_score), 1) as avg_overlook_score,
        -- Total CBPF going to this sector
        ROUND(SUM(cbpf_allocation_musd), 1) as total_cbpf_musd,
        SUM(CASE WHEN is_overlooked THEN 1 ELSE 0 END) as overlooked_count
    FROM crisislens.gold_crisis_rankings
    GROUP BY primary_cluster
    ORDER BY avg_overlook_score DESC
""")

(gold_cluster
 .write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("crisislens.gold_cluster_summary"))

print("📊 Cluster (Sector) Summary:")
display(gold_cluster)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💜 Gold Table 4: CBPF Mismatch Analysis
# MAGIC
# MAGIC The most actionable output for UN OCHA:
# MAGIC Which crises NEED pooled funds but AREN'T receiving them?

# COMMAND ----------

gold_cbpf_mismatch = spark.sql("""
    SELECT
        r.country,
        r.iso3,
        r.region,
        r.global_overlook_rank,
        r.overlook_score,
        r.coverage_rate_pct,
        r.funding_gap_musd,
        r.cbpf_allocation_musd as country_cbpf_total_musd,
        r.cbpf_gap_coverage_pct,
        r.cbpf_adequacy,
        r.people_in_need_millions,
        r.severity,
        r.attention_priority,
        -- Score measuring how "misallocated" pooled funds are for this crisis
        -- High overlook + low CBPF coverage = biggest mismatch
        ROUND(r.overlook_score * (1 - LEAST(r.cbpf_gap_coverage_pct / 100, 1.0)), 2) as mismatch_score,
        -- Recommended additional CBPF allocation (rough estimate)
        -- Target: cover at least 15% of the funding gap
        ROUND(
            GREATEST(
                r.funding_gap_musd * 0.15 - r.cbpf_allocation_musd,
                0
            ), 1
        ) as recommended_additional_cbpf_musd
    FROM crisislens.gold_crisis_rankings r
    ORDER BY mismatch_score DESC
""")

(gold_cbpf_mismatch
 .write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("crisislens.gold_cbpf_mismatch"))

print("💜 CBPF Mismatch Analysis (biggest pooled fund gaps):")
display(gold_cbpf_mismatch.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📢 Executive Summary Output

# COMMAND ----------

# This is designed to be copy-pasted into a UN brief or dashboard header

total_pin = spark.sql("SELECT SUM(people_in_need_millions) as t FROM crisislens.gold_crisis_rankings").collect()[0].t
total_gap = spark.sql("SELECT SUM(funding_gap_musd) as t FROM crisislens.gold_crisis_rankings WHERE funding_gap_musd > 0").collect()[0].t
avg_coverage = spark.sql("SELECT AVG(coverage_rate_pct) as t FROM crisislens.gold_crisis_rankings").collect()[0].t
overlooked = spark.sql("SELECT COUNT(*) as t FROM crisislens.gold_crisis_rankings WHERE is_overlooked").collect()[0].t
top1 = spark.sql("SELECT country, people_in_need_millions, coverage_rate_pct FROM crisislens.gold_crisis_rankings WHERE global_overlook_rank = 1").collect()[0]

print("=" * 70)
print("📢 CRISISLENS EXECUTIVE SUMMARY — 2023 GLOBAL HUMANITARIAN FUNDING")
print("=" * 70)
print(f"""
Global Overview:
  • {total_pin:.0f}M people in need across {gold_rankings.count()} active crises
  • ${total_gap:.0f}M total funding gap (money requested but not received)
  • Average funding coverage: {avg_coverage:.1f}% globally
  • {int(overlooked)} crises classified as "Overlooked" (severity ≥3, coverage <40%)

Most Overlooked Crisis:
  • {top1.country}: {top1.people_in_need_millions:.1f}M people in need, only {top1.coverage_rate_pct:.1f}% funded

Top Recommendations for UN OCHA:
  1. Urgently increase pooled fund engagement with overlooked crises
  2. Consider rebalancing CBPF allocations toward severity-4 underfunded crises
  3. Flag {int(overlooked)} overlooked crises for emergency donor appeals
  4. Review {spark.sql("SELECT COUNT(*) as t FROM crisislens.silver_hrp_projects WHERE is_efficiency_outlier").collect()[0].t} efficiency outlier projects for root cause analysis
""")

print("🎉 Notebook 03 Complete! ➡️ Run Notebook 04 next")
