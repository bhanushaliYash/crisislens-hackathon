# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ CrisisLens — Notebook 00: Setup & Configuration
# MAGIC
# MAGIC **Run this FIRST before any other notebook.**
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Installs any extra Python libraries we need
# MAGIC 2. Creates the `crisislens` database in your Databricks workspace
# MAGIC 3. Sets shared configuration variables used by all other notebooks
# MAGIC 4. Verifies your cluster has everything needed
# MAGIC
# MAGIC **Goal:** One-time setup so every other notebook "just works"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Step 1: Install Extra Libraries
# MAGIC
# MAGIC Databricks clusters come with most data science libraries pre-installed.
# MAGIC We only need to add `folium` for interactive HTML maps.

# COMMAND ----------

# Install folium for interactive geographic maps
# %pip is the Databricks way to install packages (restarts Python kernel automatically)
# folium creates Leaflet.js maps in Python - perfect for showing crisis locations

# MAGIC %pip install folium==0.14.0 branca==0.6.0 --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗄️ Step 2: Create Database and Verify Spark

# COMMAND ----------

# Import everything we'll need across the project
import os
import json
import requests          # HTTP requests to download data from HUMDATA
import pandas as pd      # Data manipulation (used before converting to Spark)
import numpy as np       # Numerical computing (for z-scores, normalization)
import io                # Read data from memory buffers (for downloaded files)
import warnings
warnings.filterwarnings('ignore')  # Suppress non-critical warnings for clean output

from pyspark.sql import SparkSession
from pyspark.sql import functions as F      # F.col(), F.when(), F.round(), etc.
from pyspark.sql import types as T          # Data types for schema definition
from pyspark.sql.window import Window       # Window functions for rankings

# Databricks automatically creates a SparkSession called 'spark'
# We just reference it here
spark = SparkSession.builder.getOrCreate()

print("=" * 60)
print("✅ CrisisLens Environment Check")
print("=" * 60)
print(f"  Spark Version:    {spark.version}")
print(f"  Python Version:   {os.sys.version.split()[0]}")
print(f"  Pandas Version:   {pd.__version__}")
print(f"  NumPy Version:    {np.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Step 3: Create the CrisisLens Database
# MAGIC
# MAGIC All our Delta tables will live inside the `crisislens` database.
# MAGIC This keeps everything organized and avoids name collisions with other projects.

# COMMAND ----------

# Drop and recreate for a fresh start (safe for hackathon - won't affect other databases)
# In production you'd use CREATE IF NOT EXISTS only
spark.sql("""
    CREATE DATABASE IF NOT EXISTS crisislens
    COMMENT 'CrisisLens Humanitarian Funding Gap Intelligence - Databricks Hackathon 2024'
""")

# Set this database as active - now we don't need to prefix every table with 'crisislens.'
spark.sql("USE crisislens")

print("✅ Database 'crisislens' is ready")
print("   All subsequent tables will be stored here automatically")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Step 4: Global Configuration
# MAGIC
# MAGIC These variables are used across ALL notebooks.
# MAGIC They control data sources, thresholds, and visual styling.

# COMMAND ----------

# ============================================================
# CONFIGURATION — Edit these values to tune the analysis
# ============================================================

CONFIG = {
    # ---- Scoring Thresholds ----
    # A crisis is "overlooked" if coverage < this % AND severity >= min_severity
    "overlooked_coverage_threshold_pct": 40,
    "overlooked_min_severity": 3,

    # ---- Outlier Detection ----
    # Projects with |z-score| > this are flagged as outliers
    "outlier_zscore_threshold": 1.5,

    # ---- KMeans Clustering ----
    # Number of benchmark groups to create from HRP projects
    "kmeans_clusters": 6,
    "kmeans_seed": 42,          # Fixed seed = reproducible results every run

    # ---- Visualization ----
    "map_center_lat": 15.0,     # Center the world map over Africa/Middle East (crisis hotspot)
    "map_center_lon": 30.0,
    "map_zoom": 2,

    # ---- Data ----
    "analysis_year": 2023,      # Which year's data to analyze

    # ---- Colors for charts ----
    "colors": {
        "critical": "#d63031",       # Deep red - critical priority
        "high": "#e17055",           # Orange-red - high priority
        "medium": "#fdcb6e",         # Yellow - medium priority
        "low": "#55efc4",            # Teal - adequately funded
        "cbpf": "#6c5ce7",           # Purple - pooled fund color
        "background": "#0f1117",     # Dark background for charts
        "text": "white",
    }
}

print("✅ Configuration loaded:")
for key, val in CONFIG.items():
    if key != "colors":
        print(f"  {key}: {val}")

# Make config available to other notebooks via Databricks widgets
# (In a real workflow you'd use dbutils.jobs.taskValues)
import json
dbutils.notebook.exit(json.dumps({"status": "setup_complete", "config": str(CONFIG)}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!
# MAGIC
# MAGIC **Next step:** Open and run `01_data_ingestion.py`
# MAGIC
# MAGIC Summary of what was created:
# MAGIC - ✅ Database `crisislens` initialized in Delta Lake
# MAGIC - ✅ All Python libraries verified and available
# MAGIC - ✅ Configuration variables set
# MAGIC - ✅ Ready to ingest data!
