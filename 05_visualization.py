# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 CrisisLens — Notebook 05: Visualization Suite & Interactive Map
# MAGIC
# MAGIC **What this notebook produces:**
# MAGIC 1. 🗺️ **Interactive HTML Map** — Choropleth showing overlook scores with clickable crisis cards
# MAGIC 2. 📈 **Bubble Chart** — Need vs. Coverage, bubble size = funding gap
# MAGIC 3. 📊 **Funding Gap Waterfall** — Top 10 most underfunded crises
# MAGIC 4. 💜 **CBPF Mismatch Chart** — Where pooled funds are misallocated
# MAGIC 5. 🔍 **Outlier Distribution** — Beneficiary-to-budget ratios by cluster
# MAGIC 6. 📋 **Final Dashboard Summary** — One-pager for judges/UN

# COMMAND ----------

# MAGIC %pip install folium==0.14.0 branca==0.6.0 --quiet

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')   # Non-interactive backend for Databricks
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import warnings
warnings.filterwarnings('ignore')

# Import folium for interactive maps
import folium
from folium import plugins
import branca.colormap as cm
import json

spark.sql("USE crisislens")
print("✅ Libraries loaded")
print("📊 Loading Gold tables for visualization...\n")

# Load all our Gold tables into pandas DataFrames for plotting
gold_df       = spark.table("crisislens.gold_crisis_rankings").toPandas()
regional_df   = spark.table("crisislens.gold_regional_summary").toPandas()
cluster_df    = spark.table("crisislens.gold_cluster_summary").toPandas()
projects_df   = spark.table("crisislens.gold_project_benchmarks").toPandas()
cbpf_df       = spark.table("crisislens.gold_cbpf_mismatch").toPandas()
silver_hrp_df = spark.table("crisislens.silver_hrp_projects").toPandas()

print(f"✅ Loaded: {len(gold_df)} crises, {len(projects_df)} projects, {len(regional_df)} regions")

# ============================================================
# SHARED STYLE CONSTANTS
# ============================================================
BG_COLOR = '#0d1117'         # Near-black background
PANEL_COLOR = '#161b22'      # Slightly lighter panel
TEXT_COLOR = 'white'
ACCENT_RED = '#f85149'       # UN-ish red for critical
ACCENT_ORANGE = '#e67e22'
ACCENT_YELLOW = '#f0c04b'
ACCENT_GREEN = '#3fb950'
ACCENT_BLUE = '#58a6ff'
ACCENT_PURPLE = '#bc8cff'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗺️ Visualization 1: Interactive Folium Map
# MAGIC
# MAGIC This creates a full Leaflet.js map with:
# MAGIC - Color-coded circles by Overlook Score (red = most overlooked)
# MAGIC - Clickable popups with full crisis details
# MAGIC - CBPF coverage indicator
# MAGIC - Layer controls

# COMMAND ----------

# Create the base map centered on the crisis hotspot region
m = folium.Map(
    location=[15, 30],    # Center: East Africa / Middle East (most crises here)
    zoom_start=3,
    tiles='CartoDB dark_matter',  # Dark base map (professional look)
    width='100%',
    height=600
)

# Color scale: green (low overlook) → red (high overlook)
# This maps 0-100 overlook score to colors
colormap = cm.LinearColormap(
    colors=['#2ecc71', '#f39c12', '#e74c3c', '#8e44ad'],
    vmin=0,
    vmax=100,
    caption='Overlook Score (0=Well Funded, 100=Most Overlooked)'
)
colormap.add_to(m)

# Add a circle for each crisis
for _, row in gold_df.iterrows():
    # Circle radius proportional to people in need (capped for readability)
    radius = min(row['people_in_need_millions'] * 25000, 800000)  # In meters

    # Get color from the colormap based on overlook score
    score = float(row['overlook_score']) if pd.notna(row['overlook_score']) else 50
    color = colormap(score)

    # Build the HTML popup content (shows full crisis details)
    popup_html = f"""
    <div style="font-family: Arial, sans-serif; min-width: 280px; max-width: 320px;
                background: #1e1e2e; color: white; padding: 15px; border-radius: 8px;
                border: 2px solid {'#e74c3c' if score > 60 else '#f39c12' if score > 30 else '#2ecc71'};">

        <h3 style="margin: 0 0 10px 0; color: white; border-bottom: 1px solid #444; padding-bottom: 8px;">
            {'🔴' if score > 60 else '🟠' if score > 30 else '🟢'} {row['country']}
            <small style="color: #aaa; font-size: 0.7em;"> | Rank #{int(row['global_overlook_rank'])}</small>
        </h3>

        <table style="width: 100%; font-size: 0.85em; border-collapse: collapse;">
            <tr><td style="color: #aaa; padding: 3px 0;">People in Need</td>
                <td style="text-align: right; font-weight: bold;">{row['people_in_need_millions']:.1f}M</td></tr>
            <tr><td style="color: #aaa; padding: 3px 0;">Required Funding</td>
                <td style="text-align: right; font-weight: bold;">${row['required_funding_musd']:.0f}M</td></tr>
            <tr><td style="color: #aaa; padding: 3px 0;">Funding Received</td>
                <td style="text-align: right; color: {'#e74c3c' if row['coverage_rate_pct'] < 40 else '#f39c12' if row['coverage_rate_pct'] < 70 else '#2ecc71'};">
                ${row['funding_received_musd']:.0f}M ({row['coverage_rate_pct']:.1f}%)</td></tr>
            <tr><td style="color: #aaa; padding: 3px 0;">Funding Gap</td>
                <td style="text-align: right; color: #e74c3c;">${max(row['funding_gap_musd'], 0):.0f}M</td></tr>
            <tr><td style="color: #aaa; padding: 3px 0;">CBPF Allocation</td>
                <td style="text-align: right; color: #bc8cff;">${row['cbpf_allocation_musd']:.1f}M</td></tr>
            <tr><td style="color: #aaa; padding: 3px 0;">Severity</td>
                <td style="text-align: right;">{'⚠️⚠️⚠️⚠️' if row['severity'] == 4 else '⚠️⚠️⚠️' if row['severity'] == 3 else '⚠️⚠️'} {row['severity']}/4</td></tr>
            <tr><td style="color: #aaa; padding: 3px 0;">Primary Sector</td>
                <td style="text-align: right;">{row['primary_cluster']}</td></tr>
        </table>

        <div style="margin-top: 10px; padding: 8px; border-radius: 5px;
                    background: {'#3d1515' if score > 60 else '#3d2b15' if score > 30 else '#153d15'};">
            <strong>Overlook Score: {score:.1f}/100</strong><br>
            <small>{row['attention_priority']}</small>
        </div>

        <!-- Funding progress bar -->
        <div style="margin-top: 8px;">
            <div style="font-size: 0.75em; color: #aaa; margin-bottom: 3px;">Funding Coverage</div>
            <div style="background: #333; border-radius: 4px; height: 8px;">
                <div style="background: {'#e74c3c' if row['coverage_rate_pct'] < 40 else '#f39c12' if row['coverage_rate_pct'] < 70 else '#2ecc71'};
                            width: {min(row['coverage_rate_pct'], 100):.0f}%; height: 8px; border-radius: 4px;"></div>
            </div>
            <div style="font-size: 0.75em; color: white; text-align: right;">{row['coverage_rate_pct']:.1f}%</div>
        </div>
    </div>
    """

    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=15,                           # Fixed radius for visibility
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.8,
        popup=folium.Popup(popup_html, max_width=340),
        tooltip=f"{row['country']}: Score {score:.0f}/100 | {row['coverage_rate_pct']:.0f}% funded"
    ).add_to(m)

    # Add a small label for top 5 most overlooked crises
    if row['global_overlook_rank'] <= 5:
        folium.Marker(
            location=[row['latitude'] + 2, row['longitude']],
            icon=folium.DivIcon(
                html=f'<div style="font-size: 10px; font-weight: bold; color: white; '
                     f'background: #e74c3c; padding: 2px 5px; border-radius: 3px; '
                     f'white-space: nowrap;">#{int(row["global_overlook_rank"])} {row["country"]}</div>',
                icon_size=(120, 20)
            )
        ).add_to(m)

# Add a title to the map
title_html = """
<div style="position: fixed; top: 10px; left: 50%; transform: translateX(-50%);
            background: rgba(13, 17, 23, 0.9); color: white; padding: 10px 20px;
            border-radius: 8px; font-family: Arial; z-index: 1000;
            border: 1px solid #30363d;">
    <h3 style="margin: 0; font-size: 16px;">🌍 CrisisLens: Humanitarian Funding Gap Map</h3>
    <p style="margin: 3px 0 0 0; font-size: 11px; color: #aaa;">
        Circle color = Overlook Score | Click for details | Red = Most Overlooked
    </p>
</div>
"""
m.get_root().html.add_child(folium.Element(title_html))

# Save map as HTML file
map_path = "/tmp/crisislens_map.html"
m.save(map_path)
print(f"✅ Interactive map saved: {map_path}")

# Display inline in Databricks
displayHTML(m._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Visualization 2: The Overlooked Crises Bubble Chart

# COMMAND ----------

fig, ax = plt.subplots(figsize=(17, 10))
fig.patch.set_facecolor(BG_COLOR)
ax.set_facecolor(PANEL_COLOR)

# Color by severity (2=yellow, 3=orange, 4=red)
sev_colors = {2: ACCENT_YELLOW, 3: ACCENT_ORANGE, 4: ACCENT_RED}
colors = [sev_colors.get(int(s), '#888') for s in gold_df['severity']]

# Bubble size proportional to funding gap
sizes = (gold_df['funding_gap_musd'].clip(lower=0) + 100) * 1.2

scatter = ax.scatter(
    gold_df['coverage_rate_pct'],
    gold_df['people_in_need_millions'],
    s=sizes,
    c=colors,
    alpha=0.85,
    edgecolors='white',
    linewidths=0.7,
    zorder=5
)

# Label top overlooked crises
for _, row in gold_df[gold_df['global_overlook_rank'] <= 10].iterrows():
    ax.annotate(
        f"#{int(row['global_overlook_rank'])} {row['country']}",
        (row['coverage_rate_pct'], row['people_in_need_millions']),
        color='white', fontsize=9, fontweight='bold',
        xytext=(10, 5), textcoords='offset points',
        arrowprops=dict(arrowstyle='->', color='white', lw=0.5),
        zorder=10
    )

# Quadrant dividers
ax.axvline(x=50, color='white', linestyle='--', alpha=0.25, linewidth=1.5)
ax.axhline(y=10, color='white', linestyle='--', alpha=0.25, linewidth=1.5)

# Quadrant labels
ax.text(12, 26, '🔴 OVERLOOKED\nHigh Need + Low Funding',
        color=ACCENT_RED, fontsize=13, fontweight='bold', alpha=0.85,
        bbox=dict(boxstyle='round', facecolor='#3d1515', alpha=0.5))
ax.text(75, 26, '✅ VISIBLE\nHigh Need + Adequate Funding',
        color=ACCENT_GREEN, fontsize=11, ha='center', alpha=0.6)
ax.text(75, 3, '🟡 STABLE\nLow Need + Adequate Funding',
        color=ACCENT_YELLOW, fontsize=9, ha='center', alpha=0.5)
ax.text(12, 3, '⚠️ AT RISK\nLow Need + Low Funding',
        color=ACCENT_ORANGE, fontsize=9, alpha=0.5)

# Axis formatting
ax.set_xlabel('Funding Coverage Rate (%)\n← Critically Underfunded | Better Funded →',
              color=TEXT_COLOR, fontsize=13)
ax.set_ylabel('People in Need (Millions)', color=TEXT_COLOR, fontsize=13)
ax.set_title(
    '🌍 Which Crises Are Most Overlooked? (2023)\n'
    'Bubble size = Funding Gap ($M) | Color = OCHA Severity (Yellow=2, Orange=3, Red=4)',
    color=TEXT_COLOR, fontsize=15, fontweight='bold', pad=20
)
ax.tick_params(colors=TEXT_COLOR, labelsize=11)
ax.set_xlim(-5, 115)
for spine in ax.spines.values():
    spine.set_color('#30363d')

# Legend for severity
legend_elements = [
    mpatches.Patch(color=ACCENT_YELLOW, label='Severity 2 (Stressed)'),
    mpatches.Patch(color=ACCENT_ORANGE, label='Severity 3 (Crisis)'),
    mpatches.Patch(color=ACCENT_RED, label='Severity 4 (Emergency)'),
]
ax.legend(handles=legend_elements, loc='lower right',
          facecolor='#161b22', labelcolor=TEXT_COLOR, fontsize=11, framealpha=0.9)

plt.tight_layout()
plt.savefig('/tmp/chart1_bubble.png', dpi=150, bbox_inches='tight', facecolor=BG_COLOR)
plt.show()
print("✅ Bubble chart saved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Visualization 3: Funding Gap Bar Chart (Top 12)

# COMMAND ----------

fig, ax = plt.subplots(figsize=(17, 9))
fig.patch.set_facecolor(BG_COLOR)
ax.set_facecolor(PANEL_COLOR)

# Sort by funding gap, top 12
top12 = gold_df.nlargest(12, 'funding_gap_musd').sort_values('funding_gap_musd', ascending=True)

cat_colors = {
    'Fully Funded (≥100%)': ACCENT_GREEN,
    'Mostly Funded (70–99%)': ACCENT_BLUE,
    'Partially Funded (40–69%)': ACCENT_YELLOW,
    'Critically Underfunded (20–39%)': ACCENT_ORANGE,
    'Severely Underfunded (<20%)': ACCENT_RED,
}
bar_colors = [cat_colors.get(c, '#888') for c in top12['funding_category']]

# Required funding (grey)
ax.barh(top12['country'], top12['required_funding_musd'],
        color='#30363d', height=0.7, label='Required', zorder=3)
# Funding received (colored by category)
bars = ax.barh(top12['country'], top12['funding_received_musd'],
               color=bar_colors, height=0.7, label='Received', zorder=4)

# CBPF allocation (purple overlay, thin)
ax.barh(top12['country'], top12['cbpf_allocation_musd'],
        color=ACCENT_PURPLE, height=0.15, alpha=0.9,
        label='CBPF/Pooled Funds', zorder=5)

# Coverage percentage labels inside bars
for i, (_, row) in enumerate(top12.iterrows()):
    # Coverage % label
    ax.text(row['funding_received_musd'] / 2, i,
            f"{row['coverage_rate_pct']:.0f}%",
            va='center', ha='center', color='white',
            fontsize=10, fontweight='bold', zorder=6)
    # Gap label outside
    gap = max(row['funding_gap_musd'], 0)
    ax.text(row['required_funding_musd'] + 30, i,
            f"Gap: ${gap:.0f}M",
            va='center', color=ACCENT_RED, fontsize=9, fontweight='bold')

ax.set_xlabel('Funding (Millions USD)', color=TEXT_COLOR, fontsize=12)
ax.set_title(
    '💸 Top 12 Crises by Absolute Funding Gap (2023)\n'
    'Grey = Required | Colored = Received | Purple bar = CBPF Pooled Funds | % shown = Coverage',
    color=TEXT_COLOR, fontsize=14, fontweight='bold'
)
ax.tick_params(colors=TEXT_COLOR, labelsize=11)
ax.legend(loc='lower right', facecolor='#161b22', labelcolor=TEXT_COLOR, fontsize=10)
for spine in ax.spines.values():
    spine.set_color('#30363d')

plt.tight_layout()
plt.savefig('/tmp/chart2_gap_bars.png', dpi=150, bbox_inches='tight', facecolor=BG_COLOR)
plt.show()
print("✅ Gap bar chart saved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💜 Visualization 4: CBPF Mismatch Analysis

# COMMAND ----------

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
fig.patch.set_facecolor(BG_COLOR)
fig.suptitle('💜 CBPF/Pooled Fund Allocation Analysis — Is the Money Going Where It\'s Needed?',
             color=TEXT_COLOR, fontsize=14, fontweight='bold', y=1.01)

# LEFT: Scatter — Overlook Score vs. CBPF Allocation
ax1.set_facecolor(PANEL_COLOR)
sc = ax1.scatter(
    cbpf_df['overlook_score'],
    cbpf_df['country_cbpf_total_musd'],
    s=cbpf_df['people_in_need_millions'] * 8,
    c=cbpf_df['coverage_rate_pct'],
    cmap='RdYlGn', vmin=0, vmax=100,
    alpha=0.85, edgecolors='white', linewidths=0.7
)
cbar = plt.colorbar(sc, ax=ax1)
cbar.set_label('Funding Coverage %', color=TEXT_COLOR)
cbar.ax.yaxis.set_tick_params(color=TEXT_COLOR)
plt.setp(cbar.ax.yaxis.get_ticklabels(), color=TEXT_COLOR)

for _, row in cbpf_df.iterrows():
    ax1.annotate(row['country'],
                (row['overlook_score'], row['country_cbpf_total_musd']),
                fontsize=8, color=TEXT_COLOR,
                xytext=(4, 4), textcoords='offset points')

# The "ideal zone" — overlooked crises should receive more CBPF
ax1.axvline(x=50, color=ACCENT_RED, linestyle='--', alpha=0.4, linewidth=1.5, label='Overlook threshold')
ax1.text(55, ax1.get_ylim()[1] * 0.9, '← More overlooked',
         color=ACCENT_RED, fontsize=9, style='italic')
ax1.text(2, ax1.get_ylim()[1] * 0.9, '⚠️ Ideal:\nHigh CBPF here',
         color=ACCENT_PURPLE, fontsize=9)

ax1.set_xlabel('Overlook Score (Higher = More Overlooked)', color=TEXT_COLOR, fontsize=11)
ax1.set_ylabel('Total CBPF Allocation ($M)', color=TEXT_COLOR, fontsize=11)
ax1.set_title('Are Pooled Funds Reaching Overlooked Crises?\n(Bottom-right = Overlooked but NOT getting CBPF support)',
              color=TEXT_COLOR, fontsize=10, fontweight='bold')
ax1.tick_params(colors=TEXT_COLOR)
ax1.legend(facecolor='#161b22', labelcolor=TEXT_COLOR)
for spine in ax1.spines.values():
    spine.set_color('#30363d')

# RIGHT: CBPF Gap Coverage % per country (bar chart)
ax2.set_facecolor(PANEL_COLOR)
top10_cbpf = cbpf_df.nlargest(12, 'mismatch_score')
bar_colors2 = [ACCENT_RED if s > 40 else ACCENT_ORANGE if s > 20 else ACCENT_GREEN
               for s in top10_cbpf['cbpf_gap_coverage_pct']]

bars2 = ax2.barh(top10_cbpf['country'], top10_cbpf['cbpf_gap_coverage_pct'],
                 color=bar_colors2, height=0.6)

# 15% target line (minimum recommended CBPF gap coverage)
ax2.axvline(x=15, color='white', linestyle='--', alpha=0.5, label='Minimum 15% target')

for i, (_, row) in enumerate(top10_cbpf.iterrows()):
    ax2.text(row['cbpf_gap_coverage_pct'] + 0.5, i,
             f"{row['cbpf_gap_coverage_pct']:.1f}%",
             va='center', color=TEXT_COLOR, fontsize=9)

ax2.set_xlabel('CBPF Coverage of Funding Gap (%)', color=TEXT_COLOR, fontsize=11)
ax2.set_title('CBPF Gap Coverage % (Top 12 Most Misallocated)\nRed = Severely insufficient pooled fund support',
              color=TEXT_COLOR, fontsize=10, fontweight='bold')
ax2.tick_params(colors=TEXT_COLOR)
ax2.legend(facecolor='#161b22', labelcolor=TEXT_COLOR)
for spine in ax2.spines.values():
    spine.set_color('#30363d')

plt.tight_layout()
plt.savefig('/tmp/chart3_cbpf.png', dpi=150, bbox_inches='tight', facecolor=BG_COLOR)
plt.show()
print("✅ CBPF mismatch chart saved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Visualization 5: Beneficiary Efficiency Outlier Distribution

# COMMAND ----------

fig, axes = plt.subplots(2, 4, figsize=(20, 10))
fig.patch.set_facecolor(BG_COLOR)
fig.suptitle(
    '📊 Project Efficiency Outlier Detection — Beneficiaries per $1,000 Budget by Cluster\n'
    'Red dashed lines = ±1.5 std dev threshold | Red dots = outlier projects requiring investigation',
    color=TEXT_COLOR, fontsize=13, fontweight='bold'
)

cluster_names = sorted(silver_hrp_df['cluster'].unique())

for idx, (cluster, ax) in enumerate(zip(cluster_names, axes.flat)):
    cluster_data = silver_hrp_df[silver_hrp_df['cluster'] == cluster].copy()
    normal = cluster_data[~cluster_data['is_efficiency_outlier']]
    outliers = cluster_data[cluster_data['is_efficiency_outlier']]

    ax.set_facecolor(PANEL_COLOR)

    # Distribution histogram for normal projects
    if len(normal) > 0:
        ax.hist(normal['ben_per_1000_usd'], bins=12, color=ACCENT_BLUE,
                alpha=0.7, label=f'Normal ({len(normal)})', edgecolor='none')

    # Mark outliers as vertical lines
    for _, outlier in outliers.iterrows():
        color = ACCENT_GREEN if outlier['ben_ratio_zscore'] > 0 else ACCENT_RED
        ax.axvline(x=outlier['ben_per_1000_usd'], color=color, alpha=0.8, linewidth=1.5)

    # Average and threshold lines
    if 'cluster_avg_ben_ratio' in cluster_data.columns and len(cluster_data) > 0:
        avg = cluster_data['cluster_avg_ben_ratio'].iloc[0]
        std = cluster_data['cluster_std_ben_ratio'].iloc[0]
        ax.axvline(x=avg, color='white', linewidth=2, label=f'Mean: {avg:.0f}')
        ax.axvline(x=avg + 1.5 * std, color=ACCENT_RED, linewidth=1,
                   linestyle='--', alpha=0.7, label='±1.5σ')
        ax.axvline(x=max(avg - 1.5 * std, 0), color=ACCENT_RED, linewidth=1,
                   linestyle='--', alpha=0.7)

    ax.set_title(cluster, color=TEXT_COLOR, fontsize=10, fontweight='bold')
    ax.set_xlabel('Beneficiaries per $1,000', color=TEXT_COLOR, fontsize=8)
    ax.tick_params(colors=TEXT_COLOR, labelsize=7)
    ax.legend(fontsize=7, facecolor=BG_COLOR, labelcolor=TEXT_COLOR, framealpha=0.7)
    for spine in ax.spines.values():
        spine.set_color('#30363d')

    # Outlier annotation
    if len(outliers) > 0:
        ax.text(0.97, 0.95, f'⚠️ {len(outliers)} outliers',
                transform=ax.transAxes, ha='right', va='top',
                color=ACCENT_RED, fontsize=8, fontweight='bold')

plt.tight_layout()
plt.savefig('/tmp/chart4_outliers.png', dpi=150, bbox_inches='tight', facecolor=BG_COLOR)
plt.show()
print("✅ Outlier distribution chart saved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 Final Summary: Key Numbers for Judges

# COMMAND ----------

print("=" * 70)
print("🌍 CRISISLENS — FINAL FINDINGS FOR UN OCHA & HACKATHON JUDGES")
print("=" * 70)

total_pin = gold_df['people_in_need_millions'].sum()
total_gap = gold_df['funding_gap_musd'].clip(lower=0).sum()
avg_cov = gold_df['coverage_rate_pct'].mean()
overlooked_n = gold_df['is_overlooked'].sum()
outlier_n = silver_hrp_df['is_efficiency_outlier'].sum()
top3 = gold_df.nsmallest(3, 'global_overlook_rank')

print(f"""
📊 SCALE OF ANALYSIS
   {len(gold_df)} active humanitarian crises analyzed
   {total_pin:.0f} million people in need globally
   ${total_gap:.0f}M total funding gap (money requested but not received)
   {avg_cov:.1f}% average global funding coverage

🔴 OVERLOOKED CRISES ({int(overlooked_n)} total, severity ≥3, coverage <40%)""")

for _, row in top3.iterrows():
    unfunded = row.get('estimated_unfunded_people_millions', 0)
    print(f"""
   #{int(row['global_overlook_rank'])} {row['country']} ({row['region']})
      {row['people_in_need_millions']:.1f}M people in need | Severity {row['severity']}/4
      Only {row['coverage_rate_pct']:.1f}% funded (${row['funding_received_musd']:.0f}M of ${row['required_funding_musd']:.0f}M)
      Funding gap: ${max(row['funding_gap_musd'],0):.0f}M | {unfunded:.1f}M people without support
      CBPF gap coverage: {row['cbpf_gap_coverage_pct']:.1f}% — {'⚠️ Pooled funds barely engaged' if row['cbpf_gap_coverage_pct'] < 10 else ''}""")

print(f"""
🤖 ML ANALYSIS ({len(projects_df)} HRP projects across {silver_hrp_df['country'].nunique()} countries)
   {int(outlier_n)} efficiency outlier projects detected ({outlier_n/len(projects_df)*100:.1f}%)
   6 benchmark groups created for performance comparison
   Benchmarking engine ready: find comparable projects for any project

💜 CBPF/POOLED FUND MISMATCHES
   {cbpf_df[cbpf_df['cbpf_gap_coverage_pct'] < 10].shape[0]} crises receiving <10% gap coverage from pooled funds
   Most misallocated: {cbpf_df.nlargest(1, 'mismatch_score')['country'].values[0] if len(cbpf_df) > 0 else 'N/A'}

🗺️ INTERACTIVE MAP: /tmp/crisislens_map.html (click any country circle for full details)
""")

print("=" * 70)
print("✅ ALL VISUALIZATIONS COMPLETE!")
print("🎉 CrisisLens is ready for submission!")
print("=" * 70)
