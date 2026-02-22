# 📐 CrisisLens — Methodology & Technical Documentation

## The Overlook Score Algorithm

### Problem
With 25+ simultaneous humanitarian crises, UN OCHA cannot manually evaluate which crises deserve priority attention for funding advocacy. A systematic, comparable score is needed.

### Formula

```
OverlookScore_raw = (PeopleInNeed_M × Severity²) / CoverageRate

Where:
  PeopleInNeed_M  = People needing humanitarian assistance (millions, from HNO)
  Severity        = OCHA classification 1-4 (4 = catastrophe/emergency)
  CoverageRate    = FundingReceived / FundingRequired (from OCHA FTS)
  
Then normalized via min-max scaling to 0-100:
  OverlookScore = ((raw - min_raw) / (max_raw - min_raw)) × 100
```

### Why severity²?
Severity is squared rather than linear to give exponentially more weight to the worst crises. The difference between severity 3 and 4 represents catastrophic deterioration, not just incremental change. A severity-4 crisis receives 78% more weight than a severity-3 crisis with identical need and coverage.

### Why normalize to 0-100?
The raw score is difficult to interpret and compare year-over-year. Min-max normalization creates a relative ranking while preserving the ordering and proportional differences between crises.

### Limitations
- Relies on self-reported OCHA data (some crises are underreported)
- Coverage rate can exceed 100% (over-funded crises); we cap at 150%
- Severity classifications may change during the year
- Does not account for access constraints that may limit absorption capacity

---

## Beneficiary Efficiency Z-Score

### Purpose
Identify HRP projects where the beneficiary-to-budget ratio deviates significantly from comparable projects in the same cluster.

### Formula
```
Z = (ProjectEfficiency - ClusterMeanEfficiency) / ClusterStdDevEfficiency

Where:
  ProjectEfficiency = target_beneficiaries / (budget_USD / 1000)
                    = beneficiaries per $1,000 budget
  ClusterMeanEfficiency = avg efficiency for all projects in same cluster
  ClusterStdDevEfficiency = std dev of efficiency within the cluster

Flag as outlier if |Z| > 1.5
```

### Interpretation
| Z-Score | Interpretation | Recommended Action |
|---------|---------------|-------------------|
| > +2.0  | Highly efficient vs cluster | Investigate for scale-up potential |
| +1.5 to +2.0 | Above average | Monitor; document methodology |
| -1.5 to +1.5 | Normal range | No action needed |
| -1.5 to -2.0 | Below average | Review project design |
| < -2.0  | Significantly low efficiency | Investigate: access issues? methodology problems? |

### Why compare within cluster?
A Food Security project naturally reaches more beneficiaries per dollar than a Shelter project (food is cheaper to distribute than building materials). Cross-cluster comparisons would be meaningless.

---

## KMeans Benchmark Clustering

### Purpose
Group HRP projects into 6 "benchmark clusters" — groups of similar projects that can be legitimately compared with each other.

### Features used
| Feature | Weight Rationale |
|---------|-----------------|
| budget_usd | Reflects project scale |
| target_beneficiaries | Reflects scope of intervention |
| ben_per_1000_usd | Core efficiency metric |
| funding_coverage_pct | Reflects funding environment |
| cluster_index | Project type (Food/Health/WASH etc.) |

All features are StandardScaled (mean=0, std=1) before clustering to prevent scale dominance.

### Model Selection
We tested k=4 through k=9 and selected k=6 based on:
- Silhouette Score maximized at k=6
- Group sizes are reasonably balanced (no single dominant cluster)
- Groups produce interpretable profiles

### Finding Comparable Projects
Given project P in benchmark group G, we find the top-N most similar projects as:

```
SimilarityDistance = 
  |P.efficiency - Q.efficiency| × 0.5 +
  |P.coverage - Q.coverage| × 0.3 +
  |ln(P.budget) - ln(Q.budget)| × 5

Lower distance = more similar project
```

Log-transforming budget normalizes the large variance in project sizes.

---

## CBPF Mismatch Score

Measures how misaligned pooled fund allocations are relative to crisis need:

```
MismatchScore = OverlookScore × (1 - CBPF_GapCoverage%)

High score = High need + Low pooled fund engagement
```

A country with Overlook Score 80 and only 5% CBPF gap coverage has a very high Mismatch Score — the pooled fund system is not responding to this overlooked crisis.
