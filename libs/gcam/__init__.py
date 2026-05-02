"""GCAM: Global Community Alignment Model — post-hoc evidence-quality layer on raw Glicko-2 outputs."""

from .adjustment import (
    adjusted_rating,
    baseline_rating,
    combine_total_rd,
    power_score,
    structural_rd,
    trust_factor,
)
from .config import DEFAULT_GCAM_CONFIG, GCAMConfig
from .football import fact_table_to_weighted_matches
from .pipeline import build_directed_edges, build_gcam_diagnostics, run_posthoc_gcam
from .simple import (
    GCAMSimplifiedConfig,
    adjusted_rating_simple,
    comparability_rational,
    diffuse_seed_on_fixture_graph,
    run_simple_comparability,
)

__all__ = [
    "DEFAULT_GCAM_CONFIG",
    "GCAMConfig",
    "adjusted_rating",
    "baseline_rating",
    "combine_total_rd",
    "power_score",
    "structural_rd",
    "trust_factor",
    "fact_table_to_weighted_matches",
    "build_directed_edges",
    "build_gcam_diagnostics",
    "run_posthoc_gcam",
    "GCAMSimplifiedConfig",
    "adjusted_rating_simple",
    "comparability_rational",
    "diffuse_seed_on_fixture_graph",
    "run_simple_comparability",
]
