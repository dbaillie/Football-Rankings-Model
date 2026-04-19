import itertools
import numpy as np
import pandas as pd

from .core import run_glicko2


# =========================
# Metrics
# =========================

EPS = 1e-12


def log_loss_binary(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.clip(np.asarray(y_pred, dtype=float), EPS, 1.0 - EPS)
    return float(-np.mean(y_true * np.log(y_pred) + (1.0 - y_true) * np.log(1.0 - y_pred)))


def brier_score_binary(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return float(np.mean((y_pred - y_true) ** 2))


def accuracy_binary(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    mask = np.abs(y_true - 0.5) > 1e-12
    if mask.sum() == 0:
        return np.nan
    return float(np.mean((y_pred[mask] >= 0.5) == (y_true[mask] >= 0.5)))


# =========================
# Data prep
# =========================

def _make_synthetic_weeks(n_periods: int, start_year: int = 2020):
    """
    Create YYYYWW values for n ordered periods.
    Supports >52 periods by rolling into later ISO years.
    """
    weeks = []
    for i in range(n_periods):
        year = start_year + (i // 52)
        week = (i % 52) + 1
        weeks.append(year * 100 + week)
    return weeks


def prepare_match_history(
    history: pd.DataFrame,
    entity_a_col: str,
    entity_b_col: str,
    score_col: str,
    period_col: str = None,
):
    """
    Convert raw h2h history into the engine format expected by run_glicko2:
        week, EventId, PlayerA, PlayerB, scoreA

    Notes
    -----
    - If period_col is None, row order is treated as time order and each row
      gets its own synthetic week.
    - If period_col is supplied, unique periods are ordered and mapped to
      synthetic YYYYWW values preserving chronology.
    """
    df = history.copy().reset_index(drop=True)

    if period_col is not None:
        df = df.sort_values(period_col).reset_index(drop=True)
        unique_periods = list(pd.Index(df[period_col]).drop_duplicates())
        synthetic_weeks = _make_synthetic_weeks(len(unique_periods))
        period_to_week = dict(zip(unique_periods, synthetic_weeks))
        df["week"] = df[period_col].map(period_to_week).astype(int)
        df["_original_period"] = df[period_col]
    else:
        synthetic_weeks = _make_synthetic_weeks(len(df))
        df["week"] = synthetic_weeks
        df["_original_period"] = np.arange(len(df))

    entities = pd.Index(pd.concat([df[entity_a_col], df[entity_b_col]], axis=0).unique())
    entity_to_id = {entity: i for i, entity in enumerate(entities, start=1)}
    id_to_entity = {i: entity for entity, i in entity_to_id.items()}

    df["PlayerA"] = df[entity_a_col].map(entity_to_id).astype(int)
    df["PlayerB"] = df[entity_b_col].map(entity_to_id).astype(int)
    df["scoreA"] = df[score_col].astype(float)

    # EventId is only needed structurally by the engine / SoF output.
    # For plain h2h optimisation, one event per row is fine.
    df["EventId"] = np.arange(1, len(df) + 1, dtype=int)

    matches = df[["week", "EventId", "PlayerA", "PlayerB", "scoreA"]].copy()
    weeks = sorted(matches["week"].unique())

    meta = {
        "entity_to_id": entity_to_id,
        "id_to_entity": id_to_entity,
        "prepared_history": df.copy(),
    }

    return matches, weeks, meta


# =========================
# Evaluation
# =========================

def evaluate_params_on_future_h2h(
    matches: pd.DataFrame,
    weeks: list,
    params: dict,
    split_ratio: float = 0.7,
):
    """
    Run the engine across the full ordered history, but score only future periods.

    Parameters
    ----------
    matches : pd.DataFrame
        Engine-format match history.
    weeks : list[int]
        Ordered rating periods used by the engine.
    params : dict
        Parameters passed into run_glicko2.
    split_ratio : float
        Fraction of periods used as training history.
    """
    if len(weeks) < 2:
        raise ValueError("Need at least 2 periods to evaluate future h2h performance.")

    split_idx = max(1, min(len(weeks) - 1, int(len(weeks) * split_ratio)))
    test_weeks = set(weeks[split_idx:])

    state, pred_df, week_snapshots, sof_df = run_glicko2(
        matches_pdf=matches,
        weeks=weeks,
        **params,
    )

    test_pred = pred_df[pred_df["week"].isin(test_weeks)].copy()
    if test_pred.empty:
        return {
            "log_loss": np.nan,
            "brier": np.nan,
            "accuracy": np.nan,
            "n_test": 0,
            "n_test_weeks": 0,
        }

    y_true = test_pred["actual_scoreA"].astype(float).values
    y_pred = test_pred["pred_pA"].astype(float).values

    return {
        "log_loss": log_loss_binary(y_true, y_pred),
        "brier": brier_score_binary(y_true, y_pred),
        "accuracy": accuracy_binary(y_true, y_pred),
        "n_test": int(len(test_pred)),
        "n_test_weeks": int(len(test_weeks)),
    }


def rolling_origin_evaluation(
    matches: pd.DataFrame,
    weeks: list,
    params: dict,
    min_train_periods: int = 10,
    step: int = 1,
):
    """
    Optional rolling-origin evaluation for more stable parameter selection.

    At each split:
      - train history = weeks[:i]
      - test period(s) = weeks[i:i+step]
    """
    if len(weeks) <= min_train_periods:
        raise ValueError("Not enough periods for rolling-origin evaluation.")

    rows = []

    state, pred_df, week_snapshots, sof_df = run_glicko2(
        matches_pdf=matches,
        weeks=weeks,
        **params,
    )

    unique_weeks = list(weeks)
    for i in range(min_train_periods, len(unique_weeks), step):
        test_weeks = set(unique_weeks[i:i + step])
        if not test_weeks:
            continue

        curr = pred_df[pred_df["week"].isin(test_weeks)].copy()
        if curr.empty:
            continue

        y_true = curr["actual_scoreA"].astype(float).values
        y_pred = curr["pred_pA"].astype(float).values

        rows.append({
            "split_start_idx": i,
            "test_week_min": min(test_weeks),
            "test_week_max": max(test_weeks),
            "n_test": len(curr),
            "log_loss": log_loss_binary(y_true, y_pred),
            "brier": brier_score_binary(y_true, y_pred),
            "accuracy": accuracy_binary(y_true, y_pred),
        })

    detail_df = pd.DataFrame(rows)
    if detail_df.empty:
        return {
            "log_loss": np.nan,
            "brier": np.nan,
            "accuracy": np.nan,
            "n_test": 0,
            "n_splits": 0,
        }, detail_df

    summary = {
        "log_loss": float(detail_df["log_loss"].mean()),
        "brier": float(detail_df["brier"].mean()),
        "accuracy": float(detail_df["accuracy"].mean(skipna=True)),
        "n_test": int(detail_df["n_test"].sum()),
        "n_splits": int(len(detail_df)),
    }
    return summary, detail_df


# =========================
# Optimisation
# =========================

def optimise_glicko2_from_history(
    history: pd.DataFrame,
    entity_a_col: str,
    entity_b_col: str,
    score_col: str,
    period_col: str = None,
    param_grid: dict = None,
    split_ratio: float = 0.7,
    objective: str = "log_loss",
    use_rolling_origin: bool = False,
    min_train_periods: int = 10,
    rolling_step: int = 1,
):
    """
    Optimise Glicko-2 parameters directly against future h2h results.

    Required raw inputs
    -------------------
    history:
        Must contain:
        - entity_a_col
        - entity_b_col
        - score_col  (1 / 0 / 0.5 from A's perspective)

    Optional raw input
    ------------------
    period_col:
        Any sortable period/date/week column. If omitted, row order is used.

    Returns
    -------
    best_params : dict
    results_df : pd.DataFrame
    matches : pd.DataFrame
    meta : dict
    """
    matches, weeks, meta = prepare_match_history(
        history=history,
        entity_a_col=entity_a_col,
        entity_b_col=entity_b_col,
        score_col=score_col,
        period_col=period_col,
    )

    if param_grid is None:
        param_grid = {
            "init_rating": [1500.0],
            "init_rd": [200.0, 250.0, 300.0],
            "init_sigma": [0.06, 0.08, 0.1],
            "tau": [0.3, 0.5, 0.8],
            "inactivity_drift": [0.0],
            "max_sigma": [0.1],
            "upset_gate_max": [0.0],
            "upset_gate_k": [0.0],
            "info_gate_scale": [0.0],
            "inactivity_decay_pts": [0.0],
            "inactivity_decay_grace": [0.0],
            "reseed_after_weeks": [0],
            "sof_pos_sigma": [50],
            "sof_norm_top_n": [150],
            "sof_norm_target": [1000.0],
            "seed_from_wagr": [False],
            "wagr_rank_map": [None],
            "rank_to_rating_fn": [None],
            "initial_state": [None],
            "snapshot_weeks": [None],
            "diag_every": [0],
        }

    grid_keys = list(param_grid.keys())
    grid_values = [param_grid[k] for k in grid_keys]

    rows = []
    for combo in itertools.product(*grid_values):
        params = dict(zip(grid_keys, combo))

        if use_rolling_origin:
            metrics, detail_df = rolling_origin_evaluation(
                matches=matches,
                weeks=weeks,
                params=params,
                min_train_periods=min_train_periods,
                step=rolling_step,
            )
        else:
            metrics = evaluate_params_on_future_h2h(
                matches=matches,
                weeks=weeks,
                params=params,
                split_ratio=split_ratio,
            )

        row = {**params, **metrics}
        rows.append(row)

    results_df = pd.DataFrame(rows)

    if results_df.empty:
        raise ValueError("Parameter search returned no results.")

    valid_mask = results_df[objective].notna()
    if valid_mask.sum() == 0:
        raise ValueError(f"No valid optimisation results for objective '{objective}'.")

    best_idx = results_df.loc[valid_mask, objective].idxmin()
    best_params = {k: results_df.loc[best_idx, k] for k in grid_keys}

    return best_params, results_df, matches, meta


# =========================
# Final run
# =========================

def run_best_model_from_history(
    history: pd.DataFrame,
    entity_a_col: str,
    entity_b_col: str,
    score_col: str,
    best_params: dict,
    period_col: str = None,
):
    """
    Prepare history and run the full model using best_params.

    Returns
    -------
    state, pred_df, week_snapshots, sof_df, matches, meta
    """
    matches, weeks, meta = prepare_match_history(
        history=history,
        entity_a_col=entity_a_col,
        entity_b_col=entity_b_col,
        score_col=score_col,
        period_col=period_col,
    )

    state, pred_df, week_snapshots, sof_df = run_glicko2(
        matches_pdf=matches,
        weeks=weeks,
        **best_params,
    )

    return state, pred_df, week_snapshots, sof_df, matches, meta