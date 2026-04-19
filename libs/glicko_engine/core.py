"""
Glicko-2 Engine Library
=======================
Shared Glicko-2 rating engine, helper functions, and evaluation metrics
used across the WAGR Algorithm notebooks.
"""

import math
import numpy as np
import pandas as pd
from datetime import date
from collections import defaultdict


# =========================================
# CONSTANTS
# =========================================

GLICKO2_SCALE = 173.7178
EPS = 1e-12


# =========================================
# WEEK / DATE HELPERS
# =========================================

def yyyyww_to_year_week(x: int):
    y = int(x) // 100
    w = int(x) % 100
    return y, w


def iso_week_to_sunday(yyyyww: int):
    y, w = yyyyww_to_year_week(int(yyyyww))
    return date.fromisocalendar(y, w, 7)


def weeks_between(prev_yyyyww: int, curr_yyyyww: int) -> int:
    prev_sun = iso_week_to_sunday(int(prev_yyyyww))
    curr_sun = iso_week_to_sunday(int(curr_yyyyww))
    return max(0, int((curr_sun - prev_sun).days // 7))


# =========================================
# RATING SEEDING
# =========================================

def rank_to_initial_rating(
    rank,
    intercept: float = 3000.0,
    log_coeff: float = 160.0,
    unranked_ceiling: int = 4000,
    rating_floor: float = 400.0,
) -> float:
    """Log-based mapping: more spread at top ranks, compressed at lower ranks.
    Rank 1 -> ~2200, Rank 100 -> ~1464, Rank 2500 -> ~952."""
    if rank is None or pd.isna(rank):
        rank = unranked_ceiling
    rank = max(1, min(int(rank), unranked_ceiling))
    rating = intercept - log_coeff * math.log(rank)
    return max(rating_floor, float(rating))


# =========================================
# GLICKO-2 SCALE CONVERSIONS
# =========================================

def rating_to_mu(rating: float, init_rating_centre: float = 1500.0) -> float:
    return (float(rating) - float(init_rating_centre)) / GLICKO2_SCALE


def mu_to_rating(mu: float, init_rating_centre: float = 1500.0) -> float:
    return float(init_rating_centre) + GLICKO2_SCALE * float(mu)


def rd_to_phi(rd: float) -> float:
    return float(rd) / GLICKO2_SCALE


def phi_to_rd(phi: float) -> float:
    return GLICKO2_SCALE * float(phi)


# =========================================
# GLICKO-2 MATH
# =========================================

def g(phi):
    return 1.0 / math.sqrt(1.0 + (3.0 * phi * phi) / (math.pi * math.pi))


def E(mu, mu_j, phi_j):
    """Standard Glicko-2 expectation."""
    x = -g(phi_j) * (mu - mu_j)
    if x > 35:
        return 0.0
    if x < -35:
        return 1.0
    return 1.0 / (1.0 + math.exp(x))


def volatility_update(mu, phi, sigma, delta, v, tau):
    """Standard Glicko-2 volatility update (Illinois algorithm)."""
    a = math.log(sigma * sigma)

    def f(x):
        ex = math.exp(x)
        num = ex * (delta * delta - phi * phi - v - ex)
        den = 2.0 * ((phi * phi + v + ex) ** 2)
        return (num / den) - ((x - a) / (tau * tau))

    A = a
    if delta * delta > phi * phi + v:
        B = math.log(delta * delta - phi * phi - v)
    else:
        k = 1
        while f(a - k * tau) < 0:
            k += 1
        B = a - k * tau

    fA = f(A)
    fB = f(B)

    for _ in range(100):
        C = A + (A - B) * fA / (fB - fA)
        fC = f(C)
        if fC * fB < 0:
            A, fA = B, fB
        else:
            fA = fA / 2.0
        B, fB = C, fC
        if abs(B - A) < 1e-6:
            break

    return math.exp(A / 2.0)


# =========================================
# EVALUATION METRICS
# =========================================

def log_loss_binary(y, p):
    p = np.clip(np.asarray(p, dtype=float), EPS, 1.0 - EPS)
    y = np.asarray(y, dtype=float)
    return float(-np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))


def brier_score_binary(y, p):
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    return float(np.mean((p - y) ** 2))


def accuracy_binary(y, p):
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    mask = (np.abs(y - 0.5) > 1e-12)
    if mask.sum() == 0:
        return np.nan
    return float(np.mean((p[mask] >= 0.5) == (y[mask] >= 0.5)))


# =========================================
# EVALUATION HELPERS
# =========================================

def fit_rank_to_rating_curve(
    model_state, wk_rank_map, init_rating_centre,
    default_intercept=3000.0, default_log_coeff=160.0,
):
    """
    Fit rating = a - b*ln(rank) on overlapping players for one week.
    Falls back to defaults if insufficient overlap.
    """
    pairs = []
    for pid, rk in wk_rank_map.items():
        if pid in model_state and rk is not None and rk > 0:
            rating = mu_to_rating(model_state[pid]["mu"], init_rating_centre)
            pairs.append((float(rk), float(rating)))

    if len(pairs) < 10:
        return float(default_intercept), float(default_log_coeff)

    x = np.array([p[0] for p in pairs], dtype=float)
    y = np.array([p[1] for p in pairs], dtype=float)
    coeff = np.polyfit(np.log(x), y, 1)
    a = float(coeff[1])
    b = float(-coeff[0])
    return a, b


def wagr_rank_to_rating(rank, a, b):
    rank = max(1, int(rank))
    return float(a - b * math.log(rank))


def score_predictions_elite_only(
    pred_df,
    valid_weeks,
    valid_state_snapshots,
    wagr_weekly,
    init_rating_centre,
    pred_top_n=500,
    burn_in_weeks=0
):
    """
    Compare model predictions against WAGR-implied predictions week by week.
    Uses PRIOR-week model state and PRIOR-week WAGR ranking.
    Restricts scoring to matches where at least one player is top-N in either system.
    """
    rows = []
    start_idx = max(1, int(burn_in_weeks))
    valid_weeks = list(valid_weeks)

    for i in range(start_idx, len(valid_weeks)):
        prev_wk = int(valid_weeks[i - 1])
        curr_wk = int(valid_weeks[i])

        curr_pred = pred_df[pred_df["week"] == curr_wk].copy()
        if curr_pred.shape[0] == 0:
            continue

        prev_state = valid_state_snapshots.get(prev_wk, {})
        prev_ranks = wagr_weekly.get(prev_wk, {})

        if len(prev_state) == 0:
            continue

        model_rating_map = {
            int(pid): mu_to_rating(v["mu"], init_rating_centre)
            for pid, v in prev_state.items()
        }
        model_top = set(sorted(model_rating_map, key=model_rating_map.get, reverse=True)[:pred_top_n])
        wagr_top = {int(pid) for pid, rk in prev_ranks.items() if rk <= pred_top_n}
        elite_set = model_top | wagr_top

        fit_a, fit_b = fit_rank_to_rating_curve(prev_state, prev_ranks, init_rating_centre)

        model_probs = []
        wagr_probs = []
        actuals = []

        for _, r in curr_pred.iterrows():
            a = int(r["PlayerA"])
            b = int(r["PlayerB"])
            actual = float(r["actual_scoreA"])

            if a not in elite_set and b not in elite_set:
                continue

            p_model = float(r["pred_pA"])

            ra = prev_ranks.get(a)
            rb = prev_ranks.get(b)
            if ra is None or rb is None:
                continue

            rating_a = wagr_rank_to_rating(ra, fit_a, fit_b)
            rating_b = wagr_rank_to_rating(rb, fit_a, fit_b)

            mu_a = rating_to_mu(rating_a, init_rating_centre)
            mu_b = rating_to_mu(rating_b, init_rating_centre)

            phi_proxy = rd_to_phi(250.0)
            p_wagr = E(mu_a, mu_b, phi_proxy)

            model_probs.append(p_model)
            wagr_probs.append(p_wagr)
            actuals.append(actual)

        if len(actuals) == 0:
            continue

        rows.append({
            "week": curr_wk,
            "n_preds": len(actuals),
            "model_log_loss": log_loss_binary(actuals, model_probs),
            "model_brier": brier_score_binary(actuals, model_probs),
            "model_accuracy": accuracy_binary(actuals, model_probs),
            "wagr_log_loss": log_loss_binary(actuals, wagr_probs),
            "wagr_brier": brier_score_binary(actuals, wagr_probs),
            "wagr_accuracy": accuracy_binary(actuals, wagr_probs),
        })

    return pd.DataFrame(rows)


# =========================================
# GLICKO-2 ENGINE
# =========================================

def run_glicko2(
    matches_pdf,
    weeks,
    init_rating=1500.0,
    init_rd=250.0,
    init_sigma=0.06,
    tau=0.5,
    inactivity_drift=0.0,
    max_sigma=0.1,
    upset_gate_max=0.0,
    upset_gate_k=0.0,
    info_gate_scale=0.0,
    inactivity_decay_pts=0.0,
    inactivity_decay_grace=8,
    reseed_after_weeks=0,
    sof_pos_sigma=50,
    sof_norm_top_n=150,
    sof_norm_target=1000.0,
    seed_from_wagr=False,
    wagr_rank_map=None,
    rank_to_rating_fn=None,
    initial_state=None,
    snapshot_weeks=None,
    diag_every=10,
):
    """
    Runs standard Glicko-2 over weekly rating periods.

    Parameters
    ----------
    matches_pdf : pd.DataFrame
        Columns: week, EventId, PlayerA, PlayerB, scoreA
    weeks : list[int]
        Ordered list of YYYYWW rating periods.
    rank_to_rating_fn : callable, optional
        Function(rank) -> float for seeding new players from WAGR rank.
        If None and seed_from_wagr=True, uses rank_to_initial_rating defaults.
    reseed_after_weeks : int
        If a player returns after >= this many weeks of inactivity,
        fully reset them as a new player. Set to 0 to disable.
    sof_pos_sigma : float
        Gaussian sigma for positional weighting in SoF.
        w(rank) = exp(-rank^2 / (2*sigma^2)). Set 0 to disable.
    sof_norm_top_n : int
        Number of top-rated players used as the reference field for SoF
        normalisation. The weighted sum of this reference field is scaled
        to ``sof_norm_target``. Set to 0 to disable normalisation.
    sof_norm_target : float
        Target value for the reference field's normalised SoF (default 1000).

    Returns
    -------
    state, pred_df, week_snapshots, sof_df
    """
    if wagr_rank_map is None:
        wagr_rank_map = {}
    if rank_to_rating_fn is None:
        rank_to_rating_fn = rank_to_initial_rating

    gate_active = (upset_gate_max > 0 and upset_gate_k > 0)
    info_gate_active = (info_gate_scale > 0)
    decay_active = (inactivity_decay_pts > 0)
    decay_mu_per_week = inactivity_decay_pts / GLICKO2_SCALE if decay_active else 0.0
    reseed_active = (reseed_after_weeks > 0)
    sof_norm_active = (sof_norm_top_n > 0 and sof_norm_target > 0)

    sof_sigma_sq2 = 2.0 * sof_pos_sigma * sof_pos_sigma if sof_pos_sigma > 0 else 1.0

    if initial_state is None:
        state = {}
    else:
        state = {
            int(pid): {
                "mu": float(v["mu"]),
                "phi": float(v["phi"]),
                "sigma": float(v["sigma"]),
                "last_week_seen": int(v["last_week_seen"])
            }
            for pid, v in initial_state.items()
        }

    events_by_week = defaultdict(lambda: defaultdict(set))
    for _, row in matches_pdf.iterrows():
        wk = int(row["week"])
        eid = row["EventId"]
        events_by_week[wk][eid].add(int(row["PlayerA"]))
        events_by_week[wk][eid].add(int(row["PlayerB"]))

    predictions = []
    sof_records = []
    week_snapshots = {}
    weeks_list = list(weeks)
    n_weeks = len(weeks_list)
    prev_loop_week = None
    ref_phi = rd_to_phi(init_rd)

    for week_idx, week in enumerate(weeks_list):
        week = int(week)
        wk_df = matches_pdf[matches_pdf["week"] == week]

        if prev_loop_week is not None:
            weeks_elapsed = weeks_between(prev_loop_week, week)
        else:
            weeks_elapsed = 0

        if wk_df.shape[0] == 0:
            prev_loop_week = week
            continue

        # 1) Ensure all players in this week exist
        players_this_week = set(wk_df["PlayerA"].astype(int)).union(set(wk_df["PlayerB"].astype(int)))
        for pid in players_this_week:
            if pid not in state:
                if seed_from_wagr:
                    seeded_rating = rank_to_rating_fn(wagr_rank_map.get(pid))
                else:
                    seeded_rating = float(init_rating)

                state[int(pid)] = {
                    "mu": rating_to_mu(seeded_rating, init_rating_centre=init_rating),
                    "phi": rd_to_phi(init_rd),
                    "sigma": float(init_sigma),
                    "last_week_seen": int(week)
                }

        # 1b) Re-seed players returning after long absence
        if reseed_active:
            for pid in players_this_week:
                gap = weeks_between(state[pid]["last_week_seen"], week)
                if gap >= reseed_after_weeks:
                    if seed_from_wagr:
                        seeded_rating = rank_to_rating_fn(wagr_rank_map.get(pid))
                    else:
                        seeded_rating = float(init_rating)
                    state[pid]["mu"] = rating_to_mu(seeded_rating, init_rating_centre=init_rating)
                    state[pid]["phi"] = rd_to_phi(init_rd)
                    state[pid]["sigma"] = float(init_sigma)
                    state[pid]["last_week_seen"] = int(week)

        # 2) Apply inactivity RD inflation for players about to play
        for pid in players_this_week:
            gap = weeks_between(state[pid]["last_week_seen"], week)
            if gap > 0:
                phi = state[pid]["phi"]
                sigma = state[pid]["sigma"]
                phi_pre = math.sqrt(phi * phi + sigma * sigma * gap + inactivity_drift * inactivity_drift * gap)
                state[pid]["phi"] = phi_pre

        # 2b) Apply weekly decay to all inactive players beyond grace period
        if decay_active and weeks_elapsed > 0:
            for pid in state:
                if pid in players_this_week:
                    continue
                gap_now = weeks_between(state[pid]["last_week_seen"], week)
                gap_before = gap_now - weeks_elapsed
                decay_weeks = max(0, gap_now - inactivity_decay_grace) - max(0, gap_before - inactivity_decay_grace)
                if decay_weeks > 0:
                    state[pid]["mu"] -= decay_mu_per_week * decay_weeks

        # 2c) Strength of Field — computed before rating updates
        sof_factor = 1.0
        if sof_norm_active:
            all_rd_contribs = []
            for pid in state:
                r = mu_to_rating(state[pid]["mu"], init_rating)
                phi_p = state[pid]["phi"]
                w_rd = min(1.0, ref_phi / max(phi_p, 1e-12))
                all_rd_contribs.append(r * w_rd)

            all_rd_contribs.sort(reverse=True)
            sof_ref_top = all_rd_contribs[:sof_norm_top_n]

            sof_ref_sum = 0.0
            for rank_idx, contrib in enumerate(sof_ref_top):
                w_pos = math.exp(-(rank_idx * rank_idx) / sof_sigma_sq2) if sof_pos_sigma > 0 else 1.0
                sof_ref_sum += contrib * w_pos

            sof_factor = sof_norm_target / sof_ref_sum if sof_ref_sum > 0 else 1.0

        for eid, event_pids in events_by_week[week].items():
            ratings = []
            player_rd_contrib = []

            for pid in event_pids:
                if pid in state:
                    r = mu_to_rating(state[pid]["mu"], init_rating)
                    phi_p = state[pid]["phi"]
                    ratings.append(r)
                    w_rd = min(1.0, ref_phi / max(phi_p, 1e-12))
                    player_rd_contrib.append(r * w_rd)

            if ratings:
                player_rd_contrib.sort(reverse=True)
                weighted_sum = 0.0
                for rank_idx, contrib in enumerate(player_rd_contrib):
                    w_pos = math.exp(-(rank_idx * rank_idx) / sof_sigma_sq2) if sof_pos_sigma > 0 else 1.0
                    weighted_sum += contrib * w_pos

                sof_records.append({
                    "week": week,
                    "EventId": eid,
                    "field_size": len(ratings),
                    "sof_sum": sum(ratings),
                    "sof_avg": sum(ratings) / len(ratings),
                    "sof_rd_weighted": weighted_sum,
                    "sof_norm": weighted_sum * sof_factor,
                })

        # 3) Store pre-update match predictions
        for _, row in wk_df.iterrows():
            a = int(row["PlayerA"])
            b = int(row["PlayerB"])
            actual = float(row["scoreA"])

            mu_a = state[a]["mu"]
            mu_b = state[b]["mu"]
            phi_b = state[b]["phi"]

            pred_a = E(mu_a, mu_b, phi_b)

            predictions.append({
                "week": week,
                "PlayerA": a,
                "PlayerB": b,
                "actual_scoreA": actual,
                "pred_pA": pred_a,
            })

        # 4) Accumulate each player's within-period games
        games_by_player = defaultdict(list)
        for _, row in wk_df.iterrows():
            a = int(row["PlayerA"])
            b = int(row["PlayerB"])
            s_a = float(row["scoreA"])
            s_b = 1.0 - s_a

            games_by_player[a].append((b, s_a))
            games_by_player[b].append((a, s_b))

        # Snapshot pre-update ratings for diagnostics
        if diag_every > 0:
            pre_mu = {pid: state[pid]["mu"] for pid in games_by_player}
        else:
            pre_mu = {}

        # 5) Freeze weekly state so all updates use the same rating-period inputs
        pre_state = {
            int(pid): {
                "mu": float(v["mu"]),
                "phi": float(v["phi"]),
                "sigma": float(v["sigma"]),
                "last_week_seen": int(v["last_week_seen"]),
            }
            for pid, v in state.items()
        }

        # 6) Update each active player using frozen pre_state
        for pid, games in games_by_player.items():
            mu = pre_state[pid]["mu"]
            phi = pre_state[pid]["phi"]
            sigma = pre_state[pid]["sigma"]

            n_games = len(games)
            if n_games == 0:
                state[pid]["last_week_seen"] = week
                continue

            v_inv = 0.0
            delta_sum = 0.0

            for opp, score in games:
                mu_j = pre_state[opp]["mu"]
                phi_j = pre_state[opp]["phi"]

                g_j = g(phi_j)
                e_j = E(mu, mu_j, phi_j)

                base_info = (g_j ** 2) * e_j * (1.0 - e_j)
                if info_gate_active:
                    rating_diff = GLICKO2_SCALE * (mu_j - mu)
                    x = rating_diff / info_gate_scale
                    if x < -35:
                        info_w = 0.0
                    elif x > 35:
                        info_w = 1.0
                    else:
                        info_w = 1.0 / (1.0 + math.exp(-x))
                    v_inv += info_w * base_info
                else:
                    v_inv += base_info

                contribution = g_j * (score - e_j)

                if gate_active and contribution > 0:
                    rating_diff = GLICKO2_SCALE * (mu_j - mu)
                    x = upset_gate_k * rating_diff
                    M = upset_gate_max
                    if x < -35:
                        quality = 0.0
                    elif x > 35:
                        quality = M
                    else:
                        quality = M / (1.0 + (M - 1.0) * math.exp(-x))
                    contribution *= quality

                delta_sum += contribution

            if n_games > 1:
                vol_scale = math.sqrt(n_games)
                v_inv /= vol_scale
                delta_sum /= vol_scale

            if v_inv <= 0:
                state[pid]["last_week_seen"] = week
                continue

            v = 1.0 / v_inv
            delta = v * delta_sum

            sigma_new = volatility_update(mu, phi, sigma, delta, v, tau)
            sigma_new = min(sigma_new, max_sigma)
            phi_star = math.sqrt(phi * phi + sigma_new * sigma_new)
            phi_new = 1.0 / math.sqrt((1.0 / (phi_star * phi_star)) + (1.0 / v))
            mu_new = mu + (phi_new * phi_new) * delta_sum

            state[pid]["mu"] = mu_new
            state[pid]["phi"] = phi_new
            state[pid]["sigma"] = sigma_new
            state[pid]["last_week_seen"] = week

        # Diagnostics
        is_last = (week_idx == n_weeks - 1)
        if diag_every > 0 and (week_idx % diag_every == 0 or is_last):
            top_pid = max(state, key=lambda p: state[p]["mu"])
            top_rating = mu_to_rating(state[top_pid]["mu"], init_rating)
            top_rd = phi_to_rd(state[top_pid]["phi"])
            top_wagr = wagr_rank_map.get(top_pid, "?")

            best_rise_pid, best_rise = None, -1e9
            best_drop_pid, best_drop = None, 1e9
            for pid in games_by_player:
                if pid in pre_mu:
                    delta_r = GLICKO2_SCALE * (state[pid]["mu"] - pre_mu[pid])
                    if delta_r > best_rise:
                        best_rise = delta_r
                        best_rise_pid = pid
                    if delta_r < best_drop:
                        best_drop = delta_r
                        best_drop_pid = pid

            n_active = len(games_by_player)
            line = (
                f"  wk {week} [{week_idx+1:>3}/{n_weeks}]  "
                f"players={len(state):,}  active={n_active:,}  "
                f"#1: pid={top_pid} (WAGR {top_wagr}) r={top_rating:.1f} rd={top_rd:.1f}"
            )
            if best_rise_pid is not None:
                line += f"  | biggest rise: pid={best_rise_pid} +{best_rise:.1f}"
            if best_drop_pid is not None:
                line += f"  drop: pid={best_drop_pid} {best_drop:.1f}"
            print(line)

        # 7) Snapshot if requested
        if snapshot_weeks is not None and week in snapshot_weeks:
            week_snapshots[week] = {
                int(pid): {
                    "mu": float(v["mu"]),
                    "phi": float(v["phi"]),
                    "sigma": float(v["sigma"]),
                    "last_week_seen": int(v["last_week_seen"])
                }
                for pid, v in state.items()
            }

        prev_loop_week = week

    pred_df = pd.DataFrame(predictions)
    sof_df = pd.DataFrame(sof_records)
    return state, pred_df, week_snapshots, sof_df