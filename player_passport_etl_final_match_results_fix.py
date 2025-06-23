import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

DB_PATH = 'betcity_results.db'
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"База данных не найдена по пути: {DB_PATH}\nСкопируй файл betcity_results.db в эту же папку.")

conn = sqlite3.connect(DB_PATH)

def table_and_columns(conn):
    tbls = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)['name'].tolist()
    info = {}
    for t in tbls:
        try:
            cols = pd.read_sql_query(f"PRAGMA table_info({t})", conn)
            info[t] = list(cols['name'])
        except Exception as e:
            info[t] = [f"Could not read columns: {e}"]
    return info

info = table_and_columns(conn)
print('\nВ базе найдены таблицы и столбцы:')
for t, cols in info.items():
    print(f'  {t}: {cols}')

# --- Грузим match_results и set_scores ---
match_results = pd.read_sql_query("SELECT * FROM match_results", conn)
set_scores = pd.read_sql_query("SELECT * FROM set_scores", conn)

# --- Грузим имена игроков из results ---
results = pd.read_sql_query("SELECT * FROM results", conn)

# Получаем мапу: match_id -> (player1, player2)
matchid2players = results.set_index('match_id')[['player1', 'player2']].to_dict('index')

# Добавляем player1/player2 к match_results (мердж по match_id)
match_results = match_results.merge(
    results[['match_id', 'player1', 'player2']],
    how='left', on='match_id',
)

match_results['finished_ts'] = pd.to_datetime(match_results['finished_ts'], errors='coerce')
recent_matches = match_results[
    (match_results['finished_ts'] >= pd.Timestamp.now() - pd.Timedelta(days=365))
].copy()

# Список всех игроков по results
players = pd.unique(pd.concat([
    results['player1'],
    results['player2']
])).tolist()

# League rolling avg duration
recent_30d = match_results[
    match_results['finished_ts'] >= pd.Timestamp.now() - pd.Timedelta(days=30)
]
league_avg_duration_30d = recent_30d['duration_sec'].replace(0, np.nan).dropna().mean()
if np.isnan(league_avg_duration_30d):
    league_avg_duration_30d = 930

def safe_div(a, b):
    return float(a)/b if b else np.nan

passports = []
for player in players:
    pm = recent_matches[(recent_matches['player1'] == player) | (recent_matches['player2'] == player)].sort_values('finished_ts')
    if len(pm) < 3:
        continue

    pm = pm.copy()
    pm['is_win'] = np.where(
        ((pm['player1'] == player) & (pm['p1_sets'] > pm['p2_sets'])) |
        ((pm['player2'] == player) & (pm['p2_sets'] > pm['p1_sets'])), 1, 0
    )
    set_margins = np.where(pm['player1'] == player, pm['p1_sets'] - pm['p2_sets'], pm['p2_sets'] - pm['p1_sets'])
    avg_set_margin = np.mean(set_margins)
    set_margin_sd = np.std(set_margins, ddof=1)
    matches_played = len(pm)
    win_pct_all = safe_div(pm['is_win'].sum(), matches_played)
    win_pct_20 = safe_div(pm.tail(20)['is_win'].sum(), min(20, matches_played))

    total_pts = []
    for idx, row in pm.iterrows():
        mid = row['match_id']
        s = set_scores[set_scores['match_id'] == mid]
        pts = s['p1_pts'].sum() + s['p2_pts'].sum() if not s.empty else np.nan
        total_pts.append(pts)
    pm['total_pts'] = total_pts

    ov70_5_hit_pct = safe_div((pm['total_pts'] > 70.5).sum(), matches_played)
    ov72_5_hit_pct = safe_div((pm['total_pts'] > 72.5).sum(), matches_played)
    ov74_5_hit_pct = safe_div((pm['total_pts'] > 74.5).sum(), matches_played)
    ov75_5_hit_pct = safe_div((pm['total_pts'] > 75.5).sum(), matches_played)
    ov78_5_hit_pct = safe_div((pm['total_pts'] > 78.5).sum(), matches_played)
    ov80_5_hit_pct = safe_div((pm['total_pts'] > 80.5).sum(), matches_played)

    set_covers_m1_5 = (set_margins >= 2).sum()
    set_covers_p1_5 = ((pm['is_win'] == 1).sum() + (((set_margins == -1) & (pm['is_win'] == 0)).sum()))
    cover_set_m1_5_pct = safe_div(set_covers_m1_5, matches_played)
    cover_set_p1_5_pct = safe_div(set_covers_p1_5, matches_played)

    p_points, o_points = [], []
    for idx, row in pm.iterrows():
        mid = row['match_id']
        s = set_scores[set_scores['match_id'] == mid]
        if row['player1'] == player:
            ppts, opts = s['p1_pts'].sum(), s['p2_pts'].sum()
        else:
            ppts, opts = s['p2_pts'].sum(), s['p1_pts'].sum()
        p_points.append(ppts)
        o_points.append(opts)
    pm['player_pts'] = p_points
    pm['opp_pts'] = o_points
    pt_margin = pm['player_pts'] - pm['opp_pts']
    cover_pt_m3_5_pct = safe_div(((pt_margin >= 4) & (pm['is_win'] == 1)).sum(), (pm['is_win'] == 1).sum())
    cover_pt_p3_5_pct = safe_div(((pt_margin >= -3) & (pm['is_win'] == 0)).sum(), (pm['is_win'] == 0).sum())

    duration_flags = []
    rolling_avg = league_avg_duration_30d
    for dur in pm['duration_sec']:
        if pd.isna(dur) or dur <= 0:
            duration_flags.append('missing')
        elif dur < 600:
            duration_flags.append('short')
        elif dur > 2 * rolling_avg:
            duration_flags.append('long')
        else:
            duration_flags.append('ok')
    pm['duration_flag'] = duration_flags
    avg_match_duration_sec = pm['duration_sec'].replace(0, np.nan).dropna().mean() or rolling_avg
    pct_matches_gt18min = safe_div((pm['duration_sec'] > 1080).sum(), matches_played)

    fatigue_adj_rest_h = []
    last_start = None
    for idx, row in pm.iterrows():
        duration = row['duration_sec'] if row['duration_sec'] > 0 else rolling_avg
        start_ts = row['finished_ts'] - pd.Timedelta(seconds=duration)
        if last_start is not None:
            rest_h = (start_ts - last_start).total_seconds() / 3600
            if rest_h <= 0.01:
                rest_h = 0.10
            fatigue_adj_rest_h.append(rest_h)
        else:
            fatigue_adj_rest_h.append(np.nan)
        last_start = start_ts
    avg_rest_hours = np.nanmean([r for r in fatigue_adj_rest_h if not pd.isna(r)])
    pct_rest_lt8h = safe_div(np.sum(np.array(fatigue_adj_rest_h) < 8), len([r for r in fatigue_adj_rest_h if not pd.isna(r)]))

    passport = {
        'player_name': player,
        'matches_played': matches_played,
        'win_pct_all': win_pct_all,
        'win_pct_20': win_pct_20,
        'avg_set_margin': avg_set_margin,
        'set_margin_sd': set_margin_sd,
        'ov70_5_hit_pct': ov70_5_hit_pct,
        'ov72_5_hit_pct': ov72_5_hit_pct,
        'ov74_5_hit_pct': ov74_5_hit_pct,
        'ov75_5_hit_pct': ov75_5_hit_pct,
        'ov78_5_hit_pct': ov78_5_hit_pct,
        'ov80_5_hit_pct': ov80_5_hit_pct,
        'cover_set_m1_5_pct': cover_set_m1_5_pct,
        'cover_set_p1_5_pct': cover_set_p1_5_pct,
        'cover_pt_m3_5_pct': cover_pt_m3_5_pct,
        'cover_pt_p3_5_pct': cover_pt_p3_5_pct,
        'avg_match_duration_sec': avg_match_duration_sec,
        'pct_matches_gt18min': pct_matches_gt18min,
        'avg_rest_hours': avg_rest_hours,
        'pct_rest_lt8h': pct_rest_lt8h,
    }
    passports.append(passport)

df_pass = pd.DataFrame(passports)
if not df_pass.empty:
    df_pass.to_sql('player_passports', conn, if_exists='replace', index=False)
    print(f"Готово: {len(df_pass)} паспортов обновлено. Файл БД: {DB_PATH}")
else:
    print(f"Нет данных для паспортов (слишком мало матчей или фильтрация). Файл БД: {DB_PATH}")
conn.close()
