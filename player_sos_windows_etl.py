"""
player_sos_windows_etl.py
------------------------
ETL для расчета Strength of Schedule (SoS) по разным окнам:
- последние 5, 10, 30 матчей (sos_last_5m, sos_last_10m, sos_last_30m)
- последние 2, 7, 30 дней (sos_last_2d, sos_last_7d, sos_last_30d)

cf_flag: ✅ если >=5 матчей в окне, ⚠️ если 1-4, ⛔ если 0 (NULL)

Результат: таблица player_sos_windows в betcity_results.db

Author: GPT-4 + Кирилл
"""

import sqlite3
import pandas as pd
import numpy as np
import os

DB_PATH = 'betcity_results.db'
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"База данных не найдена по пути: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
results = pd.read_sql_query("SELECT * FROM results", conn)
match_results = pd.read_sql_query("SELECT * FROM match_results", conn)
try:
    glicko2_snap = pd.read_sql_query("SELECT * FROM glicko2_snapshot", conn)
except Exception:
    print("glicko2_snapshot не найден — используем elo_final как proxy.")
    glicko2_snap = pd.read_sql_query("SELECT player_name, elo_final as rating, last_match_ts as snap_ts FROM player_elo", conn)

# Универсальный привод к datetime
for col in ['finished_ts', 'snap_ts', 'last_match_ts']:
    for df in [match_results, results, glicko2_snap]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

match_results = match_results.merge(
    results[['match_id', 'player1', 'player2']],
    on='match_id', how='left')
players = pd.unique(pd.concat([results['player1'], results['player2']])).tolist()

rows = []
for player in players:
    pm = match_results[(match_results['player1'] == player) | (match_results['player2'] == player)].copy()
    pm = pm.sort_values('finished_ts')
    if len(pm) == 0:
        continue
    pm['opponent'] = pm.apply(lambda row: row['player2'] if row['player1'] == player else row['player1'], axis=1)
    # Для каждого матча — рейтинг соперника на этот день
    def get_opp_rating(opp_name, snap_ts):
        subset = glicko2_snap[glicko2_snap['player_name'] == opp_name]
        subset = subset.dropna(subset=['snap_ts', 'rating'])
        subset = subset[subset['snap_ts'] <= snap_ts]
        if len(subset) == 0:
            subset = glicko2_snap[glicko2_snap['player_name'] == opp_name]
        if len(subset) == 0:
            return np.nan
        return subset.sort_values('snap_ts').iloc[-1]['rating']
    pm['opp_rating'] = pm.apply(lambda row: get_opp_rating(row['opponent'], row['finished_ts']), axis=1)
    pm = pm.dropna(subset=['opp_rating'])
    # Окна по матчам
    def sos_last_nm(n):
        vals = pm['opp_rating'].tail(n)
        cf = '✅' if len(vals) >= 5 else ('⚠️' if len(vals) > 0 else '⛔')
        return (vals.mean() if len(vals) > 0 else np.nan, cf)
    # Окна по дням
    now = pd.Timestamp.now()
    def sos_last_nd(days):
        vals = pm[pm['finished_ts'] >= now - pd.Timedelta(days=days)]['opp_rating']
        cf = '✅' if len(vals) >= 5 else ('⚠️' if len(vals) > 0 else '⛔')
        return (vals.mean() if len(vals) > 0 else np.nan, cf)
    row = {'player_name': player}
    for n in [5, 10, 30]:
        mean, cf = sos_last_nm(n)
        row[f'sos_last_{n}m'] = mean
        row[f'sos_last_{n}m_cf'] = cf
    for d in [2, 7, 30]:
        mean, cf = sos_last_nd(d)
        row[f'sos_last_{d}d'] = mean
        row[f'sos_last_{d}d_cf'] = cf
    rows.append(row)

df_sos = pd.DataFrame(rows)
df_sos.to_sql('player_sos_windows', conn, if_exists='replace', index=False)
print(f'Таблица player_sos_windows обновлена: {len(df_sos)} игроков')
conn.close()
