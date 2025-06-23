"""
player_h2h_etl.py
-----------------
Агрегация H2H winrate каждого игрока против разных стилей соперников.

- Для каждого игрока считает winrate (и overrate, если нужно) против:
    - aggressive
    - defensive
    - balanced
    - chaotic
- Стили соперников берутся из таблицы player_style
- Только матчи за год (finished_ts)
- В таблице player_h2h сохраняет:
    player_name,
    h2h_vs_aggressive_winrate,
    h2h_vs_defensive_winrate,
    h2h_vs_balanced_winrate,
    h2h_vs_chaotic_winrate,
    ... (всё аналогично)

Author: GPT-4 + Кирилл
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime

DB_PATH = 'betcity_results.db'
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"База данных не найдена по пути: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
results = pd.read_sql_query("SELECT * FROM results", conn)
match_results = pd.read_sql_query("SELECT * FROM match_results", conn)
player_style = pd.read_sql_query("SELECT * FROM player_style", conn)

match_results = match_results.merge(
    results[['match_id', 'player1', 'player2']],
    on='match_id', how='left')
match_results['finished_ts'] = pd.to_datetime(match_results['finished_ts'], errors='coerce')
last_year = match_results[match_results['finished_ts'] >= pd.Timestamp.now() - pd.Timedelta(days=365)]

# Получаем стили соперников
style_dict = dict(zip(player_style['player_name'], player_style['style']))

players = pd.unique(pd.concat([results['player1'], results['player2']])).tolist()

rows = []
for player in players:
    pm = last_year[(last_year['player1'] == player) | (last_year['player2'] == player)].copy()
    pm = pm.sort_values('finished_ts')
    if len(pm) < 5:
        continue
    h2h = {s: {'win': 0, 'total': 0} for s in ['aggressive', 'defensive', 'balanced', 'chaotic']}
    for idx, row in pm.iterrows():
        # Определяем соперника
        if row['player1'] == player:
            opp = row['player2']
            is_win = row['p1_sets'] > row['p2_sets']
        else:
            opp = row['player1']
            is_win = row['p2_sets'] > row['p1_sets']
        style = style_dict.get(opp)
        if style in h2h:
            h2h[style]['total'] += 1
            if is_win:
                h2h[style]['win'] += 1
    result = {'player_name': player}
    for style in h2h:
        key = f'h2h_vs_{style}_winrate'
        total = h2h[style]['total']
        result[key] = h2h[style]['win'] / total if total else np.nan
    rows.append(result)

df_h2h = pd.DataFrame(rows)
df_h2h.to_sql('player_h2h', conn, if_exists='replace', index=False)
print(f'Таблица player_h2h обновлена: {len(df_h2h)} игроков')
conn.close()
