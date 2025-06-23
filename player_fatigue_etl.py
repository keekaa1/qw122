"""
player_fatigue_etl.py
---------------------
Агрегация метрик усталости (fatigue) для каждого игрока ЛигиПро за последний год.

- Средний отдых между матчами (hours)
- Процент матчей с отдыхом < 4h, < 8h
- Среднее penalty за короткий отдых (skew по winrate, можно добавить дальше)
- Доля ночных матчей, средний winrate ночью/днем
- Доля back-to-back дней (2+ матча в сутки)
- Все метрики — за 365 дней (или по всей истории, если мало матчей)
- Сохраняет таблицу player_fatigue

Author: GPT-4 + Кирилл
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

DB_PATH = 'betcity_results.db'
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"База данных не найдена по пути: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
results = pd.read_sql_query("SELECT * FROM results", conn)
match_results = pd.read_sql_query("SELECT * FROM match_results", conn)

# Мерджим имена игроков в матчах
match_results = match_results.merge(
    results[['match_id', 'player1', 'player2']],
    on='match_id', how='left'
)

# Время окончания матча
match_results['finished_ts'] = pd.to_datetime(match_results['finished_ts'], errors='coerce')

# За год
last_year = match_results[match_results['finished_ts'] >= pd.Timestamp.now() - pd.Timedelta(days=365)]

# Список игроков
players = pd.unique(pd.concat([results['player1'], results['player2']])).tolist()

fatigue_rows = []
for player in players:
    pm = last_year[(last_year['player1'] == player) | (last_year['player2'] == player)]
    pm = pm.sort_values('finished_ts')
    if len(pm) < 3:
        continue
    # Вычисляем интервалы отдыха (между start-таймами)
    rest_hours = []
    back_to_back_days = set()
    last_start = None
    night_matches = 0
    night_wins = 0
    total_wins = 0
    for idx, row in pm.iterrows():
        # Старт = финиш - длительность
        dur = row['duration_sec'] if row['duration_sec'] and row['duration_sec'] > 0 else 930
        start_ts = row['finished_ts'] - pd.Timedelta(seconds=dur)
        # Отдых между матчами
        if last_start is not None:
            rest = (start_ts - last_start).total_seconds() / 3600
            rest_hours.append(rest)
        last_start = start_ts
        # Ночные матчи (старт в 22:00–07:00)
        if 22 <= start_ts.hour or start_ts.hour < 7:
            night_matches += 1
            # Победа
            is_win = (row['player1'] == player and row['p1_sets'] > row['p2_sets']) or (row['player2'] == player and row['p2_sets'] > row['p1_sets'])
            if is_win:
                night_wins += 1
        # Все победы
        is_win = (row['player1'] == player and row['p1_sets'] > row['p2_sets']) or (row['player2'] == player and row['p2_sets'] > row['p1_sets'])
        if is_win:
            total_wins += 1
        # Back-to-back
        date = start_ts.date()
        if date in back_to_back_days:
            pass  # уже был матч в этот день
        else:
            back_to_back_days.add(date)

    avg_rest_h = np.mean(rest_hours) if rest_hours else np.nan
    pct_rest_lt4h = np.mean(np.array(rest_hours) < 4) if rest_hours else np.nan
    pct_rest_lt8h = np.mean(np.array(rest_hours) < 8) if rest_hours else np.nan
    pct_back_to_back = np.nan
    if len(pm) > 1:
        btb_count = sum([list(pm['finished_ts'].dt.date).count(d) > 1 for d in set(pm['finished_ts'].dt.date)])
        pct_back_to_back = btb_count / len(set(pm['finished_ts'].dt.date))
    pct_night_matches = night_matches / len(pm)
    winrate_night = night_wins / night_matches if night_matches else np.nan
    winrate_all = total_wins / len(pm)

    fatigue_rows.append({
        'player_name': player,
        'matches_played': len(pm),
        'avg_rest_hours': avg_rest_h,
        'pct_rest_lt4h': pct_rest_lt4h,
        'pct_rest_lt8h': pct_rest_lt8h,
        'pct_back_to_back_days': pct_back_to_back,
        'pct_night_matches': pct_night_matches,
        'winrate_night': winrate_night,
        'winrate_all': winrate_all,
    })

fatigue_df = pd.DataFrame(fatigue_rows)
fatigue_df.to_sql('player_fatigue', conn, if_exists='replace', index=False)
print(f'Таблица player_fatigue обновлена: {len(fatigue_df)} игроков')
conn.close()
