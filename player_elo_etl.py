"""
player_elo_etl.py
-----------------
Динамический расчет Elo-рейтинга для каждого игрока ЛигиПро.

- Стартовое значение: 1000
- K-фактор: 32 (до 30 матчей), 28 (31–100), 24 (101–300), 20 (301–600), 16 (>600)
- Decay: -0.03% Elo за каждый день простоя, начиная с 4-го дня
- Минимальное значение Elo: 800
- Для каждого игрока сохраняется финальный Elo, total_matches, последний матч (ts), дата последней активности, decay_penalty
- История Elo по матчам сохраняется в таблицу 'player_elo_history' (для графиков и ML)
- Таблица player_elo: итоговый рейтинг на сегодня

Author: GPT-4 + Твои правки
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

# Берём игроков из results
results = pd.read_sql_query("SELECT * FROM results", conn)
match_results = pd.read_sql_query("SELECT * FROM match_results", conn)

# Датафрейм для истории эло
elo_history = []

# Стартовые значения
START_ELO = 1000
MIN_ELO = 800

def k_factor(n_games):
    if n_games <= 30:
        return 32
    elif n_games <= 100:
        return 28
    elif n_games <= 300:
        return 24
    elif n_games <= 600:
        return 20
    else:
        return 16

def expected_score(elo_a, elo_b):
    return 1.0 / (1.0 + 10**((elo_b - elo_a) / 400))

def apply_decay(elo, days_idle):
    if days_idle <= 3:
        return elo, 0
    decay_days = days_idle - 3
    penalty = elo * 0.0003 * decay_days
    new_elo = max(MIN_ELO, elo - penalty)
    return new_elo, penalty

# Хронологический список матчей
all_matches = match_results.merge(
    results[['match_id', 'player1', 'player2']],
    on='match_id', how='left'
).sort_values('finished_ts')

# Собираем список всех игроков
players = pd.unique(pd.concat([results['player1'], results['player2']])).tolist()
player_elo = {p: START_ELO for p in players}
player_last_match = {p: None for p in players}
player_games = {p: 0 for p in players}
player_decay = {p: 0 for p in players}

# Проход по всем матчам по хронологии
for idx, row in all_matches.iterrows():
    p1, p2 = row['player1'], row['player2']
    ts = row['finished_ts']
    if pd.isna(ts):
        continue
    ts = pd.to_datetime(ts, errors='coerce')
    # Decay если с прошлого матча прошло >3 дня
    for p in [p1, p2]:
        last_ts = player_last_match[p]
        if last_ts is not None:
            days_idle = (ts - last_ts).days
            old_elo = player_elo[p]
            new_elo, penalty = apply_decay(old_elo, days_idle)
            player_elo[p] = new_elo
            player_decay[p] += penalty
    # Текущий эло
    elo1, elo2 = player_elo[p1], player_elo[p2]
    # Считаем, кто выиграл
    if (row['p1_sets'] > row['p2_sets'] and row['player1'] == p1) or \
       (row['p2_sets'] > row['p1_sets'] and row['player2'] == p1):
        s1, s2 = 1, 0
    elif (row['p2_sets'] > row['p1_sets'] and row['player1'] == p1) or \
         (row['p1_sets'] > row['p2_sets'] and row['player2'] == p1):
        s1, s2 = 0, 1
    else:
        s1 = s2 = 0.5  # draw, если ничья (теоретически)

    # Текущий K
    k1, k2 = k_factor(player_games[p1]), k_factor(player_games[p2])
    e1 = expected_score(elo1, elo2)
    e2 = expected_score(elo2, elo1)
    # Обновляем рейтинг
    new_elo1 = max(MIN_ELO, elo1 + k1 * (s1 - e1))
    new_elo2 = max(MIN_ELO, elo2 + k2 * (s2 - e2))
    # Запоминаем
    elo_history.append({
        'player': p1,
        'match_id': row['match_id'],
        'elo_before': elo1,
        'elo_after': new_elo1,
        'opponent': p2,
        'date': ts,
        'result': s1,
        'k': k1,
        'decay_penalty': player_decay[p1],
    })
    elo_history.append({
        'player': p2,
        'match_id': row['match_id'],
        'elo_before': elo2,
        'elo_after': new_elo2,
        'opponent': p1,
        'date': ts,
        'result': s2,
        'k': k2,
        'decay_penalty': player_decay[p2],
    })
    player_elo[p1] = new_elo1
    player_elo[p2] = new_elo2
    player_last_match[p1] = ts
    player_last_match[p2] = ts
    player_games[p1] += 1
    player_games[p2] += 1

# Сохраняем финальные эло в DataFrame
elo_list = []
for p in players:
    last_ts = player_last_match[p]
    elo_list.append({
        'player_name': p,
        'elo_final': player_elo[p],
        'total_matches': player_games[p],
        'last_match_ts': last_ts,
        'total_decay_penalty': player_decay[p]
    })
df_elo = pd.DataFrame(elo_list)
df_elo.to_sql('player_elo', conn, if_exists='replace', index=False)
print(f'Таблица player_elo обновлена: {len(df_elo)} игроков')

# Сохраняем историю
df_hist = pd.DataFrame(elo_history)
df_hist.to_sql('player_elo_history', conn, if_exists='replace', index=False)
print(f'Таблица player_elo_history обновлена: {len(df_hist)} записей (по всем матчам)')

conn.close()
