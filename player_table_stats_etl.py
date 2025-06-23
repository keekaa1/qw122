"""
player_table_stats_etl.py
-------------------------
Для каждого игрока и стола считает value-метрики (edge, winrate, tot_points, ov74_5, variance и т.д.)
Source: results (match_id, table_label, player1, player2, sc_ev, sc_ext_ev, finished)

Фильтр: matches_played >= 20 (везде, кроме стола A9 — >= 10)
Флаг cf_flag (⚠️ small_sample), edge_flag_table (True/False)

Author: GPT-4 + Кирилл, 2025
"""
import sqlite3
import pandas as pd
import numpy as np
import os
from collections import defaultdict

DB_PATH = 'betcity_results.db'
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"База данных не найдена по пути: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query("SELECT * FROM results", conn)

if not set(['match_id','table_label','player1','player2','sc_ev','sc_ext_ev','finished']).issubset(df.columns):
    raise Exception("В таблице results отсутствуют необходимые столбцы!")

def get_tot_points(sc_ext_ev):
    # Суммируем очки по всем сетам, пример: '11:8, 5:11, 6:11, 5:11, 7:11'
    total = 0
    for set_str in str(sc_ext_ev).split(','):
        if ':' in set_str or '-' in set_str:
            sep = ':' if ':' in set_str else '-'
            try:
                a, b = set_str.strip().split(sep)
                total += int(a) + int(b)
            except: pass
    return total if total > 0 else np.nan

def is_winner(row, player):
    # Возвращает True, если player выиграл матч (по sc_ev)
    try:
        left, right = [int(x) for x in str(row['sc_ev']).replace(':', '-').split('-')]
        if player == row['player1'] and left > right:
            return True
        if player == row['player2'] and right > left:
            return True
    except:
        pass
    return False

def rolling_avg(seq):
    seq = [x for x in seq if not np.isnan(x)]
    return np.mean(seq) if seq else np.nan

data = []
grouped = defaultdict(list)
# Собираем все матчи для каждого (player, table_label)
for idx, row in df.iterrows():
    for player in [row['player1'], row['player2']]:
        grouped[(player, row['table_label'])].append(row)

# Общая статистика по игроку для edge (по всем столам)
overall_stats = defaultdict(list)
for (player, _), rows in grouped.items():
    for r in rows:
        pts = get_tot_points(r['sc_ext_ev'])
        overall_stats[player].append({
            'win': is_winner(r, player),
            'tot_points': pts,
        })

overall_win_pct = {p: np.mean([x['win'] for x in stats]) for p, stats in overall_stats.items()}
overall_ov74_5 = {p: np.mean([(x['tot_points']>74.5) for x in stats if not np.isnan(x['tot_points'])]) for p, stats in overall_stats.items()}
overall_tot_pts_mu = {p: rolling_avg([x['tot_points'] for x in stats]) for p, stats in overall_stats.items()}
overall_tot_pts_std = {p: np.std([x['tot_points'] for x in stats if not np.isnan(x['tot_points'])]) for p, stats in overall_stats.items()}

for (player, table), matches in grouped.items():
    n = len(matches)
    min_sample = 10 if table == 'A9' else 20
    wins = [is_winner(r, player) for r in matches]
    tot_points = [get_tot_points(r['sc_ext_ev']) for r in matches]
    ov74_5 = [tp > 74.5 if not np.isnan(tp) else False for tp in tot_points]
    variance = np.std([tp for tp in tot_points if not np.isnan(tp)])
    # сравнение с общей std по игроку
    variance_factor = variance / overall_tot_pts_std[player] if overall_tot_pts_std[player]>0 else np.nan
    avg_pts_match_diff = rolling_avg(tot_points) - overall_tot_pts_mu[player] if not np.isnan(overall_tot_pts_mu[player]) else np.nan
    rec = {
        'player': player,
        'table_label': table,
        'matches_played': n,
        'win_pct': np.mean(wins) if n>0 else np.nan,
        'ov74_5_hit_pct': np.mean(ov74_5) if n>0 else np.nan,
        'avg_pts_match_diff': avg_pts_match_diff,
        'variance_factor': variance_factor,
        'edge_flag_table': False, # вычислим ниже
        'cf_flag': '',
    }
    # edge flag logic
    if n >= min_sample:
        if (rec['win_pct'] - overall_win_pct[player] >= 0.10) or (rec['ov74_5_hit_pct'] - overall_ov74_5[player] >= 0.10):
            rec['edge_flag_table'] = True
    else:
        rec['cf_flag'] = '⚠️ small_sample'
    data.append(rec)

result_df = pd.DataFrame(data)
result_df.to_sql('player_table_stats', conn, if_exists='replace', index=False)
print(f'player_table_stats обновлён! Только значения с min_sample (A9:10, остальные 20) edge-флагируются.')
conn.close()
