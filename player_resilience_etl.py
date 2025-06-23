"""
player_resilience_etl.py
-----------------------
Расчёт resilience, volatility, clutch, surrender для каждого игрока ЛигиПро за год.

Метрики:
- % побед после проигранного первого сета (comeback_1set_win)
- % побед после 0:2 (comeback_0_2_win, только BO5)
- % проигранных матчей с 0:2 (surrender_0_2_lose)
- % матчей, где выигран решающий сет (clutch_win, BO3/BO5)
- % сухих побед (dry_win, 3:0 / 2:0)
- std по разнице очков и сетов за год (volatility_pts, volatility_sets)
- медианная разница очков в проигранных матчах (median_lose_margin)
- средняя длина comeback-серии (comeback_streak_avg)

Сохраняет: player_resilience (player_name, все метрики)

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
set_scores = pd.read_sql_query("SELECT * FROM set_scores", conn)

match_results = match_results.merge(
    results[['match_id', 'player1', 'player2']],
    on='match_id', how='left')
match_results['finished_ts'] = pd.to_datetime(match_results['finished_ts'], errors='coerce')
last_year = match_results[match_results['finished_ts'] >= pd.Timestamp.now() - pd.Timedelta(days=365)]
players = pd.unique(pd.concat([results['player1'], results['player2']])).tolist()

rows = []
for player in players:
    pm = last_year[(last_year['player1'] == player) | (last_year['player2'] == player)].copy()
    pm = pm.sort_values('finished_ts')
    if len(pm) < 10:
        continue
    comebacks_1set = 0
    comebacks_0_2 = 0
    surrenders_0_2 = 0
    clutch_win = 0
    dry_win = 0
    clutch_total = 0
    lose_margins = []
    pt_margins = []
    set_margins = []
    streak_lens = []
    for idx, row in pm.iterrows():
        mid = row['match_id']
        s = set_scores[set_scores['match_id'] == mid].sort_values('set_no')
        bo = max(row['p1_sets'], row['p2_sets'])
        if row['player1'] == player:
            is_win = row['p1_sets'] > row['p2_sets']
            p_sets = s['p1_pts']
            o_sets = s['p2_pts']
            first_set_win = s.iloc[0]['p1_pts'] > s.iloc[0]['p2_pts'] if len(s) > 0 else False
        else:
            is_win = row['p2_sets'] > row['p1_sets']
            p_sets = s['p2_pts']
            o_sets = s['p1_pts']
            first_set_win = s.iloc[0]['p2_pts'] > s.iloc[0]['p1_pts'] if len(s) > 0 else False
        # dry win (3:0/2:0)
        if is_win and (row['p1_sets'] == 3 or row['p2_sets'] == 3 or row['p1_sets'] == 2 or row['p2_sets'] == 2) and (abs(row['p1_sets'] - row['p2_sets']) == max(row['p1_sets'], row['p2_sets'])):
            dry_win += 1
        # comeback 1st set lose
        if not first_set_win and is_win:
            comebacks_1set += 1
        # comeback 0:2 (BO5 only)
        if bo == 3:
            if (row['player1'] == player and row['p1_sets'] == 3 and row['p2_sets'] == 2 and list(s['p1_pts'][:2] < s['p2_pts'][:2]) == [True, True]) or \
               (row['player2'] == player and row['p2_sets'] == 3 and row['p1_sets'] == 2 and list(s['p2_pts'][:2] < s['p1_pts'][:2]) == [True, True]):
                comebacks_0_2 += 1
        # surrender 0:2 (BO5 only)
        if bo == 3:
            if (row['player1'] == player and row['p1_sets'] == 2 and row['p2_sets'] == 3 and list(s['p1_pts'][:2] < s['p2_pts'][:2]) == [True, True]) or \
               (row['player2'] == player and row['p2_sets'] == 2 and row['p1_sets'] == 3 and list(s['p2_pts'][:2] < s['p1_pts'][:2]) == [True, True]):
                surrenders_0_2 += 1
        # clutch win (выиграл решающий сет)
        if (bo == 3 and abs(row['p1_sets'] - row['p2_sets']) == 1) or (bo == 2 and row['p1_sets'] == row['p2_sets']):
            if is_win:
                clutch_win += 1
            clutch_total += 1
        # проигрыш — margin для std/median
        margin_pts = p_sets.sum() - o_sets.sum()
        pt_margins.append(margin_pts)
        set_margin = row['p1_sets']-row['p2_sets'] if row['player1'] == player else row['p2_sets']-row['p1_sets']
        set_margins.append(set_margin)
        if not is_win:
            lose_margins.append(margin_pts)
        # comeback streaks (выиграл N сетов подряд после отставания)
        # простая реализация: макс streak после минуса
        s_player = list(p_sets.values)
        s_opp = list(o_sets.values)
        d = np.array(s_player) - np.array(s_opp)
        losing = d < 0
        streak = 0
        max_streak = 0
        for i in range(1, len(d)):
            if losing[i-1] and d[i] > 0:
                streak = 1
            elif streak > 0 and d[i] > 0:
                streak += 1
            else:
                streak = 0
            max_streak = max(max_streak, streak)
        if max_streak:
            streak_lens.append(max_streak)
    total = len(pm)
    resilience_row = {
        'player_name': player,
        'matches_played': total,
        'comeback_1set_win': comebacks_1set / total,
        'comeback_0_2_win': comebacks_0_2 / total,
        'surrender_0_2_lose': surrenders_0_2 / total,
        'clutch_win': clutch_win / clutch_total if clutch_total else np.nan,
        'dry_win': dry_win / total,
        'volatility_pts': np.std(pt_margins),
        'volatility_sets': np.std(set_margins),
        'median_lose_margin': np.median(lose_margins) if lose_margins else np.nan,
        'comeback_streak_avg': np.mean(streak_lens) if streak_lens else np.nan,
    }
    rows.append(resilience_row)

df = pd.DataFrame(rows)
df.to_sql('player_resilience', conn, if_exists='replace', index=False)
print(f'Таблица player_resilience обновлена: {len(df)} игроков')
conn.close()
