"""
player_style_etl.py
-------------------
Анализирует стиль каждого игрока ЛигиПро на основе матчей за год
и относит к одному из кластеров (aggressive, defensive, balanced, chaotic).

Используемые метрики для кластеризации:
- Средний тотал очков за матч
- Средняя длительность матча
- Средняя разница очков
- Средняя разница сетов

Алгоритм:
- Вычисляем эти фичи для всех игроков
- Нормализуем
- KMeans (k=4) с seed=42 (чтобы кластеры были стабильны)
- Присваиваем лейблы кластерам по средним значениям (ручная маркировка)
- Сохраняем в таблицу player_style (player_name, style, все средние метрики)

Author: GPT-4 + Кирилл
"""

import sqlite3
import pandas as pd
import numpy as np
import os
from datetime import datetime
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

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
    # Средний тотал очков
    total_pts = []
    pt_margin = []
    set_margin = []
    durations = []
    for idx, row in pm.iterrows():
        mid = row['match_id']
        s = set_scores[set_scores['match_id'] == mid]
        if row['player1'] == player:
            ppts, opts = s['p1_pts'].sum(), s['p2_pts'].sum()
            setm = row['p1_sets'] - row['p2_sets']
        else:
            ppts, opts = s['p2_pts'].sum(), s['p1_pts'].sum()
            setm = row['p2_sets'] - row['p1_sets']
        total = ppts + opts
        total_pts.append(total)
        pt_margin.append(ppts - opts)
        set_margin.append(setm)
        durations.append(row['duration_sec'] if row['duration_sec'] else 930)
    rows.append({
        'player_name': player,
        'matches_played': len(pm),
        'avg_total_pts': np.mean(total_pts),
        'avg_duration_sec': np.mean(durations),
        'avg_pt_margin': np.mean(pt_margin),
        'avg_set_margin': np.mean(set_margin),
    })

df = pd.DataFrame(rows)
features = ['avg_total_pts', 'avg_duration_sec', 'avg_pt_margin', 'avg_set_margin']
scaler = StandardScaler()
X = scaler.fit_transform(df[features])
kmeans = KMeans(n_clusters=4, random_state=42)
labels = kmeans.fit_predict(X)
df['cluster'] = labels

# Маркируем вручную (можно сделать динамически)
centers = kmeans.cluster_centers_
styles = {}
order = np.argsort(centers[:,0])  # по среднему тоталу
for i, c in enumerate(order):
    if i == 0:
        styles[c] = 'defensive'
    elif i == 1:
        styles[c] = 'balanced'
    elif i == 2:
        styles[c] = 'chaotic'
    else:
        styles[c] = 'aggressive'
df['style'] = df['cluster'].map(styles)

# Выводим топ-3 по каждому стилю
for st in df['style'].unique():
    print(f"Топ-3 {st}: ", df[df['style']==st].sort_values('avg_total_pts', ascending=(st=='defensive')).head(3)[['player_name','avg_total_pts']].values)

df_style = df[['player_name','matches_played','avg_total_pts','avg_duration_sec','avg_pt_margin','avg_set_margin','style']]
df_style.to_sql('player_style', conn, if_exists='replace', index=False)
print(f'Таблица player_style обновлена: {len(df_style)} игроков')
conn.close()
