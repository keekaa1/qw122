"""
league_reference_etl_ultimate.py
-------------------------------
- Честная агрегация value-метрик лиги по всем окнам и слотам.
- Вдохновлен рабочими версиями: только поштучная обработка!
- sc_ext_ev, sc_ev, finished - обязательны.
- Нет Series внутри функций, все по apply(axis=1).
- Тоталы: ov65_5, ov70_5, ov74_5, ov75_5, ov76_5, ov78_5, ov80_5, ov85_5.
- dry_win_mu, точные счета, камбэк, равные игры, медиана, min, max tot_points.

Author: GPT-4 + Кирилл
"""
import sqlite3
import pandas as pd
import numpy as np
import os

def get_tot_points(row):
    sc_ext_ev = row['sc_ext_ev']
    if pd.isnull(sc_ext_ev): return np.nan
    total = 0
    for set_str in str(sc_ext_ev).split(','):
        set_str = set_str.strip()
        if not set_str: continue
        if ':' in set_str:
            try:
                a, b = set_str.split(':')
                total += int(a) + int(b)
            except: continue
        elif '-' in set_str:
            try:
                a, b = set_str.split('-')
                total += int(a) + int(b)
            except: continue
    return total if total > 0 else np.nan

def get_pts_diff(row):
    sc_ext_ev = row['sc_ext_ev']
    if pd.isnull(sc_ext_ev): return np.nan
    p1, p2 = 0, 0
    for set_str in str(sc_ext_ev).split(','):
        set_str = set_str.strip()
        if not set_str: continue
        if ':' in set_str:
            try:
                a, b = set_str.split(':')
                p1 += int(a)
                p2 += int(b)
            except: continue
        elif '-' in set_str:
            try:
                a, b = set_str.split('-')
                p1 += int(a)
                p2 += int(b)
            except: continue
    return abs(p1 - p2) if (p1 > 0 or p2 > 0) else np.nan

def come_from_behind(row):
    try:
        sets = str(row['sc_ext_ev']).split(',')
        if not sets or len(sets) < 1: return np.nan
        first_set = sets[0].strip()
        if not first_set: return np.nan
        sep = ':' if ':' in first_set else '-'
        fs = [int(x) for x in first_set.split(sep)]
        left, right = [int(x) for x in str(row['sc_ev']).replace(':','-').split('-')]
        winner = 1 if left > right else 2
        if (winner == 1 and fs[0] < fs[1]) or (winner == 2 and fs[1] < fs[0]):
            return 1
        return 0
    except: return np.nan

def get_score_code(row):
    try:
        left, right = [int(x) for x in str(row['sc_ev']).replace(':','-').split('-')]
        s = f"{max(left,right)}-{min(left,right)}"
        return s
    except: return None

def get_slot(row):
    h = row['finished_ts'].hour if not pd.isnull(row['finished_ts']) else None
    if h is None: return 'unknown'
    if (h >= 23 or h < 7): return 'night'
    if (h >= 7 and h < 10): return 'morning'
    if (h >= 10 and h < 18): return 'day'
    if (h >= 18 and h < 23): return 'evening'
    return 'unknown'

DB_PATH = 'betcity_results.db'
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"База данных не найдена по пути: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query("SELECT * FROM results", conn)

for col in ['sc_ext_ev','sc_ev','finished']:
    if col not in df.columns:
        raise Exception(f"В таблице results нет нужного столбца: {col}")

df['finished_ts'] = pd.to_datetime(df['finished'], errors='coerce')
df = df[df['finished_ts'].notna()].copy()
df['slot'] = df.apply(get_slot, axis=1)
now = pd.Timestamp.now()

df['tot_points'] = df.apply(get_tot_points, axis=1)
df['pts_diff'] = df.apply(get_pts_diff, axis=1)
df['come_from_behind'] = df.apply(come_from_behind, axis=1)
df['score_code'] = df.apply(get_score_code, axis=1)

windows = {
    '1d': 1,
    '3d': 3,
    '7d': 7,
    '30d': 30,
    '365d': 365,
}
totals = [65.5, 70.5, 74.5, 75.5, 76.5, 78.5, 80.5, 85.5]
rows = []
for win_label, days in windows.items():
    window_start = now - pd.Timedelta(days=days)
    df_win = df[df['finished_ts'] >= window_start].copy()
    for slot in ['night','morning','day','evening','all']:
        if slot == 'all':
            df_slot = df_win.copy()
        else:
            df_slot = df_win[df_win['slot']==slot]
        if len(df_slot) == 0: continue
        score_counts = df_slot['score_code'].value_counts(normalize=True)
        rec = {
            'window': win_label,
            'slot': slot,
            'n_matches': len(df_slot),
            'mean_tot_points': df_slot['tot_points'].mean(),
            'median_tot_points': df_slot['tot_points'].median(),
            'min_tot_points': df_slot['tot_points'].min(),
            'max_tot_points': df_slot['tot_points'].max(),
            'dry_win_mu': score_counts.get('3-0', 0),
            'score_3_2_mu': score_counts.get('3-2', 0),
            'score_3_1_mu': score_counts.get('3-1', 0),
            'score_3_0_mu': score_counts.get('3-0', 0),
            'come_from_behind_mu': df_slot['come_from_behind'].mean(),
            'pts_diff_lt8': (df_slot['pts_diff'] < 8).mean(),
            'pts_diff_lt12': (df_slot['pts_diff'] < 12).mean(),
        }
        for t in totals:
            rec[f'ov{str(t).replace(".","_")}_mu'] = (df_slot['tot_points'] > t).mean()
        rows.append(rec)

league_df = pd.DataFrame(rows)
league_df.to_sql('league_reference_by_time', conn, if_exists='replace', index=False)
print(f'Таблица league_reference_by_time обновлена! (all value-метрики по results)')
conn.close()
