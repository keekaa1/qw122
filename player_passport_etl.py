"""
player_passport_etl.py
---------------------
Финальный ETL-скрипт, собирающий "паспорт" каждого игрока ЛигиПро.

- Подтягивает все результаты из:
    - player_elo
    - player_fatigue
    - player_style
    - player_resilience
    - player_h2h
- Объединяет по player_name (left join)
- Сохраняет результат в player_passport (готов к ML/аналитике/выгрузке)

Author: GPT-4 + Кирилл
"""

import sqlite3
import pandas as pd
import os

DB_PATH = 'betcity_results.db'
if not os.path.exists(DB_PATH):
    raise FileNotFoundError(f"База данных не найдена по пути: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
elo = pd.read_sql_query("SELECT * FROM player_elo", conn)
fatigue = pd.read_sql_query("SELECT * FROM player_fatigue", conn)
style = pd.read_sql_query("SELECT * FROM player_style", conn)
resilience = pd.read_sql_query("SELECT * FROM player_resilience", conn)
h2h = pd.read_sql_query("SELECT * FROM player_h2h", conn)

# Собираем master-list игроков (все кто был хотя бы в одном из модулей)
players = pd.unique(pd.concat([
    elo['player_name'], fatigue['player_name'], style['player_name'],
    resilience['player_name'], h2h['player_name']
])).tolist()

passport = pd.DataFrame({'player_name': players})
passport = passport.merge(elo, on='player_name', how='left')
passport = passport.merge(fatigue, on='player_name', how='left')
passport = passport.merge(style, on='player_name', how='left')
passport = passport.merge(resilience, on='player_name', how='left')
passport = passport.merge(h2h, on='player_name', how='left')

passport.to_sql('player_passport', conn, if_exists='replace', index=False)
print(f'Паспорт игроков собран: {len(passport)} строк. Таблица player_passport обновлена!')
conn.close()
