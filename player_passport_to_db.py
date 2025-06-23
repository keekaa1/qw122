import sqlite3
from datetime import datetime, timedelta
import numpy as np
import sys
import json

DB_PATH = 'betcity_results.db'
TABLES = ['A3','A4','A5','A6','A9']
NIGHT_START = 0
NIGHT_END = 7
FATIGUE_THRESHOLD = 0.8
ROLLING_N = 120

# Вспомогательные

def is_night(ts: str) -> bool:
    try:
        dt = datetime.fromisoformat(ts)
        return NIGHT_START <= dt.hour < NIGHT_END
    except Exception:
        return False

def streak_calc(results):
    max_streak = streak = 0
    for win in results:
        if win:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak

# Создать таблицу для паспортов игроков

def ensure_passport_table(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS player_passports (
            player_name TEXT PRIMARY KEY,
            json_blob TEXT,
            last_updated TEXT
        )
    """)
    conn.commit()
    conn.close()

# Построение паспорта по игроку

def build_player_passport(player_name, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        SELECT match_id, finished_ts, p1_sets, p2_sets, winner_id, duration_sec, match_intensity, progress, comeback, table_label
        FROM results NATURAL JOIN match_results
        WHERE player1=? OR player2=?
        ORDER BY finished_ts DESC
    """, (player_name, player_name))
    rows = c.fetchall()
    if not rows:
        conn.close()
        return None

    total_matches = len(rows)
    win_matches = 0
    night_matches = 0
    night_wins = 0
    fatigue_matches = 0
    fatigue_wins = 0
    set_surrender = 0
    comeback_cnt = 0
    volatility_scores = []
    table_wins = {t:0 for t in TABLES}
    table_games = {t:0 for t in TABLES}
    last10 = []
    last5 = []
    winseries = []
    finals_appear = 0

    for idx, (mid, ts, p1s, p2s, winner_id, duration_sec, match_intensity, progress, comeback, table) in enumerate(rows):
        is_p1 = False
        is_win = False
        if c.execute("SELECT player1 FROM results WHERE match_id=?", (mid,)).fetchone()[0] == player_name:
            is_p1 = True
            is_win = (p1s > p2s)
        else:
            is_win = (p2s > p1s)
        win_matches += is_win
        if is_night(ts):
            night_matches += 1
            if is_win:
                night_wins += 1
        if duration_sec and (duration_sec > 1200 or (match_intensity and match_intensity > FATIGUE_THRESHOLD)):
            fatigue_matches += 1
            if is_win:
                fatigue_wins += 1
        # Set surrender
        try:
            c.execute("SELECT p1_pts, p2_pts FROM set_scores WHERE match_id=?", (mid,))
            sets = c.fetchall()
            if is_p1:
                for s1,s2 in sets:
                    if s1<=1 and s2>=11:
                        set_surrender += 1
            else:
                for s1,s2 in sets:
                    if s2<=1 and s1>=11:
                        set_surrender += 1
        except:
            pass
        if comeback:
            comeback_cnt += 1
        try:
            diff = [abs(s1-s2) for s1,s2 in sets]
            volatility_scores.append(np.std(diff) if diff else 0)
        except:
            pass
        if table in TABLES:
            table_games[table] += 1
            if is_win:
                table_wins[table] += 1
        if idx<10:
            last10.append(is_win)
        if idx<5:
            last5.append(is_win)
        winseries.append(is_win)

    current_streak = streak_calc(winseries)
    tournament_streak = 0
    passport = {
        "player_name": player_name,
        "total_matches": total_matches,
        "winrate": round(win_matches/total_matches,3),
        "form_winrate_5": round(sum(last5)/len(last5),3) if last5 else 0,
        "form_winrate_10": round(sum(last10)/len(last10),3) if last10 else 0,
        "night_winrate": round(night_wins/night_matches,3) if night_matches else 0,
        "fatigue_winrate": round(fatigue_wins/fatigue_matches,3) if fatigue_matches else 0,
        "set_surrender_rate": round(set_surrender/total_matches,3),
        "comeback_rate": round(comeback_cnt/total_matches,3),
        "volatility_index": round(np.mean(volatility_scores),3) if volatility_scores else 0,
        "table_winrates": {k: round(table_wins[k]/table_games[k],3) if table_games[k] else None for k in TABLES},
        "current_streak": current_streak,
        "tournament_streak": tournament_streak,
        "finals_appear": finals_appear,
        "last_updated": datetime.now().isoformat(timespec='seconds')
    }
    conn.close()
    return passport

# Сохраняем паспорта игроков в БД

def save_passports_to_db(all_passports, db_path=DB_PATH):
    ensure_passport_table(db_path)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    for passport in all_passports:
        c.execute("""
            INSERT OR REPLACE INTO player_passports (player_name, json_blob, last_updated)
            VALUES (?, ?, ?)
        """, (
            passport['player_name'],
            json.dumps(passport, ensure_ascii=False),
            passport['last_updated']
        ))
    conn.commit()
    conn.close()

# Экспортируем все паспорта игроков в .db

def export_all_passports_to_db(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    players = set()
    c.execute("SELECT DISTINCT player1 FROM results")
    players.update([row[0] for row in c.fetchall() if row[0]])
    c.execute("SELECT DISTINCT player2 FROM results")
    players.update([row[0] for row in c.fetchall() if row[0]])
    conn.close()
    print(f"Найдено {len(players)} уникальных игроков.")
    all_passports = []
    for pname in sorted(players):
        passport = build_player_passport(pname, db_path)
        if passport:
            all_passports.append(passport)
            print(f"✅ Паспорт заполнен: {pname} (матчей: {passport['total_matches']})")
        else:
            print(f"⛔ Нет данных: {pname}")
    save_passports_to_db(all_passports, db_path)
    print(f"Всё готово! Паспортов сохранено: {len(all_passports)} в базе данных {db_path} (таблица player_passports)")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        export_all_passports_to_db()
    else:
        pname = sys.argv[1]
        passport = build_player_passport(pname)
        if not passport:
            print('Нет данных по игроку!')
        else:
            # Пишем только одного игрока
            save_passports_to_db([passport])
            print(f"Паспорт игрока {pname} обновлён в базе!")
