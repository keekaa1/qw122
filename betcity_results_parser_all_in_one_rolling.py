#!/usr/bin/env python3
import os
import json
import argparse
import asyncio
import aiohttp
import sqlite3
import sys
import re
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

DB_PATH = "betcity_results.db"
RESULTS_URL = os.getenv('RESULTS_SOURCE_URL', 'https://ad.betcity.ru/d/score')
REV = os.getenv('RESULTS_REV', '5')
VER = os.getenv('RESULTS_VER', '39')
CSN = os.getenv('RESULTS_CSN', 'ooca9s')
ROLLING_MONTHS = 12  # Сколько месяцев хранить

# --- Таблицы ---
def create_tables(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS results (
            match_id INTEGER PRIMARY KEY,
            table_label TEXT,
            player1 TEXT,
            player2 TEXT,
            sc_ev TEXT,
            sc_ext_ev TEXT,
            finished TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS match_results (
            match_id INTEGER PRIMARY KEY,
            finished_ts TEXT,
            p1_sets INTEGER,
            p2_sets INTEGER,
            winner_id INTEGER,
            loser_id INTEGER,
            duration_sec INTEGER,
            match_intensity REAL,
            progress TEXT,
            comeback INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS set_scores (
            match_id INTEGER,
            set_no INTEGER,
            p1_pts INTEGER,
            p2_pts INTEGER,
            PRIMARY KEY (match_id, set_no)
        )
    """)
    conn.commit()
    conn.close()
    print("Таблицы results, match_results, set_scores созданы/актуализированы.")

def prune_old_results(db_path=DB_PATH, months=ROLLING_MONTHS):
    """
    Удаляет все матчи старше X месяцев во всех таблицах
    """
    cutoff = (datetime.now() - timedelta(days=months*30)).strftime('%Y-%m-%d')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Сначала найти match_id старых матчей
    c.execute("SELECT match_id FROM results WHERE finished < ?", (cutoff,))
    old_ids = [row[0] for row in c.fetchall()]
    print(f"Удаляем {len(old_ids)} старых матчей до {cutoff}...")
    if old_ids:
        ids_tuple = tuple(old_ids)
        # Удаляем из set_scores, match_results, results
        c.execute(f"DELETE FROM set_scores WHERE match_id IN ({','.join(['?']*len(ids_tuple))})", ids_tuple)
        c.execute(f"DELETE FROM match_results WHERE match_id IN ({','.join(['?']*len(ids_tuple))})", ids_tuple)
        c.execute(f"DELETE FROM results WHERE match_id IN ({','.join(['?']*len(ids_tuple))})", ids_tuple)
        conn.commit()
    conn.close()

# --- Фильтр только нужных столов ---
NEEDED_TABLES = {"A3", "A4", "A5", "A6", "A9"}

# --- Загрузка результатов по дате ---
def build_url(date_str: str) -> str:
    params = {'rev': REV, 'date': date_str, 'ver': VER, 'csn': CSN}
    query = '&'.join(f"{k}={v}" for k, v in params.items())
    return f"{RESULTS_URL}?{query}"

async def fetch_json(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=30) as resp:
            resp.raise_for_status()
            return await resp.json()

def parse_results(feed: dict) -> List[dict]:
    events = []
    reply = feed.get('reply', feed)
    sports = reply.get('sports', {})
    items = sports.items() if isinstance(sports, dict) else ((sp.get('id_sp'), sp) for sp in sports)
    for sp_id, sp in items:
        try:
            if int(sp_id) != 46:
                continue
        except Exception:
            continue
        chmps = sp.get('chmps', {})
        ch_list = chmps.values() if isinstance(chmps, dict) else chmps
        for ch in ch_list:
            name_ch = ch.get('name_ch', '')
            m = re.search(r"Стол\s+([A-ZА-Я]\d+)", name_ch)
            if not m:
                continue
            table = m.group(1).replace('А', 'A')
            if table not in NEEDED_TABLES:
                continue
            evts = ch.get('evts', {})
            evt_list = evts.values() if isinstance(evts, dict) else evts
            for ev in evt_list:
                sc = ev.get('sc_ev')
                if not sc or ':' not in sc:
                    continue
                events.append({
                    'match_id': ev.get('id_ev'),
                    'table_label': table,
                    'player1': ev.get('name_ht'),
                    'player2': ev.get('name_at'),
                    'sc_ev': sc,
                    'sc_ext_ev': ev.get('sc_ext_ev'),
                    'finished': ev.get('finished') or ev.get('date_ev')
                })
    return events

def save_to_db(events, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    create_tables(db_path)
    c = conn.cursor()
    for e in events:
        try:
            c.execute("""
                INSERT INTO results (match_id, table_label, player1, player2, sc_ev, sc_ext_ev, finished)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(match_id) DO UPDATE SET
                    table_label=excluded.table_label,
                    player1=excluded.player1,
                    player2=excluded.player2,
                    sc_ev=excluded.sc_ev,
                    sc_ext_ev=excluded.sc_ext_ev,
                    finished=excluded.finished
            """, (
                e['match_id'],
                e['table_label'],
                e['player1'],
                e['player2'],
                e['sc_ev'],
                e['sc_ext_ev'],
                e['finished']
            ))
        except Exception as ex:
            print("DB error:", ex)
    conn.commit()
    conn.close()

# --- Парсинг sc_ev, sc_ext_ev ---
def parse_sets(sc_ev: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not sc_ev:
        return None, None
    m = re.match(r'^(\d+):(\d+)$', sc_ev.strip())
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))

def parse_set_scores(sc_ext_ev: Optional[str]) -> List[Tuple[int, int]]:
    if not sc_ext_ev:
        return []
    pairs = re.findall(r'(\d+):(\d+)', sc_ext_ev)
    return [(int(a), int(b)) for a, b in pairs][:5]

# --- Progress/comeBack/duration/intensity ---
def calc_progress_and_comeback(set_scores):
    p1, p2 = 0, 0
    progress_steps = [f"{p1}:{p2}"]
    was_comeback = 0
    lead_tracker = []
    for s1, s2 in set_scores:
        if s1 > s2:
            p1 += 1
        else:
            p2 += 1
        progress_steps.append(f"{p1}:{p2}")
        lead_tracker.append((p1, p2))
    winner = 1 if p1 > p2 else 2
    for a, b in lead_tracker:
        if (winner == 1 and a < b) or (winner == 2 and b < a):
            was_comeback = 1
            break
    return ' -> '.join(progress_steps), was_comeback

def calc_duration_and_intensity(set_scores):
    t_rally = 3.8
    t_break = 7.8
    t_set_pause = 32.5
    total_points = sum(a + b for a, b in set_scores)
    sets_played = len(set_scores)
    duration_sec = int(total_points * t_rally + (total_points - sets_played) * t_break + (sets_played - 1) * t_set_pause)
    intensity = min(1, duration_sec / 900) if duration_sec else 0
    return duration_sec, intensity

def upsert_match_results(conn, match_id, finished_ts, p1_sets, p2_sets, winner_id, loser_id, duration_sec, match_intensity, progress, comeback):
    c = conn.cursor()
    c.execute("""
        INSERT INTO match_results
            (match_id, finished_ts, p1_sets, p2_sets, winner_id, loser_id, duration_sec, match_intensity, progress, comeback, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        ON CONFLICT(match_id) DO UPDATE SET
            finished_ts=excluded.finished_ts,
            p1_sets=excluded.p1_sets,
            p2_sets=excluded.p2_sets,
            winner_id=excluded.winner_id,
            loser_id=excluded.loser_id,
            duration_sec=excluded.duration_sec,
            match_intensity=excluded.match_intensity,
            progress=excluded.progress,
            comeback=excluded.comeback,
            updated_at=datetime('now')
    """, (match_id, finished_ts, p1_sets, p2_sets, winner_id, loser_id, duration_sec, match_intensity, progress, comeback))
    conn.commit()

def bulk_insert_set_scores(conn, match_id, set_scores: List[Tuple[int, int]]):
    c = conn.cursor()
    for idx, (p1_pts, p2_pts) in enumerate(set_scores, start=1):
        c.execute("""
            INSERT OR IGNORE INTO set_scores (match_id, set_no, p1_pts, p2_pts)
            VALUES (?, ?, ?, ?)
        """, (match_id, idx, p1_pts, p2_pts))
    conn.commit()

def fill_match_results_and_sets(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    create_tables(db_path)
    c = conn.cursor()
    c.execute("SELECT match_id, finished, sc_ev, sc_ext_ev FROM results")
    all_rows = c.fetchall()
    print(f"Found {len(all_rows)} rows in results.")
    for match_id, finished, sc_ev, sc_ext_ev in all_rows:
        p1_sets, p2_sets = parse_sets(sc_ev)
        set_scores = parse_set_scores(sc_ext_ev)
        if p1_sets is None or p2_sets is None:
            continue
        if not set_scores:
            continue
        winner_id = 1 if p1_sets > p2_sets else 2
        loser_id = 2 if winner_id == 1 else 1
        progress, comeback = calc_progress_and_comeback(set_scores)
        duration_sec, match_intensity = calc_duration_and_intensity(set_scores)
        upsert_match_results(conn, match_id, finished, p1_sets, p2_sets, winner_id, loser_id, duration_sec, match_intensity, progress, comeback)
        bulk_insert_set_scores(conn, match_id, set_scores)
    conn.close()
    print("match_results и set_scores успешно заполнены/обновлены!")

# --- Live polling ---
async def poll_results(interval_sec=60, db_path=DB_PATH):
    print(f"Старт live-режима. Обновление каждые {interval_sec} секунд.")
    while True:
        date_str = datetime.now().strftime("%Y-%m-%d")
        url = build_url(date_str)
        try:
            feed = await fetch_json(url)
            evs = parse_results(feed)
            print(f"[{datetime.now().isoformat(timespec='seconds')}] Найдено {len(evs)} матчей за {date_str}.")
            save_to_db(evs, db_path)
            fill_match_results_and_sets(db_path)
            prune_old_results(db_path)
        except Exception as ex:
            print(f"Ошибка на {date_str}: {ex}")
        print(f"Ожидание {interval_sec} секунд...")
        await asyncio.sleep(interval_sec)

# --- CLI/ENTRYPOINT ---
def run_main():
    parser = argparse.ArgumentParser(description='All-in-One Betcity Results Parser with rolling window')
    parser.add_argument('--live-poll', action='store_true', help='Собирать результаты в live-режиме раз в N секунд')
    parser.add_argument('--poll-interval', type=int, default=60, help='Интервал опроса в секундах (по умолчанию 60)')
    parser.add_argument('--create-tables', action='store_true', help='Создать/обновить структуру всех таблиц')
    parser.add_argument('--from-date', help='Дата начала диапазона YYYY-MM-DD')
    parser.add_argument('--to-date', help='Дата конца диапазона YYYY-MM-DD')
    parser.add_argument('--fill-matches', action='store_true', help='Нормализовать/заполнить match_results, set_scores из results')
    parser.add_argument('--prune', action='store_true', help='Удалить все матчи старше ROLLING_MONTHS')
    parser.add_argument('--db-path', default=DB_PATH, help='Путь к базе данных')
    args = parser.parse_args()

    async def main():
        if args.live_poll:
            await poll_results(args.poll_interval, args.db_path)
            return
        if args.create_tables:
            create_tables(args.db_path)
            return
        if args.from_date and args.to_date:
            start = datetime.strptime(args.from_date, "%Y-%m-%d")
            end = datetime.strptime(args.to_date, "%Y-%m-%d")
            curr = start
            while curr <= end:
                date_str = curr.strftime("%Y-%m-%d")
                url = build_url(date_str)
                print(f"Скачиваем результаты за {date_str}...")
                try:
                    feed = await fetch_json(url)
                    evs = parse_results(feed)
                    print(f"Найдено {len(evs)} матчей.")
                    save_to_db(evs, args.db_path)
                except Exception as ex:
                    print(f"Ошибка на {date_str}: {ex}")
                curr += timedelta(days=1)
            print("Загрузка сырых данных завершена.")
            fill_match_results_and_sets(args.db_path)
            prune_old_results(args.db_path)
            return
        if args.fill_matches:
            fill_match_results_and_sets(args.db_path)
            return
        if args.prune:
            prune_old_results(args.db_path)
            return
        print('--from-date ... --to-date ... для загрузки матчей, --fill-matches для нормализации, --create-tables для структуры, --prune для очистки старых матчей.')

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            print("Event loop уже запущен! Запусти скрипт напрямую, без вложенных event loop.")
        else:
            loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Остановлено пользователем.")

if __name__ == "__main__":
    run_main()
