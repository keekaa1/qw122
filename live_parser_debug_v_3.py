#!/usr/bin/env python3
"""BetCity ▹ LIVE Parser v4.5 | Liga Pro Men
============================================================
• Полная схема: `live_matches`, `live_market_odds`, `live_history`.
• WAL‑режим + 3 сек `busy_timeout` — параллельный доступ.
• CLI:
  ```bash
  python live_parser_debug_v_3.py --create-tables        # инициализация схемы
  python live_parser_debug_v_3.py --fetch-once --verbose # одиночный тик
  python live_parser_debug_v_3.py --live-poll --poll-interval 2 --verbose
  ```
• Verbose‑лог: `[DB] 4 matches ▸ 96 odds @ 1750664020`.
"""
from __future__ import annotations
import argparse, asyncio, os, re, sqlite3, time
from pathlib import Path
from typing import Dict, Any, Tuple
import aiohttp

# --------------------------------------------------------------------
ROOT = Path(os.getcwd())
DB   = ROOT / "betcity_results.db"
URL  = "https://ad.betcity.ru/d/on_air/bets?rev=8&add=dep_event&ver=39&csn=ooca9s"
HEAD = {"User-Agent": "LiveParser/4.5"}
POLL_DEFAULT = 2  # seconds
TABLES = {"A3", "A4", "A5", "A6", "A9"}
TOURN  = re.compile(r"Лига\s+Про\.\s*Мужчины", re.I)
MARKETS = {69, 71, 72, 112, 882, 122, 126, 84}
VERBOSE = False

# --------------------------------------------------------------------
# SQLite helpers

def conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB)
    c.execute("PRAGMA journal_mode=WAL")     # enable WAL for parallel read
    c.execute("PRAGMA busy_timeout = 3000")  # wait up to 3 s if locked
    return c

DDL = """
CREATE TABLE IF NOT EXISTS live_matches(
  match_id        INTEGER PRIMARY KEY,
  updated_at      INTEGER,
  table_id        TEXT,
  start_ts        INTEGER,
  current_score   TEXT,
  set_score       TEXT,
  kf_P1           REAL,
  kf_P2           REAL,
  handicap_line   REAL,
  kf_F1           REAL,
  kf_F2           REAL,
  total_line      REAL,
  kf_Tm           REAL,
  kf_Tb           REAL,
  next_kf_P1      REAL,
  next_kf_P2      REAL,
  is_live         INTEGER,
  name_P1         TEXT,
  name_P2         TEXT
);
CREATE TABLE IF NOT EXISTS live_market_odds(
  match_id   INTEGER,
  updated_at INTEGER,
  market_id  INTEGER,
  line_value REAL,
  side       TEXT,
  kf         REAL,
  marg       REAL,
  maximum    REAL,
  PRIMARY KEY(match_id, updated_at, market_id, side)
);
CREATE TABLE IF NOT EXISTS live_history AS SELECT * FROM live_market_odds WHERE 0;
"""

MATCH_COLS = [
    "match_id", "updated_at", "table_id", "start_ts", "current_score", "set_score",
    "kf_P1", "kf_P2", "handicap_line", "kf_F1", "kf_F2",
    "total_line", "kf_Tm", "kf_Tb", "next_kf_P1", "next_kf_P2",
    "is_live", "name_P1", "name_P2"
]

def ensure_schema() -> None:
    with conn() as c:
        c.executescript(DDL)
        existing = {row[1] for row in c.execute("PRAGMA table_info(live_matches)")}
        for col in MATCH_COLS:
            if col not in existing:
                c.execute(f"ALTER TABLE live_matches ADD COLUMN {col} TEXT")
        c.commit()

# --------------------------------------------------------------------
# Parsing helpers

def extract_lv(odds: Dict[str, Any], blk_name: str, m_id: int) -> float:
    sample = next((v for v in odds.values() if isinstance(v, dict)), {})
    lv = sample.get("lv") or sample.get("lvt") or sample.get("lvl")
    if lv is None and m_id in {71, 72, 112, 122, 126, 84}:
        if m := re.search(r"([-+]?[0-9]+(?:[.,][0-9]+)?)", blk_name):
            lv = float(m.group(1).replace(',', '.'))
    return lv or 0.0

async def fetch_json(session: aiohttp.ClientSession) -> Dict[str, Any]:
    if VERBOSE:
        print('[NET]', URL)
    async with session.get(URL, headers=HEAD, timeout=20) as resp:
        resp.raise_for_status()
        return await resp.json()

# --------------------------------------------------------------------
# Core write

async def write_tick(feed: Dict[str, Any]) -> Tuple[int, int]:
    ts = int(time.time())
    match_cnt = odds_cnt = 0
    with conn() as c:
        cur = c.cursor()
        for sp in feed.get('reply', feed).get('sports', {}).values():
            if str(sp.get('id_sp')) != '46':
                continue
            for ch in sp.get('chmps', {}).values():
                if not TOURN.search(ch.get('name_ch', '')):
                    continue
                table_tag = re.search(r"Стол\s+([A-ZА-Я]\d+)", ch.get('name_ch', ''))
                table_id = table_tag.group(1).replace('А', 'A') if table_tag else None
                if table_id not in TABLES:
                    continue
                for ev in ch.get('evts', {}).values():
                    match_cnt += 1
                    mid = ev['id_ev']
                    main = ev.get('main', {})
                    row = {col: None for col in MATCH_COLS}
                    row.update(
                        match_id=mid,
                        updated_at=ts,
                        table_id=table_id,
                        start_ts=ev.get('date_ev', 0),
                        current_score=ev.get('sc_ev') or ev.get('cur_score', ''),
                        set_score=ev.get('sc_ext_ev', ''),
                        is_live=1,
                        name_P1=ev.get('name_ht', ''),
                        name_P2=ev.get('name_at', '')
                    )
                    wm = (main.get('69', {}).get('data', {}).get(str(mid), {})
                          .get('blocks', {}).get('Wm', {}))
                    row['kf_P1'] = wm.get('P1', {}).get('kf')
                    row['kf_P2'] = wm.get('P2', {}).get('kf')
                    fb = (main.get('71', {}).get('data', {}).get(str(mid), {}).get('blocks', {}))
                    if fb:
                        blk = next(iter(fb.values()))
                        row['handicap_line'] = extract_lv(blk, '', 71)
                        row['kf_F1'] = blk.get('KF_F1', {}).get('kf')
                        row['kf_F2'] = blk.get('KF_F2', {}).get('kf')
                    tb_src = main.get('72') or main.get('112') or {}
                    tb = (tb_src.get('data', {}).get(str(mid), {}).get('blocks', {}))
                    if tb:
                        blk = next(iter(tb.values()))
                        row['total_line'] = extract_lv(blk, '', 72)
                        row['kf_Tm'] = blk.get('Tm', {}).get('kf')
                        row['kf_Tb'] = blk.get('Tb', {}).get('kf')
                    np_blk = (main.get('882', {}).get('data', {}).get(str(mid), {}).get('blocks', {}))
                    if np_blk:
                        blk = next(iter(np_blk.values()))
                        row['next_kf_P1'] = blk.get('P1', {}).get('kf')
                        row['next_kf_P2'] = blk.get('P2', {}).get('kf')
                    col_list = ','.join(row.keys())
                    placeholders = ','.join('?' for _ in row)
                    cur.execute(f"INSERT OR REPLACE INTO live_matches({col_list}) VALUES({placeholders})", tuple(row.values()))

                    for m_id_str, mkt in main.items():
                        try:
                            m_id = int(m_id_str)
                        except ValueError:
                            continue
                        if m_id not in MARKETS:
                            continue
                        blocks = (mkt.get('data', {}).get(str(mid), {}).get('blocks', {}))
                        for blk_name, odds in blocks.items():
                            if not isinstance(odds, dict):
                                continue
                            lv = extract_lv(odds, blk_name, m_id)
                            for side, v in odds.items():
                                if not isinstance(v, dict):
                                    continue
                                odds_cnt += 1
                                row_odds = (
                                    mid, ts, m_id, lv, side,
                                    v.get('kf'), v.get('marg'), v.get('mx')
                                )
                                cur.execute(
                                    "INSERT OR REPLACE INTO live_market_odds VALUES(?,?,?,?,?,?,?,?)", row_odds)
                                cur.execute(
                                    "INSERT INTO live_history VALUES(?,?,?,?,?,?,?,?)", row_odds)

        cur.execute("UPDATE live_matches SET is_live=0 WHERE updated_at < ?", (ts,))
        cur.execute("DELETE FROM live_matches WHERE is_live=0 AND (? - updated_at) > 600", (ts,))

    if VERBOSE:
        print(f"[DB] {match_cnt} matches ▸ {odds_cnt} odds @ {ts}")
    return match_cnt, odds_cnt

# --------------------------------------------------------------------
# wrappers

async def once():
    async with aiohttp.ClientSession() as s:
        feed = await fetch_json(s)
        await write_tick(feed)

async def loop(poll: int):
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                await write_tick(await fetch_json(s))
            except Exception as e:
                print("[ERR]", e)
            await asyncio.sleep(poll)

def main():
    global VERBOSE
    ap = argparse.ArgumentParser()
    ap.add_argument("--create-tables", action="store_true")
    ap.add_argument("--fetch-once",   action="store_true")
    ap.add_argument("--live-poll",    action="store_true")
    ap.add_argument("--poll-interval", type=int, default=POLL_DEFAULT)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    VERBOSE = args.verbose
    ensure_schema()

    if args.create_tables:
        print("[INIT] Schema created or patched."); return
    if args.fetch_once:
        asyncio.run(once()); return
    if args.live_poll:
        asyncio.run(loop(args.poll_interval)); return

if __name__ == "__main__":
    main()
