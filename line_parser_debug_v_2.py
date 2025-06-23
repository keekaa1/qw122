#!/usr/bin/env python3
"""BetCity ▸ LINE Parser  ▸ Liga Pro Men
================================================
Собирает прематч‑линию по турниру «Россия. Лига Про. Мужчины»
и пишет в SQLite **betcity_results.db**:
  • line_matches          – мета‑инфо о матче
  • line_markets          – последний JSON‑снимок рынка
  • line_market_odds      – актуальные коэффициенты (плоско)
  • line_markets_history  – журнал изменений коэффициентов
  • results_odds          – финальные коэффициенты, когда матч finished

CLI
---
  --create-tables   пересоздать schema
  --fetch-once      один опрос
  --live-poll       цикличный опрос (по умолчанию 15 сек)
"""
from __future__ import annotations
import argparse, asyncio, json, os, re, sqlite3, sys, time
from pathlib import Path
from typing import Any, Dict, List
import aiohttp

ROOT   = Path(sys.argv[0]).resolve().parent
DB     = ROOT / "betcity_results.db"
API    = os.getenv('LINE_SOURCE_URL', 'https://ad.betcity.ru/d/off/events')
REV, VER, CSN = os.getenv('LINE_REV', '6'), os.getenv('LINE_VER', '39'), os.getenv('LINE_CSN', 'ooca9s')
POLL   = int(os.getenv('POLL_SEC', '15'))
TABLES = {'A3', 'A4', 'A5', 'A6', 'A9'}
TOURN  = re.compile(r'Лига\s+Про\.\s*Мужчины', re.I)
HEAD   = {'User-Agent': 'LineParser/2.1'}
MARKETS = {69, 71, 72}
VERBOSE = False

DDL = """
CREATE TABLE IF NOT EXISTS line_matches(
  match_id   INTEGER PRIMARY KEY,
  tournament TEXT,
  table_id   TEXT,
  start_ts   INTEGER,
  is_live    INTEGER,
  status     TEXT,
  updated_at INTEGER
);
CREATE TABLE IF NOT EXISTS line_markets(
  match_id   INTEGER,
  market_id  INTEGER,
  line_value REAL,
  coeff_json TEXT,
  updated_at INTEGER,
  PRIMARY KEY(match_id, market_id, line_value)
);
CREATE TABLE IF NOT EXISTS line_market_odds(
  match_id   INTEGER,
  market_id  INTEGER,
  line_value REAL,
  side       TEXT,
  kf         REAL,
  marg       REAL,
  maximum    REAL,
  updated_at INTEGER,
  PRIMARY KEY(match_id, market_id, line_value, side)
);
CREATE TABLE IF NOT EXISTS line_markets_history(
  match_id   INTEGER,
  market_id  INTEGER,
  line_value REAL,
  side       TEXT,
  kf         REAL,
  marg       REAL,
  maximum    REAL,
  updated_at INTEGER
);
CREATE TABLE IF NOT EXISTS results_odds(
  match_id   INTEGER,
  market_id  INTEGER,
  line_value REAL,
  side       TEXT,
  kf         REAL,
  PRIMARY KEY(match_id, market_id, line_value, side)
);
"""

def conn(): return sqlite3.connect(DB)

def create_tables() -> None:
    c = conn(); c.executescript(DDL); c.commit(); c.close()

async def fetch_json(session: aiohttp.ClientSession) -> Dict[str, Any]:
    url = f"{API}?rev={REV}&ver={VER}&csn={CSN}"
    if VERBOSE:
        print('[NET]', url)
    async with session.get(url, headers=HEAD, timeout=30) as r:
        r.raise_for_status(); return await r.json()

_table = lambda name: (m.group(1).replace('А', 'A') if (m := re.search(r'Стол\s+([A-ZА-Я]\d+)', name)) else None)

def collect_events(feed: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for sp in (feed.get('reply', feed).get('sports', {}) or {}).values():
        if str(sp.get('id_sp')) != '46':
            continue
        for ch in (sp.get('chmps', {}) or {}).values():
            n = ch.get('name_ch', '')
            if not TOURN.search(n):
                continue
            tbl = _table(n)
            if tbl not in TABLES:
                continue
            if VERBOSE:
                print('[PASS]', n)
            for ev in (ch.get('evts', {}) or {}).values():
                ev.update({'_tournament': n, '_table': tbl}); out.append(ev)
    if VERBOSE:
        print('[PARSE]', len(out))
    return out

def process(events: List[Dict[str, Any]]) -> None:
    ts = int(time.time())
    con = conn(); cur = con.cursor(); seen: set[int] = set()

    for ev in events:
        mid = ev['id_ev']; seen.add(mid)
        cur.execute("INSERT OR REPLACE INTO line_matches VALUES(?,?,?,?,?,?,?)",
                    (mid, ev['_tournament'], ev['_table'], ev.get('date_ev', 0), 1, 'open', ts))

        match_p1 = ev.get('main', {}).get("69", {}).get('data', {}).get(str(mid), {}).get('blocks', {}).get('Wm', {}).get('P1', {}).get('kf')
        match_p2 = ev.get('main', {}).get("69", {}).get('data', {}).get(str(mid), {}).get('blocks', {}).get('Wm', {}).get('P2', {}).get('kf')

        for m_id_str, mkt in (ev.get('main', {}) or {}).items():
            try:
                m_id = int(m_id_str)
            except ValueError:
                continue
            if m_id not in MARKETS:
                continue

            data = mkt.get('data', {}).get(str(mid), {})
            for blk_name, odds in (data.get('blocks', {}) or {}).items():
                if not isinstance(odds, dict):
                    continue

                sample = next((v for v in odds.values() if isinstance(v, dict)), {})
                base_lv = sample.get('lv') or sample.get('lvt') or sample.get('lvl')
                if base_lv is None and m_id in (71, 72):
                    mm = re.search(r'([-+]?[\d.,]+)', blk_name)
                    if mm:
                        base_lv = float(mm.group(1).replace(',', '.'))
                if base_lv is None:
                    base_lv = 0.0

                cur.execute("INSERT OR REPLACE INTO line_markets VALUES(?,?,?,?,?)",
                            (mid, m_id, base_lv, json.dumps(odds, separators=(',', ':'), ensure_ascii=False), ts))

                for side, v in odds.items():
                    if not isinstance(v, dict):
                        continue

                    row_lv = v.get('lv') or v.get('lvt') or v.get('lvl')
                    if row_lv is None:
                        if m_id == 71 and match_p1 and match_p2:
                            # Фора: вычисляем знак на основе сравнений
                            if match_p1 < match_p2:
                                row_lv = -abs(base_lv) if side.startswith('KF_F1') else abs(base_lv)
                            elif match_p1 > match_p2:
                                row_lv = abs(base_lv) if side.startswith('KF_F1') else -abs(base_lv)
                            else:
                                row_lv = -abs(base_lv) if side.startswith('KF_F1') else abs(base_lv)
                        else:
                            row_lv = base_lv

                    row = (mid, m_id, row_lv, side, v.get('kf'), v.get('marg'), v.get('maximum'), ts)
                    cur.execute("INSERT OR REPLACE INTO line_market_odds VALUES(?,?,?,?,?,?,?,?)", row)
                    cur.execute("INSERT INTO line_markets_history VALUES(?,?,?,?,?,?,?,?)", row)

    cur.execute(
        ("UPDATE line_matches SET is_live=0,status='finished',updated_at=? WHERE is_live=1 AND match_id NOT IN ("
         + ','.join('?' * len(seen)) + ")") if seen else
        "UPDATE line_matches SET is_live=0,status='finished',updated_at=? WHERE is_live=1",
        (ts, *seen),
    )

    cur.execute(
        """
        INSERT OR IGNORE INTO results_odds(match_id,market_id,line_value,side,kf)
        SELECT o.match_id,o.market_id,o.line_value,o.side,o.kf
        FROM   line_market_odds o JOIN line_matches m USING(match_id)
        WHERE  m.status='finished'
        """
    )

    con.commit(); con.close()
    if VERBOSE:
        print(f"[DB] commit {len(events)} ev, finished {cur.rowcount}")
