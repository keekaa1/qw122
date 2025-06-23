# db_utils.py
import sqlite3
import os

# Путь к БД, можно переопределить через переменную окружения
DB_PATH = os.getenv("BETCITY_DB_PATH", "betcity_results.db")

def get_db_conn() -> sqlite3.Connection:
    """
    Открывает sqlite-соединение в режиме WAL с увеличенным таймаутом
    и разрешением для использования в разных потоках.
    """
    conn = sqlite3.connect(
        DB_PATH,
        timeout=5.0,            # ждать до 5 секунд, если БД заблокирована
        check_same_thread=False # позволяет использовать соединение в разных async/thread
    )
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


# ПАТЧ: live_parser_debug_v_3.py
# 1) Вверху: from db_utils import get_db_conn
# 2) Удалить старую def conn(), заменить на:

def conn() -> sqlite3.Connection:
    """Переадресация на общий утилитарный коннект."""
    return get_db_conn()

# 3) Везде вместо `with conn() as c:` использовать:
#    conn = get_db_conn()
#    try:
#        cur = conn.cursor()
#        # ... операции INSERT/UPDATE
#        conn.commit()
#    finally:
#        conn.close()


# ПАТЧ: line_parser_debug_v2.py
# 1) Вверху: from db_utils import get_db_conn
# 2) Заменить старую conn():

def conn() -> sqlite3.Connection:
    """Переадресация на общий утилитарный коннект."""
    return get_db_conn()

# 3) В функции process() и других местах
#    conn = get_db_conn()
#    try:
#        cur = conn.cursor()
#        # ... SQL операции
#        conn.commit()
#    finally:
#        conn.close()


# ПАТЧ: betcity_results_parser_all_in_one_rolling.py
# 1) Вверху: from db_utils import get_db_conn
# 2) В create_tables():

#    conn = get_db_conn()
#    try:
#        c = conn.cursor()
#        # ... создание таблиц
#        conn.commit()
#    finally:
#        conn.close()

# 3) Аналогично для всех sqlite3.connect -> get_db_conn() + try/finally


# ПАТЧ: все остальные ETL-скрипты (player_*.py и т.д.)
# Заменить:
#    conn = sqlite3.connect(DB_PATH)
# на:
#    from db_utils import get_db_conn
#    conn = get_db_conn()
# и обернуть чтение/запись в try/finally:
#    try:
#        # df = pd.read_sql_query(...) или SQL операции
#    finally:
#        conn.close()
