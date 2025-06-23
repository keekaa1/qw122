#!/usr/bin/env python3
"""
run_all_etl.py

Обновлённый мастер-скрипт, который запускает все ETL-скрипты и повторяет выполнение каждые 10 минут.
Использует текущий интерпретатор Python (sys.executable) для кроссплатформенной совместимости.
"""
import os
import sys
import subprocess
import time
import logging

# Каталог, где лежат все ETL-скрипты (скрипт находится в том же каталоге)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Список всех ETL-скриптов
SCRIPTS = [
    'player_passport_etl.py',
    'player_h2h_etl.py',
    'player_resilience_etl.py',
    'player_style_etl.py',
    'player_fatigue_etl.py',
    'player_elo_etl.py',
    'player_passport_etl_final_match_results_fix.py',
    'league_reference_etl_ultimate.py',
    'player_table_stats_etl.py',
    'player_sos_windows_etl.py',
]

# Настройка логирования
log_file = os.path.join(BASE_DIR, 'etl_runner.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)


def run_all_etl():
    """Запускает все скрипты и логирует результаты"""
    logging.info('=== Начало пакетного запуска ETL ===')
    for script in SCRIPTS:
        path = os.path.join(BASE_DIR, script)
        logging.info(f'Запуск {script}')
        if not os.path.isfile(path):
            logging.error(f'Не найден файл: {script}')
            continue
        try:
            # Используем тот же интерпретатор, что запустил текущий скрипт
            result = subprocess.run(
                [sys.executable, path],
                cwd=BASE_DIR,
                capture_output=True,
                text=True
            )
            if result.stdout:
                logging.info(f'[STDOUT] {result.stdout.strip()}')
            if result.stderr:
                logging.error(f'[STDERR] {result.stderr.strip()}')
            if result.returncode != 0:
                logging.error(f'{script} завершился с кодом {result.returncode}')
            else:
                logging.info(f'{script} успешно выполнен')
        except Exception as e:
            logging.exception(f'Ошибка при запуске {script}: {e}')
    logging.info('=== Пакетный запуск ETL завершён ===')


if __name__ == '__main__':
    # Первый запуск при старте
    run_all_etl()
    # Повторять каждые 10 минут
    while True:
        logging.info('Ждём 10 минут до следующего запуска...')
        time.sleep(600)
        run_all_etl()
