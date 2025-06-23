"""
run_all_etl.py
-------------
Альтернативный мастер-скрипт для среды, где subprocess не поддерживается (например, в Jupyter/Colab/emscripten).

- Последовательно импортирует и исполняет все модули в текущем процессе.
- Если модуль выдаёт ошибку, цикл прерывается (можно убрать break).
- Требует, чтобы все ETL-скрипты были написаны в виде функций внутри файла или запускались через exec.

Author: GPT-4 + Кирилл
"""

import os
import sys

modules = [
    "player_elo_etl.py",
    "player_fatigue_etl.py",
    "player_style_etl.py",
    "player_resilience_etl.py",
    "player_h2h_etl.py",
    "player_passport_etl.py"
]

for module in modules:
    print(f"\n=== Запуск {module} ===")
    try:
        # Вариант 1: если скрипты пишутся как функции и можно импортировать
        # mod_name = module.replace('.py', '')
        # __import__(mod_name)
        # Вариант 2: универсально, для любых .py-файлов (exec):
        with open(module, encoding="utf-8") as f:
            code = f.read()
            exec(compile(code, module, 'exec'))
    except Exception as e:
        print(f"Ошибка при запуске {module}: {e}")
        break  # если хочешь запускать все несмотря на ошибки, закомментируй break
