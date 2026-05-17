import sys
sys.path.insert(0, 'WatchMonitor')
from core.paths import DB_PATH
import sqlite3
import os

print(f'DB_PATH: {DB_PATH}')
if os.path.exists(DB_PATH):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    print('Tables:', tables)

    if 'technical_indicators' in tables:
        cursor.execute('PRAGMA table_info(technical_indicators)')
        columns = [row[1] for row in cursor.fetchall()]
        print('Current columns:', columns)

        if 'obv' not in columns:
            cursor.execute('ALTER TABLE technical_indicators ADD COLUMN obv REAL')
            conn.commit()
            print('Column obv added successfully')
        else:
            print('Column obv already exists')
    conn.close()
else:
    print('DB not found')