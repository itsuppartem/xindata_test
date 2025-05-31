import aiosqlite
import pandas as pd
from pathlib import Path
import logging
from typing import Any, Optional, List
from .config import DB_PATH, CSV_PATH
import re

TABLE_NAME = 'freelancer_earnings'
MAX_ROWS_PREVIEW = 100

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def _get_type_map() -> dict:
    return {'int64': 'INTEGER', 'float64': 'REAL', 'object': 'TEXT', 'bool': 'INTEGER', }


async def init_db(db_path=DB_PATH, csv_path=CSV_PATH) -> None:
    """
    Инициализация базы данных и создание таблицы freelancer_earnings на основе CSV.
    """
    try:
        df = pd.read_csv(csv_path, nrows=MAX_ROWS_PREVIEW)
    except Exception as e:
        logging.exception('Ошибка чтения CSV для инициализации БД')
        raise
    type_map = _get_type_map()
    columns = []
    pk_set = False
    for col, dtype in df.dtypes.items():
        sql_type = type_map.get(str(dtype), 'TEXT')
        if not pk_set and re.search(r'id$', col, re.IGNORECASE):
            columns.append(f'{col} {sql_type} PRIMARY KEY')
            pk_set = True
        else:
            columns.append(f'{col} {sql_type}')
    columns_sql = ',\n    '.join(columns)
    create_sql = f'''CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n    {columns_sql}\n)'''
    try:
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(create_sql)
            await conn.commit()
        logging.info(f'База данных и таблица {TABLE_NAME} инициализированы.')
    except Exception as e:
        logging.exception('Ошибка создания таблицы в БД')
        raise


async def import_csv(db_path=DB_PATH, csv_path=CSV_PATH) -> None:
    """
    Импорт данных из CSV в таблицу freelancer_earnings.
    """
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logging.exception('Ошибка чтения CSV для импорта')
        raise
    try:
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(f'DELETE FROM {TABLE_NAME}')
            await conn.commit()
            for _, row in df.iterrows():
                await conn.execute(f'''INSERT INTO {TABLE_NAME} VALUES ({', '.join(['?'] * len(row))})''', tuple(row))
            await conn.commit()
        logging.info(f'Данные из CSV импортированы в таблицу {TABLE_NAME}.')
    except Exception as e:
        logging.exception('Ошибка импорта данных в БД')
        raise


async def query_db(query: str, params: Optional[tuple] = None, db_path=DB_PATH) -> List[Any]:
    """
    Выполнение SQL-запроса к базе данных.

    Args:
        query (str): SQL-запрос.
        params (Optional[tuple]): Параметры для запроса.
        db_path: путь к базе данных.

    Returns:
        List[Any]: Результаты запроса.
    """
    try:
        async with aiosqlite.connect(db_path) as conn:
            async with conn.execute(query, params or ()) as cursor:
                result = await cursor.fetchall()
        logging.info('SQL-запрос выполнен успешно.')
        return result
    except aiosqlite.DatabaseError as e:
        logging.error(f'Ошибка выполнения SQL-запроса: {e}')
        raise
    except Exception as e:
        logging.exception('Неизвестная ошибка при выполнении SQL-запроса')
        raise


async def get_table_schema_str(table_name: str = TABLE_NAME) -> str:
    """
    Возвращает строку-описание схемы таблицы для LLM.

    Args:
        table_name (str): Имя таблицы.

    Returns:
        str: Описание схемы таблицы.
    """
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            async with conn.execute(f'PRAGMA table_info({table_name})') as cursor:
                columns = await cursor.fetchall()
        schema_lines = [f'- {col[1]}: {col[2]}' for col in columns]
        return f'Таблица {table_name} содержит следующие поля:\n' + '\n'.join(schema_lines)
    except Exception as e:
        logging.exception('Ошибка получения схемы таблицы')
        raise


async def validate_sql(sql: str, db_path=DB_PATH) -> bool:
    """
    Проверяет синтаксис SQL-запроса через EXPLAIN и запрещает опасные запросы (разрешён только SELECT, только одно выражение).

    Args:
        sql (str): SQL-запрос.
        db_path: путь к базе данных.
    Returns:
        bool: True если синтаксис корректен и запрос безопасен, иначе выбрасывает исключение.
    """
    sql_stripped = sql.strip().lower()
    if not sql_stripped.startswith('select'):
        logging.error(f'Попытка выполнить опасный или неразрешённый SQL: {sql}')
        raise ValueError('Разрешены только SELECT-запросы!')
    if ';' in sql_stripped[:-1]:
        logging.error(f'Попытка выполнить множественные SQL-выражения: {sql}')
        raise ValueError('Разрешено только одно SQL-выражение!')
    try:
        explain_sql = f'EXPLAIN {sql}'
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute(explain_sql)
        return True
    except aiosqlite.DatabaseError as e:
        logging.error(f'Некорректный SQL: {e}')
        raise
    except Exception as e:
        logging.exception('Ошибка при валидации SQL')
        raise
