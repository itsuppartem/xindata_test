import pytest
import pytest_asyncio
from src.db import init_db, import_csv, query_db, validate_sql
from src.config import TEST_DB_PATH, TEST_CSV_PATH
import os


@pytest_asyncio.fixture(scope='module', autouse=True)
async def setup_test_db():
    # Инициализация тестовой БД
    await init_db(db_path=TEST_DB_PATH, csv_path=TEST_CSV_PATH)
    await import_csv(db_path=TEST_DB_PATH, csv_path=TEST_CSV_PATH)
    yield
    # После тестов можно удалить тестовую БД
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)


@pytest.mark.asyncio
async def test_validate_sql_select():
    sql = 'SELECT * FROM freelancer_earnings'
    assert await validate_sql(sql, db_path=TEST_DB_PATH)


@pytest.mark.asyncio
async def test_validate_sql_dangerous():
    sql = 'DROP TABLE freelancer_earnings'
    with pytest.raises(ValueError):
        await validate_sql(sql, db_path=TEST_DB_PATH)


@pytest.mark.asyncio
async def test_validate_sql_invalid():
    sql = 'SELECT * FROM not_existing_table'
    with pytest.raises(Exception):
        await validate_sql(sql, db_path=TEST_DB_PATH)


@pytest.mark.asyncio
async def test_query_db():
    sql = 'SELECT Job_Category, Earnings_USD FROM freelancer_earnings WHERE Freelancer_ID=4'
    result = await query_db(sql, db_path=TEST_DB_PATH)
    assert result == [('Data Entry', 5577)]


@pytest.mark.asyncio
async def test_validate_sql_empty():
    sql = ''
    with pytest.raises(ValueError):
        await validate_sql(sql, db_path=TEST_DB_PATH)


@pytest.mark.asyncio
async def test_validate_sql_injection():
    sql = "SELECT * FROM freelancer_earnings; DROP TABLE freelancer_earnings"
    with pytest.raises(ValueError):
        await validate_sql(sql, db_path=TEST_DB_PATH)


@pytest.mark.asyncio
async def test_validate_sql_subselect_dml():
    sql = "SELECT (SELECT 1 FROM freelancer_earnings); DELETE FROM freelancer_earnings"
    with pytest.raises(ValueError):
        await validate_sql(sql, db_path=TEST_DB_PATH)


@pytest.mark.asyncio
async def test_query_db_nonexistent_field():
    sql = 'SELECT not_a_field FROM freelancer_earnings'
    with pytest.raises(Exception):
        await query_db(sql, db_path=TEST_DB_PATH)


@pytest.mark.asyncio
async def test_query_db_large_number():
    sql = 'SELECT Earnings_USD FROM freelancer_earnings WHERE Earnings_USD > 6000'
    result = await query_db(sql, db_path=TEST_DB_PATH)
    # В тестовом датасете таких строк 8
    assert len(result) == 8


@pytest.mark.asyncio
async def test_llm_integration():
    class DummyLLM:
        async def generate_sql(self, question):
            return 'SELECT Job_Category FROM freelancer_earnings WHERE Freelancer_ID=5'

        async def detect_intent(self, question):
            return 'sql'

    llm = DummyLLM()
    question = 'Категория работы для фрилансера с id=5?'
    sql = await llm.generate_sql(question)
    assert sql.startswith('SELECT')
    result = await query_db(sql, db_path=TEST_DB_PATH)
    assert result == [('Digital Marketing',)]
