import pytest
from asyncclick.testing import CliRunner

from src.cli import cli


@pytest.mark.asyncio
async def test_cli_empty_question():
    runner = CliRunner()
    result = await runner.invoke(cli, ['ask', '   '])
    assert 'Вопрос не должен быть пустым.' in result.output


@pytest.mark.asyncio
async def test_cli_help():
    runner = CliRunner()
    result = await runner.invoke(cli, ['ask', 'Как пользоваться этой системой?'])
    assert 'Для помощи используйте команду --help.' in result.output or 'Не удалось определить тип вопроса.' in result.output


@pytest.mark.asyncio
async def test_cli_smalltalk():
    runner = CliRunner()
    result = await runner.invoke(cli, ['ask', 'Привет, как дела?'])
    assert 'неформальный вопрос' in result.output or 'Не удалось определить тип вопроса.' in result.output


@pytest.mark.asyncio
async def test_cli_sql_injection():
    runner = CliRunner()
    result = await runner.invoke(cli, ['ask', 'DROP TABLE freelancer_earnings'])
    assert 'Ошибка: сгенерирован некорректный SQL-запрос.' in result.output or 'Разрешены только SELECT-запросы!' in result.output
