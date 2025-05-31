import asyncclick as click
import logging
from .db import init_db, import_csv, query_db, validate_sql
from .llm import get_llm_client
from typing import Any

EMPTY_QUESTION_MSG = 'Вопрос не должен быть пустым.'
INFORMAL_MSG = 'Это неформальный вопрос, не связанный с аналитикой.'
HELP_MSG = 'Для помощи используйте команду --help.'
UNKNOWN_INTENT_MSG = 'Не удалось определить тип вопроса.'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


@click.group()
async def cli() -> None:
    """CLI для работы с аналитикой freelancer_earnings."""
    pass


@cli.command()
async def init() -> None:
    """
    Инициализация базы данных и импорт данных из CSV.
    """
    try:
        await init_db()
        await import_csv()
        click.echo('База данных инициализирована и данные импортированы.')
    except Exception as e:
        logging.exception('Ошибка инициализации')
        click.echo(f'Ошибка: {e}')


@cli.command()
@click.argument('question')
async def ask(question: str) -> None:
    """
    Задать вопрос на естественном языке.

    Args:
        question (str): Вопрос пользователя.
    """
    if not question.strip():
        click.echo(EMPTY_QUESTION_MSG)
        return
    try:
        llm = get_llm_client()
        intent = await llm.detect_intent(question)
        if intent == 'sql':
            sql = await llm.generate_sql(question)
            click.echo(f'SQL: {sql}')
            try:
                await validate_sql(sql)
            except Exception:
                click.echo('Ошибка: сгенерирован некорректный SQL-запрос.')
                return
            result = await query_db(sql)
            click.echo(f'Результат: {result}')
        elif intent == 'smalltalk':
            click.echo(INFORMAL_MSG)
        elif intent == 'help':
            click.echo(HELP_MSG)
        else:
            click.echo(UNKNOWN_INTENT_MSG)
    except Exception as e:
        logging.exception('Ошибка при обработке вопроса')
        click.echo(f'Ошибка: {e}')


@cli.command()
@click.argument('question')
async def answer(question: str) -> None:
    """
    Ответить на вопрос на естественном языке (выводит только ответ).

    Args:
        question (str): Вопрос пользователя.
    """
    if not question.strip():
        click.echo(EMPTY_QUESTION_MSG)
        return
    try:
        llm = get_llm_client()
        intent = await llm.detect_intent(question)
        if intent == 'sql':
            sql = await llm.generate_sql(question)
            try:
                await validate_sql(sql)
            except Exception:
                click.echo('Ошибка: сгенерирован некорректный SQL-запрос.')
                return
            result = await query_db(sql)
            click.echo(f'Ответ: {result}')
        elif intent == 'smalltalk':
            click.echo(INFORMAL_MSG)
        elif intent == 'help':
            click.echo(HELP_MSG)
        else:
            click.echo(UNKNOWN_INTENT_MSG)
    except Exception as e:
        logging.exception('Ошибка при обработке вопроса')
        click.echo(f'Ошибка: {e}')


@cli.command()
async def demo() -> None:
    """
    Демонстрация работы системы на примерах вопросов.
    """
    questions = [
        'Насколько выше доход у фрилансеров, принимающих оплату в криптовалюте, по сравнению с другими способами оплаты?',
        'Как распределяется доход фрилансеров в зависимости от региона проживания?',
        'Какой процент фрилансеров, считающих себя экспертами, выполнил менее 100 проектов?',
        'Средний рейтинг клиентов для проектов длительностью более 30 дней?',
        'Сколько фрилансеров были повторно наняты?', # не-SQL вопросы:
        'Привет, как дела?', 'Как пользоваться этой системой?', 'Сколько будет два плюс два?', 'Расскажи анекдот.',
        'Помоги с командой для запуска.', ]
    llm = get_llm_client()
    for q in questions:
        click.echo(f'Вопрос: {q}')
        try:
            intent = await llm.detect_intent(q)
            if intent == 'sql':
                sql = await llm.generate_sql(q)
                result = await query_db(sql)
                click.echo(f'Ответ: {result}\n')
            elif intent == 'smalltalk':
                click.echo(f'{INFORMAL_MSG}\n')
            elif intent == 'help':
                click.echo(f'{HELP_MSG}\n')
            else:
                click.echo(f'{UNKNOWN_INTENT_MSG}\n')
        except Exception as e:
            logging.exception('Ошибка в demo')
            click.echo(f'Ошибка: {e}\n')


if __name__ == '__main__':
    cli()
