import os
from google import genai
from .config import GEMINI_API_KEY, GEMINI_MODEL
from .db import get_table_schema_str
from typing import Optional, Type
from pydantic import BaseModel
import logging
from abc import ABC, abstractmethod

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
llm_result_logger = logging.getLogger('llm_results')
llm_result_handler = logging.FileHandler('llm_results.log', encoding='utf-8')
llm_result_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
llm_result_logger.addHandler(llm_result_handler)
llm_result_logger.setLevel(logging.INFO)

PROMPT_TEMPLATE = (
    'You are a data analytics assistant. Transform the user\'s question in Russian into a correct SQL query for the freelancer_earnings table. Do not add explanations, only SQL.\n\n'
    '{schema}\nQuestion: {question}\nSQL:')

INTENT_PROMPT_TEMPLATE = ("""
You are a data analytics assistant. Determine the intent of the user's question in Russian. Possible intents:
- sql: the question requires an SQL query to the freelancer_earnings table
- smalltalk: informal question not related to analytics
- help: request for help using the system
- unknown: could not determine

Question: {question}
Intent:
""")


class SQLResponse(BaseModel):
    sql: str


class IntentResponse(BaseModel):
    intent: str


class LLMClient(ABC):
    @abstractmethod
    async def generate_sql(self, question: str) -> str:
        """
        Сгенерировать SQL-запрос по вопросу пользователя.

        Args:
            question (str): Вопрос пользователя.
        Returns:
            str: SQL-запрос.
        """
        pass

    @abstractmethod
    async def detect_intent(self, question: str) -> str:
        """
        Определить интент вопроса пользователя.

        Args:
            question (str): Вопрос пользователя.
        Returns:
            str: Интент (sql, smalltalk, help, unknown).
        """
        pass


class GeminiLLMClient(LLMClient):
    def __init__(self, api_key: str, model: str) -> None:
        self.client = genai.Client(api_key=api_key)
        self.model = model

    async def _generate_content_with_schema(self, prompt: str, response_schema: Optional[Type[BaseModel]] = None,
            temperature: float = 0.0) -> BaseModel:
        """
        Генерирует ответ от LLM с заданной схемой.

        Args:
            prompt (str): Промпт для LLM.
            response_schema (Optional[Type[BaseModel]]): Pydantic-схема для парсинга.
            temperature (float): Температура генерации.
        Returns:
            BaseModel: Ответ, приведённый к схеме.
        """
        try:
            config = {"temperature": temperature}
            if response_schema:
                config.update({"response_mime_type": "application/json", "response_schema": response_schema})
            response = await self.client.aio.models.generate_content(model=self.model, contents=prompt, config=config)
            if response_schema:
                return response.parsed
            else:
                return response.text
        except Exception as e:
            logging.exception('Ошибка при генерации контента через LLM')
            raise Exception(f"Ошибка при генерации контента: {str(e)}")

    async def generate_sql(self, question: str) -> str:
        """
        Сгенерировать SQL-запрос по вопросу пользователя.
        """
        schema = await get_table_schema_str()
        prompt = PROMPT_TEMPLATE.format(schema=schema, question=question)
        try:
            result = await self._generate_content_with_schema(prompt, SQLResponse, temperature=0.0)
            sql = result.sql.strip()
            logging.info('SQL-запрос сгенерирован через LLM.')
            llm_result_logger.info(f'Вопрос: {question}\nSQL: {sql}')
            return sql
        except Exception as e:
            logging.exception('Ошибка генерации SQL через LLM')
            return "SELECT 1"

    async def detect_intent(self, question: str) -> str:
        """
        Определить интент вопроса пользователя.
        """
        prompt = INTENT_PROMPT_TEMPLATE.format(question=question)
        try:
            result = await self._generate_content_with_schema(prompt, IntentResponse, temperature=0.0)
            intent = result.intent.strip().lower()
            logging.info(f'Интент определён: {intent}')
            llm_result_logger.info(f'Вопрос: {question}\nИнтент: {intent}')
            return intent
        except Exception as e:
            logging.exception('Ошибка определения интента')
            return "unknown"


def get_llm_client() -> LLMClient:
    """
    Получить LLM-клиент (по умолчанию Gemini).
    """
    return GeminiLLMClient(GEMINI_API_KEY, GEMINI_MODEL)
