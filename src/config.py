from pathlib import Path
import os
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'

load_dotenv(BASE_DIR / '.env')

DB_PATH = DATA_DIR / 'freelancer_earnings.db'
CSV_PATH = DATA_DIR / 'freelancer_earnings_bd.csv'
TEST_DB_PATH = DATA_DIR / 'test_freelancer_earnings.db'
TEST_CSV_PATH = DATA_DIR / 'test_freelancer_earnings_bd.csv'
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GEMINI_MODEL = os.getenv('GEMINI_MODEL')
