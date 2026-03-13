import os

from dotenv import dotenv_values

ROOT_DIR = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
OUTPUT_ROOT = os.path.join(ROOT_DIR, 'data', 'outputs', 'v12')

dotenv_path = os.path.join(ROOT_DIR, '.env')

envs = dotenv_values(dotenv_path, verbose=True)
