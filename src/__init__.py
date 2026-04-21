import os
from typing import Any, Dict

from dotenv import dotenv_values

ROOT_DIR = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
OUTPUT_ROOT = os.path.join(ROOT_DIR, 'data', 'outputs', 'v13')

dotenv_path = os.path.join(ROOT_DIR, '.env')
_file_env = dotenv_values(dotenv_path, verbose=True) if os.path.isfile(dotenv_path) else {}
# Start from .env file, then overlay non-empty process environment (Docker Compose / K8s / CI).
envs: Dict[str, Any] = {
    k: v for k, v in _file_env.items() if v is not None and v != ""
}
for _key, _val in os.environ.items():
    if _val != "":
        envs[_key] = _val
