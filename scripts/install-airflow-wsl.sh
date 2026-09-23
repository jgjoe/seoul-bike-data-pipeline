#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "Airflow reference runtime is Linux/WSL2; refusing non-Linux install." >&2
  exit 2
fi

AIRFLOW_VERSION="3.3.2"
PYTHON_VERSION="3.12"
VENV_PATH="${1:-.venv-airflow}"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

uv venv --python "${PYTHON_VERSION}" "${VENV_PATH}"
uv pip install \
  --python "${VENV_PATH}/bin/python" \
  "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "${CONSTRAINT_URL}"
uv pip install \
  --python "${VENV_PATH}/bin/python" \
  -e ".[test]"
uv pip check --python "${VENV_PATH}/bin/python"

echo "Airflow ${AIRFLOW_VERSION} environment ready at ${VENV_PATH}"
