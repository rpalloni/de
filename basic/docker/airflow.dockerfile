FROM apache/airflow:2.2.5-python3.8

# use root for settings
USER root

ARG YOUR_ENV="virtualenv"

ENV YOUR_ENV=${YOUR_ENV} \
    PYTHONPATH="/opt/" \
    # PYTHONPATH="/opt/" -> python path to airflow-common volumes
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

# linux libs 
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc curl libpq-dev \ 
    && pip install xlsxwriter \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 

# poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | POETRY_HOME=/opt/poetry python && \
    cd /usr/local/bin && \
    ln -s /opt/poetry/bin/poetry && \
    poetry config virtualenvs.create false

# copy dependencies files
COPY pyproject.toml .
COPY poetry.lock .

# project initialization
RUN poetry config virtualenvs.create false \
    && poetry install $(test "$YOUR_ENV" = production) --no-root --no-dev --no-interaction --no-ansi

# use airflow for r/w files
USER airflow
