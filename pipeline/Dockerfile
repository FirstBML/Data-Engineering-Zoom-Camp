FROM python:3.13.1-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

ENV PATH="/code/.venv/bin:$PATH"

RUN pip install pandas pyarrow

WORKDIR /code 

COPY pyproject.toml uv.lock .python-version ./

RUN uv sync --locked

COPY ingest_data.py .

ENTRYPOINT ["python", "ingest_data.py" ]