FROM python:3.13-slim AS production
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /code/
COPY pyproject.toml .
COPY uv.lock .

ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
RUN uv sync --no-dev --frozen

COPY src/ src
COPY scripts/ scripts

CMD ["python", "-u", "/code/src/component.py"]

FROM production AS test
RUN uv sync --all-groups --frozen

COPY tests/ tests
