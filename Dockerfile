FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl git build-essential && rm -rf /var/lib/apt/lists/*


# Install Poetry globally
RUN pip install --no-cache-dir poetry

# Copy project files
WORKDIR /app
COPY pyproject.toml .
COPY poetry.lock .
COPY README.md . 
COPY promptly promptly

# Install dependencies without dev packages
RUN poetry install --without dev

COPY . .

# Use entrypoint
CMD ["poetry", "run", "entrypoint"]
