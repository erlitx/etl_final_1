FROM python:3.10

# Set the working directory
WORKDIR /app

# Install PostgreSQL client & dependencies
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

# Copy migration scripts
COPY . /app


RUN pip install alembic psycopg2-binary


CMD ["alembic", "upgrade", "head"]
