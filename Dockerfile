FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY trino_setup ./trino_setup
RUN pip install --no-cache-dir . && useradd --uid 1000 --create-home setup && mkdir /output && chown setup /output
USER 1000:1000
ENTRYPOINT ["trino-setup"]
CMD ["--help"]
