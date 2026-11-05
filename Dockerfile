FROM python:3.15-slim AS builder
WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip wheel --no-deps --wheel-dir /wheels .

FROM python:3.15-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN groupadd --gid 10001 ledgerflow && useradd --uid 10001 --gid ledgerflow --create-home ledgerflow \
    && mkdir /data && chown ledgerflow:ledgerflow /data
COPY --from=builder /wheels /wheels
RUN python -m pip install --no-cache-dir --no-deps /wheels/*.whl && rm -rf /wheels
USER 10001:10001
WORKDIR /data
ENTRYPOINT ["ledgerflow"]
CMD ["--help"]
