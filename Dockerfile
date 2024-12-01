FROM python:3.13

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY token_checker.py ./
CMD while true; do python token_checker.py; sleep 10; done
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 CMD curl -f http://localhost:8080/health || exit 1