FROM python:3.11-slim

# LibreOffice for .xls -> .xlsx conversion
RUN apt-get update && apt-get install -y --no-install-recommends     libreoffice-calc libreoffice-writer libreoffice-core fonts-dejavu  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
ENTRYPOINT ["python", "-m", "src.rtt_etl"]
