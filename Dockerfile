# Alpine Linux - minimal Docker image
FROM python:3.11-slim-alpine

# Install build deps for opcua C dependencies
RUN apk add --no-cache gcc musl-dev linux-headers

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scavenger_hunt.py .

# Default measurement name – override with --env or --label at runtime
ENV MEASUREMENT_NAME="TP"

CMD ["python", "scavenger_hunt.py"]
