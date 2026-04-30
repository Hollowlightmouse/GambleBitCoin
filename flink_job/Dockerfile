FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir redis kafka-python-ng

# Copy application files
COPY flink_streaming_job.py /opt/flink/usrlib/flink_streaming_job.py
COPY submit_job.sh /opt/flink/usrlib/submit_job.sh
RUN chmod +x /opt/flink/usrlib/submit_job.sh

# Note: docker-compose mounts ./flink_job volume which will override these files
# These COPY instructions ensure the image works standalone without docker-compose

CMD ["/opt/flink/usrlib/submit_job.sh"]
