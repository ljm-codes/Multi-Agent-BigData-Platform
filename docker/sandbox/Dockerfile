FROM python:3.8-slim-bookworm

COPY --from=multiagent-sandbox:old /usr/local/lib/python3.8/site-packages /usr/local/lib/python3.8/site-packages
COPY init-sandbox.sh /usr/local/bin/init-sandbox.sh
RUN chmod +x /usr/local/bin/init-sandbox.sh

VOLUME ["/export/server/miniconda3"]
CMD ["/usr/local/bin/init-sandbox.sh"]
