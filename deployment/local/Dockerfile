ARG BASE
FROM ${BASE}
USER root
ARG ROLE
WORKDIR /opt/oip
COPY assets/${ROLE}/ /opt/wheels/
RUN python3 -m venv --without-pip /opt/venv && python3 -m pip install --no-cache-dir --no-index --find-links=/opt/wheels --require-hashes --target=/opt/venv/lib/$(python3 -c 'import sys; print("python%d.%d" % sys.version_info[:2])')/site-packages -r /opt/wheels/requirements.lock && /opt/venv/bin/python3 -m pip check
COPY assets/source.tar /tmp/source.tar
RUN tar -xf /tmp/source.tar -C /opt/oip && rm /tmp/source.tar
COPY entrypoints.py /opt/pack/entrypoints.py
COPY oracle_adapter.py /opt/pack/oracle_adapter.py
COPY preflight.py /opt/pack/preflight.py
COPY mismatch_worker.sh /opt/pack/mismatch_worker.sh
RUN chmod 755 /opt/pack/mismatch_worker.sh
RUN if [ "$ROLE" = spark ]; then ln -s /opt/wheels/jars /opt/pack/jars && ln -s /opt/wheels/ivy /root/.ivy2; fi
ENV OIP_LOCAL_ROLE=${ROLE} PATH=/opt/venv/bin:$PATH PYTHONPATH=/opt/oip PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENTRYPOINT []
CMD ["python3", "/opt/pack/preflight.py"]
