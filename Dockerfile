FROM python:3.8-slim

ENV PYTHONUNBUFFERED=1
ENV SPACEONE_PORT=50051
ENV SERVER_TYPE=grpc
ENV PKG_DIR=/tmp/pkg
ENV SRC_DIR=/tmp/src

RUN apt update && apt upgrade -y
RUN apt-get install gcc python3-dev -y

COPY pkg/*.txt ${PKG_DIR}/

RUN pip install --upgrade pip && \
    pip install --upgrade -r ${PKG_DIR}/pip_requirements.txt

ARG CACHEBUST=1
RUN pip install --upgrade --pre spaceone-core==1.12.24 spaceone-api==1.12.19.5

COPY src ${SRC_DIR}
WORKDIR ${SRC_DIR}
RUN python3 setup.py install && \
    rm -rf /tmp/*

EXPOSE ${SPACEONE_PORT}

ENTRYPOINT ["spaceone"]
CMD ["grpc", "spaceone.cost_analysis"]