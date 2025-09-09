FROM python:3.12
ARG SERVICE_ENV
ARG SERVICE_IMAGE

WORKDIR /workspace

RUN pip config --global set global.trusted_host 10.91.11.141
RUN pip config --global set global.index_url http://10.91.11.141:3141/root/pypi/+simple/

COPY requirements.txt requirements-tools.txt /workspace/
RUN pip install -r /workspace/requirements.txt

COPY /workspace/mnt/local-repo/hf_models/Nanonets-OCR-s /data/model/Nanonets-OCR-s
COPY /workspace/mnt/local-repo/hf_models/docling-models /data/model/docling-models

ENV SERVICE_ENV=${SERVICE_ENV}
ENV SERVICE_IMAGE=${SERVICE_IMAGE}

COPY kind_parser /workspace/

