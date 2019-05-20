FROM python:2.7-alpine

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN apk add --no-cache --update --virtual .build-deps \
        g++ \
        python-dev \
        libxml2 \
        libxml2-dev && \
    apk add --no-cache --update \
        libxslt-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    apk del .build-deps

COPY . .

ENTRYPOINT [ "python", "./gateway.py" ]