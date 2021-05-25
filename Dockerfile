FROM python:3.9

RUN pip install pipenv

ENV PROJECT_DIR /crypto_momentum

WORKDIR ${PROJECT_DIR}

COPY Pipfile Pipfile.lock ${PROJECT_DIR}/
COPY live_trader.py ${PROJECT_DIR}/
COPY backtester.py ${PROJECT_DIR}/
COPY config.json ${PROJECT_DIR}/
COPY data/ ${PROJECT_DIR}/data/
COPY exchanges/ ${PROJECT_DIR}/exchanges/
COPY install.sh ${PROJECT_DIR}/
COPY strategies/ ${PROJECT_DIR}/strategies/
COPY utils/ ${PROJECT_DIR}/utils/

RUN mkdir logs
RUN ./install.sh --no-pipenv-install
RUN pipenv install --system --deploy

ENTRYPOINT ["python", "live_trader.py", "-n"]
