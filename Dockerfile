FROM python:3.7-slim

WORKDIR /app
COPY ./requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip
RUN python -m pip install --upgrade python-dev-tools
RUN pip install --no-cache-dir --upgrade -r ./requirements.txt
COPY ./app ./

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9001"]