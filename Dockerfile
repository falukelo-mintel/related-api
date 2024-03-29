FROM python:3.7-slim

WORKDIR /app
COPY ./requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r ./requirements.txt
RUN export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
COPY ./app ./

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9001"]