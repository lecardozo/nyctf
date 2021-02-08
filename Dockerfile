FROM python:3.8.6-slim
ADD . /code
RUN pip install ./code[server]
CMD ["gunicorn", "-b", "0.0.0.0:8080", "nyctf.inference.server:create_server('/code/model.pkl')"]