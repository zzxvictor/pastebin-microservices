FROM python:3.8
WORKDIR /app
COPY ./src/requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY ./src .
CMD ["sh", "./start.sh"]