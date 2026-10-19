FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py ./
# -I: 작업 디렉터리의 파일이 표준 모듈을 가리지 않게 한다
CMD ["python", "-I", "producer.py"]
