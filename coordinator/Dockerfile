FROM tiangolo/uvicorn-gunicorn:python3.10


COPY requirements.txt /

WORKDIR /

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY coordinator.py /




#CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", ""] 