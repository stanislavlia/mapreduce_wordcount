FROM tiangolo/uvicorn-gunicorn:python3.10


COPY requirements.txt /

WORKDIR /

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY count_utils.py  worker.py /




#CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", ""] 