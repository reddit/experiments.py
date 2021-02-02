FROM python:3.8

WORKDIR /src
COPY requirements*.txt .
RUN pip install -r requirements.txt
CMD ["/bin/bash"]
