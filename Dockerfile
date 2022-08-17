FROM python:3.9

WORKDIR /src

COPY requirements*.txt .
COPY docs/requirements.txt ./doc-requirements.txt

RUN pip install -r requirements.txt
RUN pip install -r doc-requirements.txt

CMD ["/bin/bash"]
