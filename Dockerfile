FROM apache/airflow:2.4.2-python3.8

# Install additional packages
#COPY requirements.txt /requirements.txt
RUN pip install azure-servicebus

#RUN pip install -r requirements.txt
