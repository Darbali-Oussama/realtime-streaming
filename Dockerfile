FROM puckel/docker-airflow:1.10.9

# Install required packages and upgrade pip
RUN pip install --upgrade pip

# Copy the requirements.txt into the image and install dependencies
COPY requirements.txt /usr/local/airflow/requirements.txt
RUN pip install -r /usr/local/airflow/requirements.txt

# Set up the Airflow command (adjust as necessary for initdb and starting services)
CMD sh -c "airflow initdb"
