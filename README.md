# Real time Streaming with Scraping and Analysis

This project involves scraping car characteristics and prices from a used car marketplace website. The scraped data is cleaned and analyzed using statistical techniques to explore various characteristics. A machine learning model is built to predict trends or patterns based on the data.

For real-time operations, Airflow is used to schedule scraping and orchestrate tasks, Apache Kafka handles data streaming, Cassandra is used for data storage, and Docker is used for containerization of the services.

### Technologies Used:
- Python, Scikit-learn, Pandas, BeautifulSoup
- Airflow, Kafka, Cassandra, Docker

### How to Build and Run

1. **Build the Docker image for Airflow:**
   ```bash
   docker build -t my_airflow .

2. **Run the containers:**
   ```bash
   docker-compose -f docker-compose.yml up
   docker-compose -f docker-compose-executer.yml up
