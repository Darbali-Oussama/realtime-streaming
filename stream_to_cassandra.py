import logging
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("cassandra_streaming")


def connect_to_cassandra():
    """
    Connect to Cassandra and return a session.
    """
    try:
        # Setup authentication if necessary
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        
        # Connect to the Cassandra cluster
        cluster = Cluster(['cassandra'], port=9042, auth_provider=auth_provider)
        session = cluster.connect()

        # Set the keyspace
        create_keyspace_query = """
        CREATE KEYSPACE IF NOT EXISTS cars_streaming
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
        AND durable_writes = true;
        """
        session.execute(create_keyspace_query)

        session.set_keyspace('cars_streaming')
        logging.info('Connected to Cassandra')
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        raise

    return session


def create_kafka_consumer():
    """
    Creates a Kafka consumer to listen for new messages on a Kafka topic.
    """
    try:
        consumer = KafkaConsumer(
            'cars_data',
            bootstrap_servers=['kafka1:19092', 'kafka2:19093', 'kafka3:19094'],
            consumer_timeout_ms=30000,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info('Kafka consumer created successfully')
    except Exception as e:
        logging.error(f"Failed to create Kafka consumer: {e}")
        raise

    return consumer

def escape_single_quotes(value):
    """Escape single quotes in a string for Cassandra."""
    return value.replace("'", "''") if value is not None else None

def format_data(data):
    """Format and sanitize data before inserting into Cassandra."""
    return {
        "prix": int(data.get("prix") or 0),  # Ensure the value is an integer
        "Cheval": int(data.get("Cheval") or 0),  # Ensure the value is an integer
        "Carburant": str(data.get("Carburant") or "Unknown"),  # Ensure the value is a string
        "Boite_a_vitesse": str(data.get("Boite_a_vitesse") or "Unknown"),  # Ensure the value is a string
        "Modele": str(data.get("Modele") or "Unknown"),  # Ensure the value is a string
        "Kilometrage": str(data.get("Kilometrage", "") or "Unknown"),  # Ensure the value is a sanitized string
        "Premiere_main": str(data.get("Premiere main") or "Non"),  # Ensure the value is a string
        "Marque": str(data.get("Marque") or "Unknown"),  # Ensure the value is a string
        "Etat": str(data.get("Etat") or "Unknown"),  # Ensure the value is a string
        "Nombre_de_portes": int(data.get("Nombre de portes") or 0),  # Ensure the value is an integer
        "Age": int(data.get("Age") or 0),  # Ensure the value is an integer
        "Origine": str(data.get("Origine") or "Unknown"),  # Ensure the value is a string
        "url": str(escape_single_quotes(data.get("url")) or ""),  # Ensure the value is a string
        #"url": escape_single_quotes(data.get("url")) if data.get("url") is not None else None,  # Escape quotes here
        "Predictions": float(data.get("Predictions") or 0.0),  # Ensure the value is a float
        "Prediction_Status": str(data.get("Prediction_Status") or "Unknown")  # Ensure the value is a string
    }

def stream_data_to_cassandra(consumer, session):

    create_table_query = """
    CREATE TABLE IF NOT EXISTS cars_streaming.cars_data (
        prix INT,
        Cheval INT,
        Carburant TEXT,
        Boite_a_vitesse TEXT,
        Modele TEXT,
        Kilometrage TEXT,
        Premiere_main TEXT,
        Marque TEXT,
        Etat TEXT,
        Nombre_de_portes INT,
        Age INT,
        Origine TEXT,
        url TEXT,
        Predictions FLOAT,
        Prediction_Status TEXT,
        PRIMARY KEY (prix)
    );
    """
    session.execute(create_table_query)


    insert_query = """
    INSERT INTO cars_streaming.cars_data (
        prix, Cheval, Carburant, Boite_a_vitesse, Modele, Kilometrage, 
        Premiere_main, Marque, Etat, Nombre_de_portes, Age, Origine, url, 
        Predictions, Prediction_Status
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for message in consumer:
        try:
            raw_message = message.value
            logging.info(f"Received raw message: {raw_message}")

            data_list = json.loads(raw_message)

            if isinstance(data_list, list):
                for data in data_list:
                    

                    formatted_data = format_data(data)

                    # Log the formatted data
                    logging.info(f"Formatted data: {formatted_data}")

                    insert_query = f"""
                    INSERT INTO cars_streaming.cars_data (
                        prix, Cheval, Carburant, Boite_a_vitesse, Modele, Kilometrage, 
                        Premiere_main, Marque, Etat, Nombre_de_portes, Age, Origine, url, 
                        Predictions, Prediction_Status
                    ) VALUES (
                        {formatted_data['prix']}, 
                        {formatted_data['Cheval']}, 
                        '{formatted_data['Carburant']}', 
                        '{formatted_data['Boite_a_vitesse']}', 
                        '{formatted_data['Modele']}', 
                        '{formatted_data['Kilometrage']}', 
                        '{formatted_data['Premiere_main']}', 
                        '{formatted_data['Marque']}', 
                        '{formatted_data['Etat']}', 
                        {formatted_data['Nombre_de_portes']}, 
                        {formatted_data['Age']}, 
                        '{formatted_data['Origine']}', 
                        '{formatted_data['url']}', 
                        {formatted_data['Predictions']}, 
                        '{formatted_data['Prediction_Status']}'
                    );
                    """
                    
                    session.execute(insert_query)

                    logging.info("Data inserted into Cassandra")
            else:
                logging.error(f"Received message is not a list: {type(data_list)}")
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error: {e}")
        except Exception as e:
            logging.error(f"Failed to insert data into Cassandra: {e}")

    logging.info("Shutting down Kafka consumer and Cassandra session")
    consumer.close()  # Close the Kafka consumer
    session.shutdown()  # Close the Cassandra session
    logging.info("Shutdown complete")





def store_to_cassandra():
    # Connect to Cassandra
    session = connect_to_cassandra()

    # Create a Kafka consumer
    consumer = create_kafka_consumer()

    # Start streaming data from Kafka to Cassandra
    stream_data_to_cassandra(consumer, session)


if __name__ == '__main__':
    store_to_cassandra()  # Fixed the function call to `store_to_cassandra`
