"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

APP_KAFKA_BOOTSTRAP_SERVER = 'PLAINTEXT://kafka0:9092,PLAINTEXT://kafka1:9093,PLAINTEXT://kafka2:9094'
APP_KAFKA_SCHEMA_REGISTRY_SERVER = 'http://schema-registry:8081'

schema_registry = CachedSchemaRegistryClient({"url": APP_KAFKA_SCHEMA_REGISTRY_SERVER})


class Producer:
    """Defines and provides common functionality amongst Producers"""

    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.broker_properties = {
            'bootstrap.servers': APP_KAFKA_BOOTSTRAP_SERVER
        }

        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(self.broker_properties, schema_registry=schema_registry)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        topic = NewTopic(
            self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas,
            config={}
        )
        AdminClient(self.broker_properties).create_topics([topic])
        logger.info("topic creation kafka integration incomplete - skipping")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.close()
        logger.info("producer close incomplete - skipping")

    @staticmethod
    def time_millis():
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
