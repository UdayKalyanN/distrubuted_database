from kafka import KafkaConsumer, TopicPartition
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import json
from datetime import datetime
import threading
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class RegionalConsumer:
    def __init__(self, topic, region, cassandra_host):
        self.topic = topic
        self.region = region
        self.cassandra_host = cassandra_host
        self.logger = logging.getLogger(f'{self.region}Consumer')
        self.setup_consumer()
        self.setup_cassandra()

    def setup_consumer(self):
        """Setup Kafka consumer"""
        self.consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=None,  # Don't use consumer groups
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Assign to all partitions of the topic
        partitions = self.consumer.partitions_for_topic(self.topic)
        if not partitions:
            self.logger.error(f"No partitions found for topic {self.topic}")
            return

        self.logger.info(f"Found partitions: {partitions}")
        topic_partitions = [TopicPartition(self.topic, p) for p in partitions]
        self.consumer.assign(topic_partitions)

        # Seek to beginning for all partitions
        for tp in topic_partitions:
            self.consumer.seek_to_beginning(tp)
            self.logger.info(f"Seeking to beginning for {tp}")
        
        self.logger.info(f"Consumer setup completed for {self.region}")

    def setup_cassandra(self):
        """Setup Cassandra connection"""
        try:
            self.cluster = Cluster([self.cassandra_host])
            self.session = self.cluster.connect('retail_analytics')
            self.prepare_statements()
            self.logger.info(f"Cassandra connection established for {self.region}")
        except Exception as e:
            self.logger.error(f"Error connecting to Cassandra: {str(e)}")
            raise

    def prepare_statements(self):
        """Prepare all CQL statements"""
        self.insert_transaction = self.session.prepare("""
            INSERT INTO transactions_by_region (
                country, invoice_no, stock_code, description, quantity,
                invoice_date, unit_price, customer_id, year_month
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        # Updated counter statements
        self.update_customer_analytics = self.session.prepare("""
            UPDATE customer_analytics 
            SET total_purchases = total_purchases + 1,
                total_amount = total_amount + ?,
                total_items = total_items + ?
            WHERE customer_id = ? 
            AND year_month = ?
        """)

        self.update_product_analytics = self.session.prepare("""
            UPDATE product_analytics 
            SET total_sales = total_sales + 1,
                total_revenue = total_revenue + ?,
                total_quantity = total_quantity + ?
            WHERE stock_code = ? 
            AND year_month = ?
        """)

        self.update_country_metrics = self.session.prepare("""
            UPDATE country_metrics 
            SET total_sales = total_sales + 1,
                total_revenue = total_revenue + ?,
                total_customers = total_customers + 1
            WHERE country = ? 
            AND date = ?
        """)

    def process_message(self, message):
        """Process a single message and store in Cassandra"""
        try:
            data = message.value
            invoice_date = datetime.fromisoformat(data['invoice_date'])
            
            # Create batch statement for non-counter updates
            non_counter_batch = BatchStatement()
            
            # Add transaction record (non-counter)
            non_counter_batch.add(self.insert_transaction, (
                data['country'],
                data['invoice_no'],
                data['stock_code'],
                data['description'],
                data['quantity'],
                invoice_date,
                data['unit_price'],
                data['customer_id'],
                data['year_month']
            ))

            # Execute non-counter batch first
            self.session.execute(non_counter_batch)
            
            # Calculate total amount in cents/pence (integer)
            total_amount = int(float(data['quantity']) * float(data['unit_price']) * 100)
            quantity = int(data['quantity'])
            
            try:
                # Update customer analytics
                self.session.execute(self.update_customer_analytics, (
                    total_amount,        # total_amount increment
                    quantity,            # total_items increment
                    data['customer_id'], # customer_id
                    data['year_month']   # year_month
                ))
                
                # Update product analytics
                self.session.execute(self.update_product_analytics, (
                    total_amount,        # total_revenue increment
                    quantity,            # total_quantity increment
                    data['stock_code'],  # stock_code
                    data['year_month']   # year_month
                ))
                
                # Update country metrics
                self.session.execute(self.update_country_metrics, (
                    total_amount,        # total_revenue increment
                    data['country'],     # country
                    invoice_date.date()  # date
                ))
                
                self.logger.info(f"Successfully processed transaction {data['invoice_no']}")
                
            except Exception as e:
                self.logger.error(f"Error updating counters: {str(e)}")
                raise
                
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            raise

    # def process_message(self, message):
    #     """Process a single message and store in Cassandra"""
    #     try:
    #         data = message.value
    #         invoice_date = datetime.fromisoformat(data['invoice_date'])
            
    #         # Create batch statement for non-counter updates
    #         non_counter_batch = BatchStatement()
            
    #         # Add transaction record (non-counter)
    #         non_counter_batch.add(self.insert_transaction, (
    #             data['country'],
    #             data['invoice_no'],
    #             data['stock_code'],
    #             data['description'],
    #             data['quantity'],
    #             invoice_date,
    #             data['unit_price'],
    #             data['customer_id'],
    #             data['year_month']
    #         ))

    #         self.logger.debug("Insert Transaction Statement Prepared")
            
    #         # Calculate total amount and convert to integer (cents/pence)
    #         total_amount = int(float(data['quantity']) * float(data['unit_price']) * 100)  # Convert to cents/pence
            
    #         # Execute non-counter batch first
    #         self.session.execute(non_counter_batch)
    #         self.logger.debug("Non-counter batch executed successfully")
            
    #         # Execute counter updates separately
    #         # Update customer analytics
    #         self.session.execute(self.update_customer_analytics, (
    #             total_amount,  # total_amount increment (in cents/pence)
    #             int(data['quantity']),  # total_items increment
    #             data['customer_id'],  # customer_id
    #             data['year_month']  # year_month
    #         ))
            
    #         # Update product analytics
    #         self.session.execute(self.update_product_analytics, (
    #             total_amount,  # total_revenue increment (in cents/pence)
    #             int(data['quantity']),  # total_quantity increment
    #             data['stock_code'],  # stock_code
    #             data['year_month']  # year_month
    #         ))
            
    #         # Update country metrics
    #         self.session.execute(self.update_country_metrics, (
    #             total_amount,  # total_revenue increment (in cents/pence)
    #             data['country'],  # country
    #             invoice_date.date()  # date
    #         ))
            
    #         self.logger.info(f"Successfully processed transaction {data['invoice_no']}")
            
    #     except Exception as e:
    #         self.logger.error(f"Error processing message: {str(e)}")
    #         raise

    def start_consuming(self):
        """Start consuming messages"""
        self.logger.info(f"Starting to consume messages from {self.topic}")
        message_count = 0
        start_time = time.time()

        try:
            while True:
                msg_pack = self.consumer.poll(timeout_ms=1000)
                if msg_pack:
                    for tp, messages in msg_pack.items():
                        for message in messages:
                            try:
                                message_count += 1
                                self.process_message(message)
                            except Exception as e:
                                self.logger.error(f"Failed to process message: {str(e)}")
                                continue

                if time.time() - start_time >= 60:  # Run for 60 seconds max
                    break

        except Exception as e:
            self.logger.error(f"Error in consumer: {str(e)}")
        finally:
            self.logger.info(f"Total messages consumed from {self.topic}: {message_count}")
            self.cleanup()

    def cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self, 'consumer'):
                self.consumer.close()
                self.logger.info("Kafka consumer closed")
            if hasattr(self, 'cluster'):
                self.cluster.shutdown()
                self.logger.info("Cassandra cluster connection shutdown")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

def main():
    try:
        # Create consumers for each region
        consumers = [
            RegionalConsumer('europe-transactions', 'Europe', 'localhost'),
            RegionalConsumer('americas-transactions', 'Americas', 'localhost'),
            RegionalConsumer('asiapacific-transactions', 'AsiaPacific', 'localhost')
        ]
        
        # Start consumers in separate threads
        threads = []
        for consumer in consumers:
            thread = threading.Thread(target=consumer.start_consuming)
            thread.start()
            threads.append(thread)
            time.sleep(2)  # Delay between starting consumers
        
        # Wait for all threads
        for thread in threads:
            thread.join()
            
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()