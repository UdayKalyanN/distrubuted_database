import pandas as pd
import numpy as np
from kafka import KafkaProducer
import json
from datetime import datetime
import time
import logging
import re
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from prometheus_client import Counter, Gauge, start_http_server
import socket

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class RetailDataProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.logger = logging.getLogger('RetailProducer')
        
        # Initialize metrics server with port finding
        self.metrics_port = self._find_available_port(start_port=9100, max_port=9200)
        if self.metrics_port:
            try:
                start_http_server(self.metrics_port)
                self.logger.info(f"Metrics server started on port {self.metrics_port}")
            except Exception as e:
                self.logger.error(f"Failed to start metrics server: {str(e)}")
                self.metrics_port = None
        
        # Initialize metrics
        self.data_quality_metrics = {
            'unknown_countries': Counter('retail_unknown_countries_total', 
                                      'Number of records with unknown countries', 
                                      ['country']),
            'invalid_records': Counter('retail_invalid_records_total', 
                                     'Number of invalid records', 
                                     ['reason']),
            'processed_records': Counter('retail_processed_records_total', 
                                       'Number of processed records', 
                                       ['status']),
            'data_quality_score': Gauge('retail_data_quality_score', 
                                      'Overall data quality score', 
                                      ['metric'])
        }

        # Define regional mappings
        self.region_mappings = {
            'europe': [
                'Austria', 'Belgium', 'Channel Islands', 'Cyprus', 
                'Czech Republic', 'Denmark', 'Ireland', 'EIRE', 
                'Finland', 'France', 'Germany', 'Greece', 'Iceland',
                'Italy', 'Lithuania', 'Malta', 'Netherlands', 'Norway',
                'Poland', 'Portugal', 'Spain', 'Sweden', 'Switzerland',
                'United Kingdom'
            ],
            'americas': [
                'Brazil', 'Canada', 'USA'
            ],
            'asiapacific': [
                'Australia', 'Bahrain', 'Hong Kong', 'Israel', 'Japan',
                'Lebanon', 'RSA', 'Saudi Arabia', 'Singapore',
                'United Arab Emirates'
            ]
        }
        
        # Create country sets for validation
        self.valid_countries = {country for countries in self.region_mappings.values() 
                              for country in countries}
        
        # Initialize Kafka setup
        self.setup_kafka(bootstrap_servers)

    def _find_available_port(self, start_port, max_port):
        """Find an available port in the specified range"""
        for port in range(start_port, max_port + 1):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('', port))
                    return port
            except OSError:
                continue
        self.logger.error(f"No available ports found in range {start_port}-{max_port}")
        return None

    def setup_kafka(self, bootstrap_servers):
        """Setup Kafka infrastructure"""
        try:
            # Create admin client
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            
            # Define topics
            topics = ['europe-transactions', 'americas-transactions', 'asiapacific-transactions']
            topic_list = [
                NewTopic(name=topic, num_partitions=3, replication_factor=1)
                for topic in topics
            ]
            
            # Create topics
            try:
                admin_client.create_topics(topic_list)
                self.logger.info("Kafka topics created successfully")
            except TopicAlreadyExistsError:
                self.logger.info("Topics already exist")
            finally:
                admin_client.close()

            # Initialize producer
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                compression_type='gzip'
            )
            self.logger.info("Kafka producer initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka: {str(e)}")
            raise

    def clean_data(self, df):
        """Clean and validate the data"""
        try:
            self.logger.info("Starting data cleaning process...")
            initial_count = len(df)
            df = df.copy()

            # Track original invalid records
            invalid_countries = df[~df['Country'].isin(self.valid_countries)]['Country'].value_counts()
            for country, count in invalid_countries.items():
                self.data_quality_metrics['unknown_countries'].labels(country=country).inc(count)
                self.logger.warning(f"Found {count} records with unknown country: {country}")

            # Remove records with invalid countries
            df = df[df['Country'].isin(self.valid_countries)]
            
            # Clean date fields
            df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m/%d/%y %H:%M', errors='coerce')
            invalid_dates = df['InvoiceDate'].isna().sum()
            if invalid_dates > 0:
                self.data_quality_metrics['invalid_records'].labels(reason='invalid_date').inc(invalid_dates)
                df = df.dropna(subset=['InvoiceDate'])

            # Clean numeric fields
            df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
            df['UnitPrice'] = pd.to_numeric(df['UnitPrice'], errors='coerce')
            
            # Remove invalid numeric values
            invalid_quantities = df['Quantity'].isna().sum() + (df['Quantity'] <= 0).sum()
            invalid_prices = df['UnitPrice'].isna().sum() + (df['UnitPrice'] <= 0).sum()
            
            self.data_quality_metrics['invalid_records'].labels(reason='invalid_quantity').inc(invalid_quantities)
            self.data_quality_metrics['invalid_records'].labels(reason='invalid_price').inc(invalid_prices)
            
            df = df[
                (df['Quantity'].notna()) & (df['Quantity'] > 0) &
                (df['UnitPrice'].notna()) & (df['UnitPrice'] > 0)
            ]

            # Remove cancelled orders
            cancelled = df['InvoiceNo'].str.startswith('C', na=False).sum()
            if cancelled > 0:
                self.data_quality_metrics['invalid_records'].labels(reason='cancelled_order').inc(cancelled)
                df = df[~df['InvoiceNo'].str.startswith('C', na=False)]

            # Calculate data quality scores
            completeness = len(df) / initial_count * 100
            validity = (1 - len(invalid_countries) / len(df['Country'].unique())) * 100
            
            self.data_quality_metrics['data_quality_score'].labels(metric='completeness').set(completeness)
            self.data_quality_metrics['data_quality_score'].labels(metric='validity').set(validity)

            # Log results
            final_count = len(df)
            self.logger.info(f"Data cleaning completed:")
            self.logger.info(f"Initial records: {initial_count}")
            self.logger.info(f"Invalid countries found: {len(invalid_countries)}")
            self.logger.info(f"Final records: {final_count}")
            
            return df

        except Exception as e:
            self.logger.error(f"Error in data cleaning: {str(e)}")
            raise

    def get_topic_for_country(self, country):
        """Get appropriate Kafka topic for country"""
        for region, countries in self.region_mappings.items():
            if country in countries:
                return f"{region.lower()}-transactions"
        return None

    def process_and_send_data(self, df, batch_size=100):
        """Process and send data to Kafka"""
        try:
            records = df.to_dict('records')
            total_records = len(records)
            successful_sends = 0
            failed_sends = 0

            for i in range(0, total_records, batch_size):
                batch = records[i:i+batch_size]
                self.logger.info(f"Processing batch {i//batch_size + 1}/{(total_records//batch_size) + 1}")

                for record in batch:
                    try:
                        topic = self.get_topic_for_country(record['Country'])
                        if not topic:
                            self.data_quality_metrics['invalid_records'].labels(reason='invalid_topic').inc()
                            failed_sends += 1
                            continue

                        # Create message with year_month
                        message = {
                            'invoice_no': record['InvoiceNo'],
                            'stock_code': record['StockCode'],
                            'description': record['Description'],
                            'quantity': int(record['Quantity']),
                            'invoice_date': record['InvoiceDate'].isoformat(),
                            'unit_price': float(record['UnitPrice']),
                            'customer_id': str(record['CustomerID']),
                            'country': record['Country'],
                            'year_month': record['InvoiceDate'].strftime('%Y-%m')  # Add year_month
                        }

                        future = self.producer.send(topic, message)
                        future.get(timeout=10)
                        successful_sends += 1
                        self.data_quality_metrics['processed_records'].labels(status='success').inc()

                    except Exception as e:
                        self.logger.error(f"Error sending message: {str(e)}")
                        failed_sends += 1
                        self.data_quality_metrics['processed_records'].labels(status='failed').inc()

                # Update progress
                progress = min((i + batch_size) / total_records * 100, 100)
                self.logger.info(f"Progress: {progress:.2f}%")
                self.logger.info(f"Successful: {successful_sends}, Failed: {failed_sends}")

                # Flush after each batch
                self.producer.flush()

            return successful_sends, failed_sends

        except Exception as e:
            self.logger.error(f"Error in process_and_send_data: {str(e)}")
            raise

    def cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self, 'producer'):
                self.producer.flush()
                self.producer.close(timeout=30)
                self.logger.info("Kafka producer closed successfully")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

def main():
    producer = None
    try:
        # Load data
        df = pd.read_csv('Online_Retail_IV.csv')
        logging.info(f"Loaded {len(df)} records")

        # Initialize producer
        producer = RetailDataProducer()

        # Clean data
        df_cleaned = producer.clean_data(df)
        logging.info(f"After cleaning: {len(df_cleaned)} records")

        # Process and send data
        successful, failed = producer.process_and_send_data(df_cleaned)

        logging.info("Data ingestion completed!")
        logging.info(f"Successfully processed: {successful} records")
        logging.info(f"Failed to process: {failed} records")

    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
    finally:
        if producer:
            producer.cleanup()

if __name__ == "__main__":
    main()