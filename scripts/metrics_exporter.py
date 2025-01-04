import socket
from prometheus_client import start_http_server, Counter, Gauge, Histogram, Summary
import time
import logging
import threading
from datetime import datetime
import psutil
import random  # For simulating data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class RetailMetricsExporter:
    def __init__(self, start_port=9100, max_port=9100):
        self.logger = logging.getLogger('RetailMetricsExporter')
        self.start_port = start_port
        self.max_port = max_port
        self.port = self._find_available_port()
        self.running = True

        # Region mappings
        self.region_mappings = {
            'Europe': ['United Kingdom', 'Germany', 'France'],
            'Americas': ['USA', 'Canada', 'Brazil'],
            'AsiaPacific': ['Australia', 'Japan', 'Singapore']
        }

        # Initialize all metrics
        self._initialize_metrics()

    def _initialize_metrics(self):
        """Initialize all metrics"""
        # Business Metrics
        self.transaction_count = Counter(
            'retail_transaction_count_total', 
            'Total number of transactions',
            ['region', 'country']
        )
        self.total_revenue = Counter(
            'retail_revenue_total',
            'Total revenue',
            ['region', 'country']
        )
        
        # Sales Performance Metrics
        self.avg_basket_size = Gauge(
            'retail_avg_basket_size',
            'Average number of items per transaction',
            ['region']
        )
        self.avg_transaction_value = Gauge(
            'retail_avg_transaction_value',
            'Average monetary value per transaction',
            ['region']
        )
        
        # Product Metrics
        self.product_stock_level = Gauge(
            'retail_product_stock_level',
            'Current stock level for products',
            ['product_category']
        )
        self.product_sales = Counter(
            'retail_product_sales_total',
            'Total sales by product category',
            ['product_category']
        )
        
        # Customer Metrics
        self.customer_segments = Gauge(
            'retail_customer_segments',
            'Number of customers in each segment',
            ['segment']
        )
        self.customer_satisfaction = Gauge(
            'retail_customer_satisfaction',
            'Customer satisfaction score',
            ['region']
        )
        
        # Operational Metrics
        self.order_processing_time = Histogram(
            'retail_order_processing_seconds',
            'Time taken to process orders',
            ['region'],
            buckets=(10, 30, 60, 120, 300, 600)
        )
        self.inventory_turnover = Gauge(
            'retail_inventory_turnover_ratio',
            'Inventory turnover ratio by region',
            ['region']
        )
        
        # System Metrics
        self.system_metrics = {
            'cpu_usage': Gauge('system_cpu_usage', 'CPU Usage Percentage'),
            'memory_usage': Gauge('system_memory_usage', 'Memory Usage Percentage'),
            'disk_usage': Gauge('system_disk_usage', 'Disk Usage Percentage')
        }

        # Performance Metrics
        self.query_latency = Histogram(
            'retail_query_latency_seconds', 
            'Time spent executing queries',
            ['query_type', 'datacenter']
        )
        
        # Initialize metrics with sample data
        self._initialize_sample_data()

    def _initialize_sample_data(self):
        """Initialize metrics with sample data"""
        # Initialize regional metrics
        for region, countries in self.region_mappings.items():
            for country in countries:
                self.transaction_count.labels(region=region, country=country).inc(100)
                self.total_revenue.labels(region=region, country=country).inc(5000)
            
            self.avg_basket_size.labels(region=region).set(random.uniform(2.5, 4.5))
            self.avg_transaction_value.labels(region=region).set(random.uniform(50, 150))
            self.customer_satisfaction.labels(region=region).set(random.uniform(3.5, 4.8))
            self.inventory_turnover.labels(region=region).set(random.uniform(4, 8))

        # Initialize product metrics
        product_categories = ['Electronics', 'Clothing', 'Home', 'Food', 'Beauty']
        for category in product_categories:
            self.product_stock_level.labels(product_category=category).set(random.randint(100, 1000))
            self.product_sales.labels(product_category=category).inc(random.randint(50, 200))

        # Initialize customer segment metrics
        segments = ['High Value', 'Mid Value', 'Low Value', 'At Risk']
        for segment in segments:
            self.customer_segments.labels(segment=segment).set(random.randint(100, 1000))

    def update_system_metrics(self):
        """Update system metrics"""
        try:
            self.system_metrics['cpu_usage'].set(psutil.cpu_percent())
            self.system_metrics['memory_usage'].set(psutil.virtual_memory().percent)
            self.system_metrics['disk_usage'].set(psutil.disk_usage('/').percent)
        except Exception as e:
            self.logger.error(f"Error updating system metrics: {str(e)}")

    def update_business_metrics(self):
        """Update business metrics with simulated data"""
        try:
            # Update regional metrics
            for region, countries in self.region_mappings.items():
                # Simulate transactions and revenue for each country
                for country in countries:
                    self.transaction_count.labels(region=region, country=country).inc(random.randint(5, 15))
                    self.total_revenue.labels(region=region, country=country).inc(random.randint(200, 800))
                
                # Update average metrics
                self.avg_basket_size.labels(region=region).set(random.uniform(2.5, 4.5))
                self.avg_transaction_value.labels(region=region).set(random.uniform(50, 150))
                
                # Simulate order processing times
                self.order_processing_time.labels(region=region).observe(random.uniform(10, 600))
                
                # Update inventory turnover
                self.inventory_turnover.labels(region=region).set(random.uniform(4, 8))
                
                # Update customer satisfaction
                self.customer_satisfaction.labels(region=region).set(random.uniform(3.5, 4.8))

            # Update product metrics
            product_categories = ['Electronics', 'Clothing', 'Home', 'Food', 'Beauty']
            for category in product_categories:
                # Simulate stock level changes
                current_stock = random.randint(100, 1000)
                self.product_stock_level.labels(product_category=category).set(current_stock)
                # Simulate sales
                self.product_sales.labels(product_category=category).inc(random.randint(5, 20))

            # Update customer segments
            segments = ['High Value', 'Mid Value', 'Low Value', 'At Risk']
            for segment in segments:
                self.customer_segments.labels(segment=segment).set(random.randint(100, 1000))

        except Exception as e:
            self.logger.error(f"Error updating business metrics: {str(e)}")

    def metrics_updater(self):
        """Background thread to periodically update metrics"""
        self.logger.info("Starting metrics updater thread")
        while self.running:
            try:
                self.update_system_metrics()
                self.update_business_metrics()
                time.sleep(15)
            except Exception as e:
                self.logger.error(f"Error in metrics updater: {str(e)}")
                time.sleep(15)

    def _find_available_port(self):
        """Find an available port in the specified range"""
        for port in range(self.start_port, self.max_port + 1):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('0.0.0.0', port))
                    return port
            except OSError:
                continue
        raise RuntimeError(f"No available ports in range {self.start_port}-{self.max_port}")

    def start_metrics_server(self):
        """Start the Prometheus metrics server"""
        try:
            start_http_server(self.port, addr='0.0.0.0')
            self.logger.info(f"Metrics server started on port {self.port}")
        except Exception as e:
            self.logger.error(f"Failed to start metrics server: {str(e)}")
            raise

    def start(self):
        """Start the metrics exporter"""
        try:
            self.logger.info("Starting metrics exporter...")
            self.start_metrics_server()
            
            updater_thread = threading.Thread(target=self.metrics_updater, daemon=True)
            updater_thread.start()
            
            self.logger.info(f"Metrics exporter running on http://0.0.0.0:{self.port}/metrics")
            
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            self.logger.error(f"Error starting metrics exporter: {str(e)}")
            self.stop()

    def stop(self):
        """Stop the metrics exporter"""
        self.logger.info("Stopping metrics exporter...")
        self.running = False

def main():
    exporter = None
    try:
        exporter = RetailMetricsExporter(start_port=9100, max_port=9100)
        exporter.start()
    except KeyboardInterrupt:
        logging.info("Shutting down metrics exporter...")
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
    finally:
        if exporter:
            exporter.stop()

if __name__ == "__main__":
    main()