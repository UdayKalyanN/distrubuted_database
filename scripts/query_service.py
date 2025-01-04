from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime, timedelta
import pandas as pd
import logging
from cassandra.policies import DCAwareRoundRobinPolicy

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class RetailDataQueryService:
    def __init__(self, cassandra_hosts=['localhost']):
        self.logger = logging.getLogger('RetailQueryService')
        
        # Initialize cluster with proper load balancing policy
        load_balancing_policy = DCAwareRoundRobinPolicy(local_dc='Europe_DC')
        self.cluster = Cluster(
            contact_points=cassandra_hosts,
            load_balancing_policy=load_balancing_policy,
            protocol_version=5
        )
        self.session = self.cluster.connect('retail_analytics')
        self.prepare_statements()

    def prepare_statements(self):
        """Prepare all statements once during initialization"""
        # Get all transactions for a month
        self.transactions_month_stmt = self.session.prepare("""
            SELECT *
            FROM retail_analytics.transactions_by_region
            WHERE year_month = ?
            ALLOW FILTERING
        """)

        # Get transactions for a specific country and month
        self.transactions_country_month_stmt = self.session.prepare("""
            SELECT *
            FROM retail_analytics.transactions_by_region
            WHERE country = ? AND year_month = ?
        """)

        # Get customer transactions
        self.customer_transactions_stmt = self.session.prepare("""
            SELECT *
            FROM retail_analytics.transactions_by_region
            WHERE year_month = ? AND customer_id = ?
            ALLOW FILTERING
        """)

        # Get product transactions
        self.product_transactions_stmt = self.session.prepare("""
            SELECT *
            FROM retail_analytics.transactions_by_region
            WHERE year_month = ? AND stock_code = ?
            ALLOW FILTERING
        """)

    def convert_to_pandas_df(self, rows):
        """Convert Cassandra rows to pandas DataFrame with proper type conversion"""
        df = pd.DataFrame(list(rows))
        if not df.empty:
            # Convert datetime columns
            if 'invoice_date' in df.columns:
                df['invoice_date'] = pd.to_datetime(df['invoice_date'])
            
            # Convert numeric columns
            if 'quantity' in df.columns:
                df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
            if 'unit_price' in df.columns:
                df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
                
        return df

    def execute_query(self, statement, parameters=None):
        """Execute a prepared statement with proper error handling"""
        try:
            if parameters is None:
                parameters = []
            rows = self.session.execute(statement, parameters)
            return self.convert_to_pandas_df(rows)
        except Exception as e:
            self.logger.error(f"Query execution error: {str(e)}")
            self.logger.error(f"Statement: {statement.query_string}")
            self.logger.error(f"Parameters: {parameters}")
            raise

    def get_sales_by_country(self, year_month):
        """Get total sales and revenue by country for a specific month"""
        try:
            df = self.execute_query(self.transactions_month_stmt, [year_month])
            if not df.empty:
                # Ensure numeric types
                df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
                df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
                
                # Calculate metrics
                result = df.groupby('country').agg({
                    'invoice_no': 'count',
                    'quantity': 'sum',
                    'unit_price': lambda x: (x * df.loc[x.index, 'quantity']).sum()
                }).rename(columns={
                    'invoice_no': 'total_sales',
                    'quantity': 'total_quantity',
                    'unit_price': 'total_revenue'
                }).reset_index()
                
                return result
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error in get_sales_by_country: {str(e)}")
            raise

    def get_regional_performance(self, year_month):
        """Get regional performance metrics"""
        try:
            df = self.execute_query(self.transactions_month_stmt, [year_month])
            if not df.empty:
                # Ensure numeric types
                df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
                df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
                
                # Calculate metrics
                result = df.groupby('country').agg({
                    'invoice_no': 'count',
                    'quantity': 'sum',
                    'unit_price': lambda x: (x * df.loc[x.index, 'quantity']).sum()
                }).rename(columns={
                    'invoice_no': 'transaction_count',
                    'quantity': 'total_items',
                    'unit_price': 'total_revenue'
                }).reset_index()
                
                return result
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error in get_regional_performance: {str(e)}")
            raise

    def get_top_products(self, year_month, limit=10):
        """Get top selling products for a specific month"""
        try:
            df = self.execute_query(self.transactions_month_stmt, [year_month])
            if not df.empty:
                # Ensure numeric types
                df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
                df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
                
                # Calculate metrics
                product_metrics = df.groupby(['stock_code', 'description']).agg({
                    'invoice_no': 'count',
                    'quantity': 'sum',
                    'unit_price': lambda x: (x * df.loc[x.index, 'quantity']).sum()
                }).rename(columns={
                    'invoice_no': 'sale_count',
                    'quantity': 'total_quantity',
                    'unit_price': 'total_revenue'
                }).reset_index()
                
                # Get top products
                return product_metrics.nlargest(limit, 'total_revenue')
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error in get_top_products: {str(e)}")
            raise

    def get_customer_metrics(self, year_month, customer_id):
        """Get customer metrics for a specific month"""
        try:
            return self.execute_query(self.customer_transactions_stmt, [year_month, customer_id])
        except Exception as e:
            self.logger.error(f"Error in get_customer_metrics: {str(e)}")
            raise

    def get_available_months(self):
        """Get list of available months"""
        try:
            query = """
            SELECT DISTINCT country, year_month 
            FROM retail_analytics.transactions_by_region
            """
            rows = self.session.execute(query)
            months = sorted(list(set(row.year_month for row in rows)))
            return months
        except Exception as e:
            self.logger.error(f"Error in get_available_months: {str(e)}")
            raise

    def get_latest_month(self):
        """Get the most recent month available"""
        try:
            months = self.get_available_months()
            return months[-1] if months else datetime.now().strftime('%Y-%m')
        except Exception as e:
            self.logger.error(f"Error getting latest month: {str(e)}")
            return '2010-12'

    def get_table_info(self):
        """Get information about the tables and sample data"""
        try:
            keyspace_query = """
            SELECT table_name 
            FROM system_schema.tables 
            WHERE keyspace_name = 'retail_analytics'
            """
            tables = self.session.execute(keyspace_query)
            
            print("\nAvailable tables:")
            for table in tables:
                table_name = table.table_name
                print(f"\nTable: {table_name}")
                
                columns_query = f"""
                SELECT column_name, type
                FROM system_schema.columns
                WHERE keyspace_name = 'retail_analytics'
                AND table_name = '{table_name}'
                """
                columns = self.session.execute(columns_query)
                print("Columns:")
                for col in columns:
                    print(f"  {col.column_name}: {col.type}")
                
                sample_query = f"SELECT * FROM retail_analytics.{table_name} LIMIT 5"
                sample_data = self.session.execute(sample_query)
                rows = list(sample_data)
                if rows:
                    print("\nSample data:")
                    df = self.convert_to_pandas_df(rows)
                    print(df)
                else:
                    print("No data in table")
                    
        except Exception as e:
            self.logger.error(f"Error getting table info: {str(e)}")
            raise

    def cleanup(self):
        """Cleanup resources"""
        try:
            if self.cluster:
                self.cluster.shutdown()
                self.logger.info("Cassandra connection closed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            raise

def main():
    query_service = RetailDataQueryService()
    try:
        print("\nChecking available data in tables...")
        query_service.get_table_info()

        months = query_service.get_available_months()
        print(f"\nAvailable months: {months}")
        
        latest_month = query_service.get_latest_month()
        print(f"\nAnalyzing data for month: {latest_month}")

        print("\nFetching sales by country...")
        sales_by_country = query_service.get_sales_by_country(latest_month)
        if not sales_by_country.empty:
            print("\nSales by Country:")
            print(sales_by_country)
        else:
            print("No sales data found")

        print("\nFetching regional performance...")
        regional_perf = query_service.get_regional_performance(latest_month)
        if not regional_perf.empty:
            print("\nRegional Performance:")
            print(regional_perf)
        else:
            print("No regional performance data found")

        print("\nFetching top products...")
        top_products = query_service.get_top_products(latest_month, limit=5)
        if not top_products.empty:
            print("\nTop 5 Products:")
            print(top_products)
        else:
            print("No product data found")

    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
    finally:
        query_service.cleanup()

if __name__ == "__main__":
    main()