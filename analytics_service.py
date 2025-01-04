import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
from query_service import RetailDataQueryService
from cassandra.query import SimpleStatement
import logging
import json
from functools import wraps
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def timer_decorator(func):
    """Decorator to measure function execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"{func.__name__} executed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper

class RetailAnalyticsService:
    def __init__(self, cache_enabled=True):
        self.logger = logging.getLogger('RetailAnalyticsService')
        self.query_service = RetailDataQueryService()
        self.cache_enabled = cache_enabled
        self._cache = {}
        self._cache_ttl = 3600  # 1 hour
        
        # Define RFM score weights
        self.rfm_weights = {
            'recency': 0.35,
            'frequency': 0.35,
            'monetary': 0.30
        }

    def _get_cached_data(self, key):
        """Get data from cache if available and not expired"""
        if not self.cache_enabled:
            return None
        
        cached_data = self._cache.get(key)
        if cached_data:
            timestamp, data = cached_data
            if time.time() - timestamp < self._cache_ttl:
                return data
        return None

    def _set_cached_data(self, key, data):
        """Set data in cache with timestamp"""
        if self.cache_enabled:
            self._cache[key] = (time.time(), data)

    @timer_decorator
    def perform_rfm_analysis(self, year_month=None):
        """Enhanced RFM Analysis with weighted scoring"""
        try:
            # Check cache first
            cache_key = f"rfm_analysis_{year_month}"
            cached_result = self._get_cached_data(cache_key)
            if cached_result is not None:
                return cached_result

            if year_month is None:
                year_month = self.query_service.get_latest_month()

            # Get transaction data
            query = """
            SELECT customer_id, invoice_date, quantity, unit_price
            FROM retail_analytics.transactions_by_region
            WHERE year_month = ?
            ALLOW FILTERING
            """
            rows = self.query_service.session.execute(
                self.query_service.session.prepare(query), 
                [year_month]
            )
            df = pd.DataFrame(list(rows))
            
            if df.empty:
                self.logger.warning(f"No data found for RFM analysis in month {year_month}")
                return pd.DataFrame()

            # Clean and prepare data
            df = df[df['customer_id'].notna()]
            df['invoice_date'] = pd.to_datetime(df['invoice_date'])
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
            df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce').fillna(0)
            df['transaction_value'] = df['quantity'] * df['unit_price']

            # Group by customer
            customer_metrics = df.groupby('customer_id').agg({
                'invoice_date': ['max', 'count'],  # last purchase and frequency
                'transaction_value': 'sum',        # total monetary value
                'quantity': 'sum'                  # total items
            }).reset_index()

            # Flatten column names
            customer_metrics.columns = ['customer_id', 'last_purchase', 'frequency', 'monetary', 'total_items']

            # Calculate recency
            end_date = pd.Timestamp(datetime.strptime(year_month, '%Y-%m') + pd.offsets.MonthEnd(0))
            customer_metrics['recency'] = (end_date - customer_metrics['last_purchase']).dt.days

            # Store original values before normalization
            customer_metrics['original_monetary'] = customer_metrics['monetary']
            customer_metrics['original_frequency'] = customer_metrics['frequency']
            customer_metrics['original_recency'] = customer_metrics['recency']

            # Normalize metrics for scoring
            for col in ['recency', 'frequency', 'monetary']:
                customer_metrics[col] = (customer_metrics[col] - customer_metrics[col].min()) / \
                    (customer_metrics[col].max() - customer_metrics[col].min())

            # Calculate weighted RFM score (invert recency as lower is better)
            customer_metrics['rfm_score'] = (
                (1 - customer_metrics['recency']) * self.rfm_weights['recency'] +
                customer_metrics['frequency'] * self.rfm_weights['frequency'] +
                customer_metrics['monetary'] * self.rfm_weights['monetary']
            )

            # Store normalized values
            customer_metrics['norm_recency'] = customer_metrics['recency']
            customer_metrics['norm_frequency'] = customer_metrics['frequency']
            customer_metrics['norm_monetary'] = customer_metrics['monetary']

            # Restore original values
            customer_metrics['recency'] = customer_metrics['original_recency']
            customer_metrics['frequency'] = customer_metrics['original_frequency']
            customer_metrics['monetary'] = customer_metrics['original_monetary']

            # Remove temporary columns
            customer_metrics = customer_metrics.drop(columns=['original_recency', 
                                                           'original_frequency', 
                                                           'original_monetary'])

            # Cache the result
            self._set_cached_data(cache_key, customer_metrics)
            
            return customer_metrics

        except Exception as e:
            self.logger.error(f"Error in RFM analysis: {str(e)}", exc_info=True)
            raise

    @timer_decorator
    def perform_customer_segmentation(self, rfm_df):
        """Perform customer segmentation using value-based scoring"""
        try:
            if rfm_df.empty:
                self.logger.warning("Empty dataframe provided for segmentation")
                return pd.DataFrame()

            # Create a value score based on RFM metrics
            df = rfm_df.copy()
            
            # Ensure numeric types
            numeric_cols = ['recency', 'frequency', 'monetary']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Calculate percentile ranks
            df['recency_rank'] = df['recency'].rank(pct=True)
            df['frequency_rank'] = df['frequency'].rank(pct=True)
            df['monetary_rank'] = df['monetary'].rank(pct=True)

            # Calculate value score (inverted recency rank because lower recency is better)
            df['value_score'] = (
                (1 - df['recency_rank']) * self.rfm_weights['recency'] +
                df['frequency_rank'] * self.rfm_weights['frequency'] +
                df['monetary_rank'] * self.rfm_weights['monetary']
            )

            # Assign segments based on value score quartiles
            df['Segment'] = pd.qcut(
                df['value_score'], 
                q=4, 
                labels=['At Risk', 'Low Value', 'Mid Value', 'High Value']
            )

            # Add customer status
            df['Customer_Status'] = df.apply(self._determine_customer_status, axis=1)
            
            # Add next best action
            df['Next_Best_Action'] = df.apply(self._recommend_next_action, axis=1)

            return df

        except Exception as e:
            self.logger.error(f"Error in customer segmentation: {str(e)}", exc_info=True)
            # Return original dataframe with default segmentation
            rfm_df['Segment'] = 'General'
            rfm_df['Customer_Status'] = 'Unknown'
            rfm_df['Next_Best_Action'] = 'Review Account'
            return rfm_df

    def _determine_customer_status(self, row):
        """Determine customer status based on RFM metrics"""
        try:
            # Get base metrics
            recency = float(row['recency'])
            frequency = float(row['frequency'])
            monetary = float(row['monetary'])

            # Calculate relative positions
            freq_percentile = row['norm_frequency'] if 'norm_frequency' in row else 0.5
            monetary_percentile = row['norm_monetary'] if 'norm_monetary' in row else 0.5

            # Define VIP threshold
            vip_threshold = monetary > 1000 and frequency > 5

            # Status determination logic
            if recency <= 7:  # Active within a week
                if vip_threshold:
                    return 'Active - VIP'
                elif freq_percentile > 0.75 or monetary_percentile > 0.75:
                    return 'Active - High Value'
                return 'Active - Regular'
                
            elif recency <= 30:  # Active within a month
                if vip_threshold:
                    return 'Active - VIP at Risk'
                elif freq_percentile > 0.75 or monetary_percentile > 0.75:
                    return 'Active - High Value'
                return 'Active - Regular'
                
            elif recency <= 90:  # Inactive within 3 months
                if monetary > 500:
                    return 'Recently Inactive - High Value'
                return 'Recently Inactive'
                
            else:  # Inactive more than 3 months
                if monetary > 1000:
                    return 'Inactive - High Value'
                elif monetary > 500:
                    return 'Inactive - Mid Value'
                return 'Inactive - Low Value'

        except Exception as e:
            self.logger.error(f"Error determining customer status: {str(e)}")
            return 'Status Unknown'

    def _recommend_next_action(self, row):
        """Recommend next best action based on customer segment and status"""
        try:
            status = row['Customer_Status']
            segment = row['Segment']
            monetary = float(row['monetary'])
            frequency = float(row['frequency'])

            if 'VIP' in status:
                if 'at Risk' in status:
                    return 'Urgent VIP Recovery Program'
                return 'Premium Service & Loyalty Rewards'
                
            elif 'High Value' in status:
                if 'Active' in status:
                    if frequency > 10:
                        return 'Loyalty Program Upgrade & Premium Offers'
                    return 'Enhanced Service & Cross-sell Premium Products'
                return 'Personalized Win-back Campaign'
                
            elif 'Active' in status:
                if segment in ['High Value', 'Mid Value']:
                    return 'Upsell & Engagement Enhancement'
                return 'Regular Engagement & Value Building'
                
            elif 'Recently Inactive' in status:
                if monetary > 500:
                    return 'High-Priority Re-engagement Campaign'
                return 'Standard Re-engagement Program'
                
            else:  # Inactive
                if monetary > 1000:
                    return 'Custom Recovery Strategy'
                elif monetary > 500:
                    return 'Targeted Reactivation Campaign'
                return 'General Reactivation Program'

        except Exception as e:
            self.logger.error(f"Error recommending next action: {str(e)}")
            return 'Review Account'

    @timer_decorator
    def analyze_sales_trends(self, year_month=None):
        """Enhanced sales trend analysis with advanced metrics"""
        try:
            if year_month is None:
                year_month = self.query_service.get_latest_month()

            # Get base sales data
            regional_data = self.query_service.get_regional_performance(year_month)
            if regional_data.empty:
                return pd.DataFrame()

            # Ensure numeric types
            numeric_cols = ['total_revenue', 'transaction_count', 'total_items']
            for col in numeric_cols:
                regional_data[col] = pd.to_numeric(regional_data[col], errors='coerce').fillna(0)

            # Calculate advanced metrics
            regional_data['avg_transaction_value'] = regional_data.apply(
                lambda x: x['total_revenue'] / x['transaction_count'] if x['transaction_count'] > 0 else 0,
                axis=1
            )
            regional_data['items_per_transaction'] = regional_data.apply(
                lambda x: x['total_items'] / x['transaction_count'] if x['transaction_count'] > 0 else 0,
                axis=1
            )
            regional_data['revenue_per_item'] = regional_data.apply(
                lambda x: x['total_revenue'] / x['total_items'] if x['total_items'] > 0 else 0,
                axis=1
            )

            # Calculate market share
            total_revenue = regional_data['total_revenue'].sum()
            regional_data['market_share'] = regional_data.apply(
                lambda x: (x['total_revenue'] / total_revenue * 100) if total_revenue > 0 else 0,
                axis=1
            )

            # Calculate performance score
            regional_data['performance_score'] = (
                regional_data['market_share'] * 0.4 +
                regional_data['avg_transaction_value'].rank(pct=True) * 0.3 +
                regional_data['items_per_transaction'].rank(pct=True) * 0.3
            )

            # Assign market categories based on performance score
            regional_data['market_category'] = pd.qcut(
                regional_data['performance_score'],
                q=4,
                labels=['Underperforming', 'Developing', 'Performing', 'Leading']
            ).astype(str)

            return regional_data

        except Exception as e:
            self.logger.error(f"Error in sales trend analysis: {str(e)}", exc_info=True)
            raise
    
    @timer_decorator
    def generate_detailed_insights(self, year_month=None):
        """Generate comprehensive business insights with advanced analytics"""
        try:
            if year_month is None:
                year_month = self.query_service.get_latest_month()

            insights = {
                'period': year_month,
                'summary_metrics': {},
                'customer_insights': {},
                'product_insights': {},
                'regional_insights': {},
                'recommendations': []
            }

            # Get and analyze sales trends
            sales_data = self.analyze_sales_trends(year_month)
            if not sales_data.empty:
                # Calculate high-level metrics
                insights['summary_metrics'] = {
                    'total_revenue': float(sales_data['total_revenue'].sum()),
                    'total_transactions': int(sales_data['transaction_count'].sum()),
                    'total_items': int(sales_data['total_items'].sum()),
                    'average_basket_size': float(sales_data['items_per_transaction'].mean()),
                    'average_transaction_value': float(sales_data['avg_transaction_value'].mean()),
                    'revenue_concentration': float(
                        sales_data.nlargest(3, 'total_revenue')['total_revenue'].sum() /
                        sales_data['total_revenue'].sum()
                    )
                }

                # Regional performance analysis
                insights['regional_insights'] = {
                    'market_performance': sales_data.to_dict('records'),
                    'market_categories': sales_data.groupby('market_category').size().to_dict(),
                    'leading_markets': sales_data[
                        sales_data['market_category'] == 'Leading'
                    ]['country'].tolist(),
                    'growth_opportunities': sales_data[
                        sales_data['market_category'].isin(['Developing', 'Underperforming'])
                    ]['country'].tolist()
                }

            # Customer analysis
            rfm_data = self.perform_rfm_analysis(year_month)
            if not rfm_data.empty:
                segmentation = self.perform_customer_segmentation(rfm_data)
                
                insights['customer_insights'] = {
                    'customer_segments': {
                        'total_customers': len(segmentation),
                        'segment_distribution': segmentation['Segment'].value_counts().to_dict(),
                        'status_distribution': segmentation['Customer_Status'].value_counts().to_dict(),
                        'avg_customer_value': float(segmentation['monetary'].mean()),
                        'customer_concentration': float(
                            segmentation.nlargest(10, 'monetary')['monetary'].sum() /
                            segmentation['monetary'].sum()
                        )
                    },
                    'top_customers': [
                        {
                            'customer_id': row['customer_id'],
                            'total_value': float(row['monetary']),
                            'segment': row['Segment'],
                            'status': row['Customer_Status'],
                            'recommended_action': row['Next_Best_Action']
                        }
                        for _, row in segmentation.nlargest(5, 'monetary').iterrows()
                    ],
                    'retention_metrics': {
                        'active_customers': len(segmentation[
                            segmentation['Customer_Status'].str.startswith('Active')
                        ]),
                        'at_risk_customers': len(segmentation[
                            segmentation['Customer_Status'] == 'Recently Inactive'
                        ]),
                        'churned_customers': len(segmentation[
                            segmentation['Customer_Status'].str.startswith('Inactive')
                        ])
                    }
                }

            # Generate recommendations
            insights['recommendations'] = self._generate_recommendations(insights)

            return insights

        except Exception as e:
            self.logger.error(f"Error generating insights: {str(e)}", exc_info=True)
            raise

    def _generate_recommendations(self, insights):
        """Generate business recommendations based on insights"""
        recommendations = []

        # Market recommendations
        if 'regional_insights' in insights:
            market_data = insights['regional_insights']
            if len(market_data.get('growth_opportunities', [])) > 0:
                recommendations.append({
                    'category': 'Market Expansion',
                    'priority': 'High',
                    'recommendation': f"Focus on market development in: {', '.join(market_data['growth_opportunities'][:3])}",
                    'rationale': "These markets show potential for growth based on performance metrics"
                })

        # Customer recommendations
        if 'customer_insights' in insights:
            customer_data = insights['customer_insights']['customer_segments']
            if customer_data.get('customer_concentration', 0) > 0.5:
                recommendations.append({
                    'category': 'Customer Risk',
                    'priority': 'High',
                    'recommendation': "Implement customer diversification strategy",
                    'rationale': "High revenue concentration in top customers poses business risk"
                })

        # Add more recommendations based on other metrics...
        return recommendations

    def cleanup(self):
        """Cleanup resources and cache"""
        try:
            if hasattr(self, 'query_service'):
                self.query_service.cleanup()
            self._cache.clear()
            self.logger.info("Successfully cleaned up resources and cache")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}", exc_info=True)
            raise

def main():
    analytics_service = RetailAnalyticsService(cache_enabled=True)
    try:
        latest_month = '2010-12'  # Using known month with data
        print(f"\nAnalyzing data for month: {latest_month}")

        # Generate detailed insights
        insights = analytics_service.generate_detailed_insights(latest_month)
        
        # Print formatted insights
        print("\nBusiness Performance Summary:")
        print("=" * 50)
        for key, value in insights['summary_metrics'].items():
            print(f"{key.replace('_', ' ').title()}: {value:,.2f}")

        print("\nRegional Performance:")
        print("=" * 50)
        for key, value in insights['regional_insights'].items():
            if key == 'market_performance':
                df = pd.DataFrame(value)
                print("\nMarket Performance Metrics:")
                print(df[['country', 'total_revenue', 'market_share', 'market_category']])
            else:
                print(f"\n{key.replace('_', ' ').title()}:")
                print(value)

        print("\nCustomer Insights:")
        print("=" * 50)
        customer_data = insights['customer_insights']['customer_segments']
        print("\nSegment Distribution:")
        for segment, count in customer_data['segment_distribution'].items():
            print(f"{segment}: {count}")
        print("\nCustomer Status:")
        for status, count in customer_data['status_distribution'].items():
            print(f"{status}: {count}")

        print("\nTop Customer Analysis:")
        print("=" * 50)
        for customer in insights['customer_insights']['top_customers']:
            print(f"\nCustomer ID: {customer['customer_id']}")
            print(f"Total Value: ${customer['total_value']:,.2f}")
            print(f"Segment: {customer['segment']}")
            print(f"Status: {customer['status']}")
            print(f"Recommended Action: {customer['recommended_action']}")

        print("\nBusiness Recommendations:")
        print("=" * 50)
        for idx, rec in enumerate(insights['recommendations'], 1):
            print(f"\n{idx}. {rec['category']} (Priority: {rec['priority']})")
            print(f"Recommendation: {rec['recommendation']}")
            print(f"Rationale: {rec['rationale']}")

    except Exception as e:
        logging.error(f"Error in main: {str(e)}", exc_info=True)
    finally:
        analytics_service.cleanup()

if __name__ == "__main__":
    main()