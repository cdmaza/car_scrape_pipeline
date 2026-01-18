
# def get_carlist():
#     print("Fetching data from Carlist...")
#     return extract_carlist()

# def get_carsome():
#     print("Fetching data from Carsome...")
#     return extract_carsome()


# def quality_check(carlist_data=None, carsome_data=None,
#                   historical_avg_carlist=None, historical_avg_carsome=None) -> Dict[str, Any]:
#     """
#     Perform comprehensive quality checks on scraped car data from multiple sources.

#     Args:
#         carlist_data: List of dictionaries or DataFrame from Carlist scraping
#         carsome_data: List of dictionaries or DataFrame from Carsome/Mudah scraping
#         historical_avg_carlist: Historical average record count for Carlist
#         historical_avg_carsome: Historical average record count for Carsome

#     Returns:
#         Dictionary containing quality check results for all data sources
#     """

#     # Configure logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
#     logger = logging.getLogger(__name__)

#     results = {}
#     checker = DataQualityChecker()

#     # Quality check for Carlist data
#     if carlist_data is not None:
#         logger.info("Starting quality check for Carlist data...")

#         # Convert to DataFrame if it's a list
#         if isinstance(carlist_data, list):
#             carlist_df = pd.DataFrame(carlist_data)
#         else:
#             carlist_df = carlist_data

#         # Define expected schema for Carlist
#         carlist_schema = {
#             'title': str,
#             'brand': str,
#             'model': str,
#             'model_group': str,
#             'variant': str,
#             'body_type': str,
#             'transmission': str,
#             'mileage': str,
#             'type': str,
#             'capacity': str,
#             'price': str,
#             'manufactured': str,
#             'data_posted': str,
#             'date_extracted': str,
#             'detail_link': str
#         }

#         # Run quality checks
#         carlist_results = checker.quality_check(
#             data=carlist_df,
#             schema=carlist_schema,
#             data_source_url='https://www.carlist.my/used-cars-for-sale/malaysia',
#             historical_avg_records=historical_avg_carlist,
#             record_count_tolerance=0.3,  # 30% tolerance
#             timestamp_column='date_extracted',
#             freshness_threshold_hours=24,
#             required_fields=['title', 'brand', 'model', 'price', 'detail_link'],
#             unique_fields=['detail_link'],
#             id_fields=['detail_link']
#         )

#         results['carlist'] = carlist_results

#         # Log results
#         if carlist_results['overall_passed']:
#             logger.info(f"✓ Carlist quality check PASSED - Score: {carlist_results['overall_quality_score']:.1f}%")
#         else:
#             logger.warning(f"✗ Carlist quality check FAILED - Score: {carlist_results['overall_quality_score']:.1f}%")
#             logger.warning(f"Summary: {carlist_results['summary']}")

#     # Quality check for Carsome/Mudah data
#     if carsome_data is not None:
#         logger.info("Starting quality check for Carsome/Mudah data...")

#         # Convert to DataFrame if it's a list
#         if isinstance(carsome_data, list):
#             carsome_df = pd.DataFrame(carsome_data)
#         else:
#             carsome_df = carsome_data

#         # Define expected schema for Carsome/Mudah
#         carsome_schema = {
#             'title': str,
#             'price': str,
#             'condition': str,
#             'mileage': str,
#             'year_produce': str,
#             'engine_capacity': str,
#             'date_upload': str,
#             'location_upload': str,
#             'detail_link': str
#         }

#         # Run quality checks
#         carsome_results = checker.quality_check(
#             data=carsome_df,
#             schema=carsome_schema,
#             data_source_url='https://www.mudah.my/malaysia/cars-for-sale',
#             historical_avg_records=historical_avg_carsome,
#             record_count_tolerance=0.3,  # 30% tolerance
#             timestamp_column='date_upload',
#             freshness_threshold_hours=72,  # 3 days for uploaded listings
#             required_fields=['title', 'price', 'detail_link'],
#             unique_fields=['detail_link'],
#             id_fields=['detail_link']
#         )

#         results['carsome'] = carsome_results

#         # Log results
#         if carsome_results['overall_passed']:
#             logger.info(f"✓ Carsome/Mudah quality check PASSED - Score: {carsome_results['overall_quality_score']:.1f}%")
#         else:
#             logger.warning(f"✗ Carsome/Mudah quality check FAILED - Score: {carsome_results['overall_quality_score']:.1f}%")
#             logger.warning(f"Summary: {carsome_results['summary']}")

#     # Generate overall summary
#     if results:
#         total_score = sum(r['overall_quality_score'] for r in results.values()) / len(results)
#         all_passed = all(r['overall_passed'] for r in results.values())

#         results['overall'] = {
#             'average_quality_score': total_score,
#             'all_checks_passed': all_passed,
#             'timestamp': datetime.now().isoformat(),
#             'sources_checked': list(results.keys())
#         }

#         logger.info(f"\n{'='*60}")
#         logger.info(f"OVERALL QUALITY CHECK SUMMARY")
#         logger.info(f"{'='*60}")
#         logger.info(f"Average Quality Score: {total_score:.1f}%")
#         logger.info(f"All Checks Passed: {all_passed}")
#         logger.info(f"Sources Checked: {', '.join([k for k in results.keys() if k != 'overall'])}")
#         logger.info(f"{'='*60}\n")

#     return results

# def log_extract():
#     # Basic configuration
#     logging.basicConfig(level=logging.INFO)

#     # Example logs
#     logging.debug("Debug message (only visible if level=DEBUG)")
#     logging.info("Informational message")
#     logging.warning("Warning!")
#     logging.error("An error occurred")
#     logging.critical("Critical failure")


#     import pandas as pd
# from sqlalchemy import create_engine
# from bs4 import BeautifulSoup
# import requests

# class WebClient:
#     def __init__(self, db_url):
#         self.engine = create_engine(db_url)

#     def scrape_and_load(self):
#         # 1. Extract
#         url = "https://example.com/products"
#         response = requests.get(url)
#         soup = BeautifulSoup(response.text, 'html.parser')
        
#         products = []
#         for item in soup.find_all('div', class_='product'):
#             products.append({
#                 "product_name": item.find('h2').text,
#                 "price_raw": item.find('span').text, # e.g., "$1,200.00"
#                 "scraped_at": pd.Timestamp.now()
#             })
        
#         # 2. Load to Postgres (Raw Zone)
#         df = pd.DataFrame(products)
#         df.to_sql('raw_products', self.engine, schema='raw', if_exists='append', index=False)
#         print("Data loaded to raw.raw_products")