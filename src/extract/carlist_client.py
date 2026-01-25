import logging
import asyncio
from playwright.async_api import async_playwright
from src.common.logger import get_logger
from src.schemas.product import ProductSchema
from pydantic import ValidationError

logger = get_logger("carlist_extractor")

class carlistWebClient:
    def __init__(self, db_url):
        self.db_url = db_url

    def scrape_site(self):
        try:
            logger.info("Attempting to fetch data from website...")
            raw_data = self.fetch_html() # Hypothetical method
            
            validated_data = []
            for item in raw_data:
                try:
                    clean_item = ProductSchema(**item).dict()
                    validated_data.append(clean_item)
                except ValidationError as ve:
                    logger.warning(f"Row failed QC: {ve.json()}")
                    continue 

            return validated_data

        except Exception as e:
            logger.error(f"CRITICAL EXTRACTION ERROR: {str(e)}")
            raise  
    
# def extract_carlist():
#     carlist_data = []
#     date_extracted = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     page = 1
#     stop_page = 2

#     try:
#         while page < stop_page:
#             chrome_options = Options()
#             chrome_options.add_argument("--headless")
#             chrome_options.add_argument("--disable-gpu")
#             chrome_options.add_argument("--window-size=1920,1080")
#             chrome_options.add_argument("--disable-dev-shm-usage")
#             chrome_options.add_argument("--no-sandbox")
#             chrome_options.add_argument("--disable-blink-features=AutomationControlled")
#             chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
#             driver = uc.Chrome(version_main=132, options=chrome_options)
#             base_url = f"https://www.carlist.my/used-cars-for-sale/malaysia?sort=modification_date_search.desc"

#             if page > 1:
#                 base_url = f"{base_url}&page_number={page}&page_size=25"

#             driver.get(base_url)
#             time.sleep(10)
#             html = driver.page_source
#             driver.quit()
#             soup = BeautifulSoup(html, 'html.parser')
            
#             article_grids = soup.find_all("article")
#             for article_grid in article_grids:

#                 # get data from article
#                 title = article_grid.get("data-title", "N/A")
#                 brand = article_grid.get("data-make", "N/A")
#                 model = article_grid.get("data-model", "N/A")
#                 model_group = article_grid.get("data-model-group", "N/A")
#                 variant = article_grid.get("data-variant", "N/A")
#                 body_type = article_grid.get("data-body-type", "N/A")
#                 transmission = article_grid.get("data-transmission", "N/A")
#                 mileage = article_grid.get("data-mileage", "N/A")
#                 type = article_grid.get("data-ad-type", "N/A")
#                 manufactured = article_grid.get("data-year", "N/A")

#                 #get link to enter subpage
#                 subpage_chrome_options = Options()
#                 subpage_chrome_options.add_argument("--headless")
#                 subpage_chrome_options.add_argument("--disable-gpu")
#                 subpage_chrome_options.add_argument("--window-size=1920,1080")
#                 subpage_chrome_options.add_argument("--disable-dev-shm-usage")

#                 link = article_grid.get("data-url", "N/A")
#                 driver = uc.Chrome(version_main=132, options=subpage_chrome_options)
#                 driver.get(link)
#                 time.sleep(5)
#                 subpage_html = driver.page_source
#                 driver.quit()

#                 subpage_soup = BeautifulSoup(subpage_html, 'html.parser')

#                 find_price = subpage_soup.find('h3')
#                 price= find_price.text.strip()

#                 elements = subpage_soup.find_all('div', class_="c-card")
#                 capacity = elements[4].text.strip()
#                 capacity = capacity.split("\n")

#                 find_date = subpage_soup.find_all('span', class_="u-color-muted")
#                 data_upload = find_date[0].text.strip()            
                
#                 carlist_data.append({
#                             "title": title, 
#                             "brand": brand, 
#                             "model":model, 
#                             "model_group": model_group, 
#                             "variant": variant,
#                             "body_type": body_type, 
#                             "transmission": transmission, 
#                             "mileage": mileage, 
#                             "type": type,
#                             "capacity": capacity[1],
#                             "price": price,
#                             "manufactured": manufactured,
#                             "data_posted": data_upload,
#                             "date_extracted": date_extracted,
#                             "detail_link": link
#                     })

#             page += 1

#         return carlist_data
        
#     except FileNotFoundError as e:
#         print(e)
#     except Exception as e:
#         print(f"An error occurred: {e}") 

# def get_carlist():
#     carlist_data = get_carlist()
#     return carlist_data

