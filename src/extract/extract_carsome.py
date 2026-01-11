from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from datetime import datetime
import os
import time
import pandas as pd
import config as config
import undetected_chromedriver as uc

chrome_options = Options()
# options = uc.ChromeOptions()
# options.add_argument("--headless")  # Run in headless mode
# options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--headless") 
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-software-rasterizer")
driver = webdriver.Chrome(ChromeDriverManager().install())
# driver = uc.Chrome(options=options)

def extract_carsome():
    mudah_data = []
    base_url = f"https://www.mudah.my/malaysia/cars-for-sale"
    page = 1
    stop_page = 2

    try:         
        while page < stop_page:
            if page > 1:
                base_url = f"{base_url}?o={page}"
            # try:
            driver.get(base_url)
            time.sleep(13)


    #             actions = ActionChains(driver)
    #             actions.move_by_offset(10, 10).perform()  # Move mouse
    #             driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  # Scroll down
    #             time.sleep(5)  # Wait for page to load  

    #             body = driver.find_element(By.TAG_NAME, "body")
    #             print("Page Content:", body.text)

    #         finally:
    #             driver.quit()

            html = driver.page_source
            driver.quit()
            print('1')

            soup = BeautifulSoup(html, 'html.parser')
            print(soup.prettify())
            if not soup:
                print(f"Could not get the page = {page}")
            else:
                boxs = soup.find_all('div', class_='sc-gGBfsJ MHECA')

                for box in boxs:
                    title_link = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='flex flex-col').find('a',class_='sc-feJyhm ktLKgO')
                    if title_link:
                        title_text = title_link.get_text()
                    else:
                        title_text = 'N/A'

                    price_span = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='flex flex-col').find('div',class_='text-sm text-[#E21E30] font-bold')
                    if price_span:
                        car_price = price_span.get_text()
                    else:
                        car_price = 'N/A'

                    condition_link = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='gap-2 grid grid-cols-2 mt-2').find('div', attrs={'title': 'Condition'}).find('div', class_='flex items-center text-[11px] text-black font-normal')
                    if condition_link:                                                                                   
                        condition = condition_link.get_text()
                    else:
                        condition = 'N/A'
                        
                    mileage_link = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='gap-2 grid grid-cols-2 mt-2').find('div', attrs={'title': 'Mileage'}).find('div', class_='flex items-center text-[11px] text-black font-normal')
                    if mileage_link:                                                                                   
                        mileage = mileage_link.get_text()
                    else:
                        mileage = 'N/A'
                        
                    year_link = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='gap-2 grid grid-cols-2 mt-2').find('div', attrs={'title': 'Manufactured Year'}).find('div', class_='flex items-center text-[11px] text-black font-normal')
                    if year_link:                                                                                   
                        year_produce = year_link.get_text()
                    else:
                        year_produce = 'N/A'
                        
                    capacity_link = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='gap-2 grid grid-cols-2 mt-2').find('div', attrs={'title': 'Engine capacity'}).find('div', class_='flex items-center text-[11px] text-black font-normal')
                    if capacity_link:                                                                                   
                        capacity = capacity_link.get_text()
                    else:
                        capacity = 'N/A'

                    date_link = box.find('div', class_='sc-kgAjT hUwTNT').find('div', class_='sc-cJSrbW bQToIk').find('span', class_='sc-ksYbfQ dEBMaz').find('span', class_='sc-hmzhuo eHBZJR')
                    if date_link:                                                                                   
                        date_upload = date_link.get_text()
                    else:
                        date_upload = 'N/A'
                        
                    location_link = box.find('div', class_='sc-kgAjT hUwTNT').find('div', class_='sc-cJSrbW bQToIk').find('span', class_='sc-frDJqD etBKWa').find('span', class_='sc-kvZOFW fINWBN')
                    if location_link:                                                                                   
                        location_upload = location_link.get_text()
                    else:
                        location_upload = 'N/A'

                    # open other page
                    detail_link = box.find('div', class_='flex flex-col').find('a',class_='sc-feJyhm ktLKgO')
                    if detail_link:
                        detail_text = detail_link.get_text()
                    else:
                        detail_text = 'N/A'

                    mudah_data.append({
                        "title": title_text, 
                        "price": car_price, 
                        "condition":condition, 
                        "mileage": mileage, 
                        "year_produce": year_produce,
                        "engine_capacity": capacity, 
                        "date_upload": date_upload, 
                        "location_upload": location_upload, 
                        "location_upload": location_upload, 
                        "detail_link": detail_text
                        })
        
            page += 1

        mudah_df = pd.DataFrame(mudah_data)
        
        print(mudah_df.head())
        return mudah_df
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}")    

if __name__ == "__main__":
    get_mudah()