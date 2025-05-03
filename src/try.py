# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.common.keys import Keys
# from bs4 import BeautifulSoup
# from datetime import datetime
# import os
# import time
# import pandas as pd
# import config as config
# import undetected_chromedriver as uc

# DRIVER_PATH = 'C:/Users/User/Desktop/Project/data_project/car_scrape_pipeline/dags/chromedriver-win64/chromedriver.exe'
# options = Options()
# # driver = uc.Chrome(version_main=132)  # Match your installed Chrome version
# # options.add_argument("--headless")  # Run in headless mode
# # options.add_argument("--disable-blink-features=AutomationControlled")
# options.add_argument("--headless")
# options.add_argument("--window-size=1920,1200")
# options.add_argument("--no-sandbox")
# options.add_argument("--disable-gpu")
# options.add_argument("--disable-dev-shm-usage")
# options.add_argument("--disable-software-rasterizer")
# # driver = webdriver.Chrome(ChromeDriverManager().install())
# driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)
# # driver = uc.Chrome(version_main=132)  # Match your installed Chrome version

# mudah_data = []
# base_url = f"https://www.carlist.my/used-cars-for-sale/malaysia?sort=modification_date_search.desc"
# page = 1
# stop_page = 2

# while page < stop_page:
#     if page > 1:
#         base_url = f"{base_url}?o={page}"
    
#     driver.get(base_url)
#     time.sleep(13)
#     html = driver.page_source
#     print(driver.page_source)
#     driver.quit()
#     print('1')

#     # soup = BeautifulSoup(html, 'html.parser')
#     # print(soup.prettify())
#     # boxs = soup.find_all('div', class_='grid  grid--full  cf')
#     # for box in boxs:
#     #     title_link = box.find('div', class_='listing__content  grid__item  soft-half--sides  four-tenths  palm-one-whole relative').find('h2', class_='listing__title  epsilon  flushl').find('a',class_='ellipsize  js-ellipsize-text')
#     #     title_text = title_link.get_text()
#     #     print(title_text)
    
#     # if not soup:
#     #     print(f"Could not get the page = {page}")
#     # else:
#     #     boxs = soup.find_all('div', class_='sc-gGBfsJ MHECA')

#     #     for box in boxs:
#     #         title_link = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='flex flex-col').find('a',class_='sc-feJyhm ktLKgO')
#     #         if title_link:
#     #             title_text = title_link.get_text()
#     #         else:
#     #             title_text = 'N/A'

#     #         price_span = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='flex flex-col').find('div',class_='text-sm text-[#E21E30] font-bold')
#     #         if price_span:
#     #             car_price = price_span.get_text()
#     #         else:
#     #             car_price = 'N/A'

#     #         condition_link = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='gap-2 grid grid-cols-2 mt-2').find('div', attrs={'title': 'Condition'}).find('div', class_='flex items-center text-[11px] text-black font-normal')
#     #         if condition_link:                                                                                   
#     #             condition = condition_link.get_text()
#     #         else:
#     #             condition = 'N/A'
                
#     #         mileage_link = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='gap-2 grid grid-cols-2 mt-2').find('div', attrs={'title': 'Mileage'}).find('div', class_='flex items-center text-[11px] text-black font-normal')
#     #         if mileage_link:                                                                                   
#     #             mileage = mileage_link.get_text()
#     #         else:
#     #             mileage = 'N/A'
                
#     #         year_link = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='gap-2 grid grid-cols-2 mt-2').find('div', attrs={'title': 'Manufactured Year'}).find('div', class_='flex items-center text-[11px] text-black font-normal')
#     #         if year_link:                                                                                   
#     #             year_produce = year_link.get_text()
#     #         else:
#     #             year_produce = 'N/A'
                
#     #         capacity_link = box.find('div', class_='flex flex-col flex-1 gap-2 self-center').find('div', class_='gap-2 grid grid-cols-2 mt-2').find('div', attrs={'title': 'Engine capacity'}).find('div', class_='flex items-center text-[11px] text-black font-normal')
#     #         if capacity_link:                                                                                   
#     #             capacity = capacity_link.get_text()
#     #         else:
#     #             capacity = 'N/A'

#     #         date_link = box.find('div', class_='sc-kgAjT hUwTNT').find('div', class_='sc-cJSrbW bQToIk').find('span', class_='sc-ksYbfQ dEBMaz').find('span', class_='sc-hmzhuo eHBZJR')
#     #         if date_link:                                                                                   
#     #             date_upload = date_link.get_text()
#     #         else:
#     #             date_upload = 'N/A'
                
#     #         location_link = box.find('div', class_='sc-kgAjT hUwTNT').find('div', class_='sc-cJSrbW bQToIk').find('span', class_='sc-frDJqD etBKWa').find('span', class_='sc-kvZOFW fINWBN')
#     #         if location_link:                                                                                   
#     #             location_upload = location_link.get_text()
#     #         else:
#     #             location_upload = 'N/A'

#     #         # open other page
#     #         detail_link = box.find('div', class_='flex flex-col').find('a',class_='sc-feJyhm ktLKgO')
#     #         if detail_link:
#     #             detail_text = detail_link.get_text()
#     #         else:
#     #             detail_text = 'N/A'
        
#     page += 1

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Set up options for headless Chrome
options = Options()
options.headless = True  # Enable headless mode for invisible operation
options.add_argument("--window-size=1920,1200")  # Define the window size of the browser

# Set the path to the Chromedriver
DRIVER_PATH = '/path/to/chromedriver'

# Initialize Chrome with the specified options
driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)

# Navigate to the Nintendo website
driver.get("https://www.nintendo.com/")

# Output the page source to the console
print(driver.page_source)

# Close the browser session cleanly to free up system resources
driver.quit()