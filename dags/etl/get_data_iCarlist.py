import cloudscraper
from bs4 import BeautifulSoup
import csv
from fake_useragent import UserAgent
import time
import requests

ua = UserAgent()

headers = {
    "User-Agent": ua.random,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Referer": "https://carro.co/",
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "Windows",
    "Upgrade-Insecure-Requests": "1"
}

def tryjer(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    print(soup)


def scrape():
    scraper = cloudscraper.create_scraper()
    scraper.headers.update(headers)

    time.sleep(5)

    response = scraper.get(URL)

    if response.status_code == 200:
        response.encoding = 'utf-8'
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # print(soup.prettify())
        
        results = soup.find('section', attrs = {'class':'D_Io'})

        i=0
        # print(results)
        # # Check if the div was found
        if results:
            # Find all divs with the specified class
            cars = results.find_all("div", attrs = {'class':'D_la D_or'})
            
            if cars:
                for car in cars:
                    title_element = car.find("p", attrs = {'class':'D_jY D_jZ D_ke D_kh D_kk D_km D_ki D_kv'}) #sc-bbmXgH exhqUY
                    if title_element:
                        i+=1
                        print(title_element.text)
                    else:
                        print("Title element not found.")
            else:
                print("No cars found.")
        else:
            print("No results f  ound with id 'sc-jlyJG'.")
    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
        print(response.text)  # Print the response content for debugging

    print(i)

if __name__ == '__main__':
    URL = "https://carro.co/my/en?utm_source=google&utm_medium=sem&utm_campaign=2024_PM_GG_MY_WEB_BUY_SEM_LEADS_Generic-BuyUsedCars_AlwaysOn&utm_content=Retail_Search_ENG_General_Carro_693302625813&utm_term=car%20dealerships%20near%20me&gad_source=1&gclid=Cj0KCQiAs5i8BhDmARIsAGE4xHz9-ZCDugvj-2ERMmUZHiPcBx0SQTrjGW9BMDv6IbZ1UtjCNOb2FdYaAgQhEALw_wcB"
    # URL = "https://www.carousell.com.my/categories/cars-32/?searchId=lDfBJN"
    tryjer(URL)
    # scrape()