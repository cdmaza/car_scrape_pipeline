import cloudscraper
from bs4 import BeautifulSoup
import csv
from fake_useragent import UserAgent
import time

# URL of the website
URL = "https://www.mudah.my/johor/cars-for-sale"

# Generate a random User-Agent
ua = UserAgent()
headers = {
    "User-Agent": ua.random,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1"
}

# Create a cloudscraper session
scraper = cloudscraper.create_scraper()
scraper.headers.update(headers)

# Add a delay to avoid being blocked
time.sleep(5)

# Attempt to fetch the webpage
response = scraper.get(URL)

if response.status_code == 200:
    # Specify the encoding explicitly
    response.encoding = 'utf-8'
    
    # Parse the HTML content of the page
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Print the HTML content for debugging
    print(soup.prettify())
    
    # sc-jlyJG fMNGsa
    results = soup.find('div', attrs = {'class_':'sc-jlyJG fMNGsa'})
    print(results)
    # # Check if the div was found
    if results:
        # Find all divs with the specified class
        cars = results.find_all("div", attrs = {'class_':'sc-gxMtzJ daQtBi'})
        
        if cars:
            for car in cars:
                title_element = car.find("a", attrs = {'class_':'sc-eXEjpC cPVpAp'}) #sc-bbmXgH exhqUY
                if title_element:
                    print(title_element.text)
                else:
                    print("Title element not found.")
        else:
            print("No cars found.")
    else:
        print("No results found with id 'sc-jlyJG'.")
else:
    print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
    print(response.text)  # Print the response content for debugging