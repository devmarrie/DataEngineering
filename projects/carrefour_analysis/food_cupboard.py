from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests
import json
import time
import csv

def scrape_carrefour_products(base_url, file_name):
    current_page = 0
    full_file = f'data/{file_name}'

    # Prepare to write to csv
    with open(full_file, mode='w', newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(['ProductName', 'OriginalPrice', 'ApplicablePrice', 'Type', 'PercentageDiscount', 'Category']) # header

        while True:
            url = f"{base_url}&currentPage={current_page}"
            try:
                driver.get(url)
                
                # Wait until the content is fully loaded
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "__NEXT_DATA__"))
                )

                # Get the page source and parse with BeautifulSoup
                soup = BeautifulSoup(driver.page_source, 'lxml')
                script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
            
                if script_tag:
                    json_data = json.loads(script_tag.contents[0])

                    # Access the list of products
                    products = json_data['props']['initialState']['search']['products']

                    if not products:
                        print("No more products found, stopping the scraping.")
                        break

                    for product in products:
                        ProductName = product['name']
                        OriginalPrice = product['originalPrice']
                        ApplicablePrice = product['applicablePrice']
                        Type = product['type']
                        PercentageDiscount = f"{product['discount']['value']}"
                        Category = file_name.split('.csv')[0]
                        writer.writerow([ProductName, OriginalPrice, ApplicablePrice, Type, PercentageDiscount, Category]) # add the row to the csv file.
                        
                #This is where the trick is , we add a page after finding the products not outside
                current_page += 1
        

            except requests.RequestException as e:
                print(f"Request failed: {e}")
                break
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing JSON: {e}")
                break
if __name__ == "__main__":
    # Base URL
    base_url ='https://www.carrefour.ke/mafken/en/c/FKEN1760000?filter=&pageSize=60&sortBy=relevance'
    file_name = 'food_cupboard.csv'
    driver = webdriver.Chrome()

    scrape_carrefour_products(base_url, file_name)

    # Close the driver
    driver.quit()