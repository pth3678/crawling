from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv
from hdfs import InsecureClient

chrome_driver_path='/home/pth/Downloads/chromedriver-linux64/chromedriver'

options=Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

service=Service(chrome_driver_path)
driver = webdriver.Chrome(service=service,options=options)

shop_url= 'https://www.lazada.vn/catalog/?q=hoodie'
driver.get(shop_url)

WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.Bm3ON')))
scroll_pause_time = 2
last_height = driver.execute_script("return document.body.scrollHeight")

while True:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(scroll_pause_time)
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height:
        break
    last_height = new_height
products = driver.find_elements(By.CSS_SELECTOR, '.Bm3ON')
sp=[]
for product in products:
    try:
        name = product.find_element(By.CSS_SELECTOR, '.RfADt [title]').text 
        price = product.find_element(By.CSS_SELECTOR, '.aBrP0').text
        sold = product.find_element(By.CSS_SELECTOR, '._1cEkb').text
        sp.append({'name':f'{name}','price':f'{price}', 'sold':f'{sold}'})
    except:
        continue
driver.quit()
# print(sp)
with open('products.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['name', 'price' ,'sold']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for product in sp:
        writer.writerow(product)

client = InsecureClient('http://localhost:9870', user='pth')
client.delete('/home/pth/desktop/crawling/products.csv',recursive=False)
client.upload('/home/pth/desktop/crawling/', 'products.csv')

