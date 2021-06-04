from bs4 import BeautifulSoup
import requests
import json
import unicodecsv as csv
from selenium import webdriver
from scrapy.http import HtmlResponse
from selenium.webdriver.chrome.options import Options

laptop_page_url = "https://shopee.vn/Laptop-cat.13030.13065?order=desc&sortBy=price&page={}"
# laptop_page_url = "https://shopee.vn/Laptop-cat.13030.13065?brands=131770&maxPrice=20000000&minPrice=5000000&page={}"
# laptop_page_url = "https://tiki.vn/dien-thoai-smartphone/c1795?src=c.1789.hamburger_menu_fly_out_banner{}"
product_url = "https://shopee.vn/api/v2/item/get?itemid={}&shopid={}"
params = {'q':'goog'}
headers={'User-Agent': 'Mozilla/5.0'}
# Them vao header "if-none-match-": "55b03-1c978cd3e7aa24bda774046ac69f692d"
options = Options()
options.headless = True
options.add_argument("window-size=1920,1080"
                     +"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                     +"AppleWebKit/537.36 (KHTML, like Gecko)"
                     +"Chrome/87.0.4280.141 Safari/537.36")
prefs = {"profile.managed_default_content_settings.images": 2}
options.add_experimental_option("prefs", prefs)

product_id_file = "./data/shopee-data/product-id.txt"
product_data_file = "./data/shopee-data/product.txt"
product_file = "./data/shopee-data/product.csv"

import MySQLdb
import environ

# Reading .env file
env = environ.Env()
environ.Env.read_env()

# Connect to SQL Server
mydb = MySQLdb.connect(
    host=env('DATABASE_HOST'),
    user=env('DATABASE_USER'),
    passwd=env('DATABASE_PASSWORD'),
    db=env('DATABASE_NAME'),
    use_unicode=True, charset="utf8")
cursor = mydb.cursor()

# Create Table
cursor.execute('DROP TABLE IF EXISTS `shopee_product`; CREATE TABLE shopee_product(id int(20) NOT NULL AUTO_INCREMENT, product_id int(20), sku varchar(50), product_title varchar(200), url_key varchar(200), url_path varchar(200), vendor varchar(20), short_desc varchar(1000), price int(20), PRIMARY KEY (id))')

product_link_list = []

def crawl_product_id():
    product_id_list = []
    i = 1
    while (i<3):
        driver = webdriver.Chrome("C:/bin/chromedriver.exe", chrome_options=options)
        driver.get(laptop_page_url.format(i))
        if "https://shopee.vn/Laptop-cat.13030.13065" in laptop_page_url.format(i):
            y = 2300
            x = 1
            while y <= 4800:
                driver.execute_script("window.scrollTo(0, " + str(y) + ")")
                y += 1000
                # print("aaaaaaaaaaa")
                # try:
                #     print("bbbbbbb" ,WebDriverWait(driver, 1).until(EC.presence_of_element_located(
                #         (By.XPATH, '//*[@class="row shopee-search-item-result__items"]/div[{}]/div/a/div/div[2]/div[1]/div'.format({x})))))
                #     print("Page is ready!")
                # except TimeoutException:
                #     print("cccccccc")
                #     print("Loading took too much time!")
                x += 10
            body = driver.page_source
            abc = driver.current_url
            response = HtmlResponse(abc, body=body, encoding='utf8')
            print(body)
            if (response == None):
                break

            for product in response.css("div.col-xs-2-4.shopee-search-item-result__item"):
                try:
                    url = product.css("div a::attr(href)").get()
                    print("link ok: ", url)

                    product_key = url.rsplit("-i.", 1)[1]
                    # product_id_dict = {"shop_id": product_key[0], "item_id": product_key[1]}
                    # shop_id = product_key[0]
                    # item_id = product_key[1]
                # parser = BeautifulSoup(body, 'html.parser')
                # product_box = parser.findAll(class_="col-xs-2-4 shopee-search-item-result__item", )
                # if (len(product_box) == 0):
                #     break
                # print(product_box[0])
                # for product in product_box:
                #     # href = product.get("href").rsplit("-i.", 1)[1]
                #     # product_id = href.split(".html")[0]
                #     product_id = product.get("div a::attr(href)")
                #     # product_id = product.css("div a::attr(href)").get()
                #     # product_id = product.get("href")
                    product_id_list.append(product_key)
                except:
                    print("no!")
        driver.close()
        print("Crawl page: ", i)
        print(product_id_list)
        # response = requests.get(laptop_page_url.format(i), params=params, headers=headers)
        # parser = BeautifulSoup(response.text, 'html.parser')
        # # print(response.content)
        # product_box = parser.findAll('a', class_="col-xs-2-4 shopee-search-item-result__item")
        #
        # if (len(product_box) == 0):
        #     break
        #
        # for product in product_box:
        #     href = product.get("href")
        #     print(href)

        i += 1

    return product_id_list, i

def save_product_id(product_id_list=[]):
    file = open(product_id_file, "w+")
    str = "\n".join(product_id_list)
    file.write(str)
    file.close()
    print("Save file: ", product_id_file)

def crawl_product(product_id_list=[]):
    product_detail_list = []
    for product_id in product_id_list:
        product_key = product_id.split(".")
        shop_id = product_key[0]
        item_id = product_key[1]
        response = requests.get(product_url.format(item_id, shop_id), params=params, headers=headers)
        if (response.status_code == 200):
            product_detail_list.append(response.text)
            print("Crawl product: ", product_id, ": ", response.status_code)
    return product_detail_list

def adjust_product(product):
    e = json.loads(product)
    return e

def save_raw_product(product_detail_list=[]):
    file = open(product_data_file, "w+")
    str = "\n".join(product_detail_list)
    file.write(str)
    file.close()
    print("Save file: ", product_data_file)

def load_raw_product():
    file = open(product_data_file, "r")
    return file.readlines()

product_id_list, page = crawl_product_id()

print("No. Page: ", page - 1)
print("No. Product ID: ", len(product_id_list))

# save product id for backup
save_product_id(product_id_list)

# crawl detail for each product id
product_list = crawl_product(product_id_list)

# save product detail for backup
save_raw_product(product_list)

product_list = load_raw_product()
# flatten detail before converting to csv
product_json_list = [adjust_product(p) for p in product_list]

# do validation and checks before insert
def validate_string(val):
   if val != None:
        if type(val) is int:
            #for x in val:
            #   print(x)
            return str(val).encode('utf-8')
        else:
            return val

# json_obj = json.loads(product_json_list)
for i, item in enumerate(product_json_list):
    print(i)
    product_id = validate_string(item['item']['itemid'])
    # sku = validate_string(item.get("sku", None))
    product_title = validate_string(item['item']['name'])
    # url_key = product_id_list[i]
    url_key = "key"
    url_path = "https://shopee.vn" + url_key
    vendor = "shopee"
    # short_desc = validate_string(item['item']['description'])
    short_desc = "description"
    price = validate_string(item['item']['price']/1E5)
    # thumbnail_url = validate_string((item.get("thumbnail_url", None)))
    print("STT: ", i)
    print(product_id)
#     cursor.execute(
#         'INSERT INTO crawl_product(product_id, product_title, url_key, url_path, vendor, short_desc, price) VALUES (%s,%s,%s,%s,%s,%s,%s)',
#         (product_id, product_title, url_key, url_path, vendor, short_desc, price))
#     #     print("Done!")
# mydb.commit()
# cursor.close()
