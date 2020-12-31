from bs4 import BeautifulSoup
import requests
import json
import csv

laptop_page_url = "https://tiki.vn/laptop/c8095?src=c.8095.hamburger_menu_fly_out_banner&_lc=&page={}"
product_url = "https://tiki.vn/api/v2/products/{}"
params = {'q':'goog'}
headers={'User-Agent': 'Mozilla/5.0'}

product_id_file = "./data/product-id.txt"
product_data_file = "./data/product.txt"
product_file = "./data/product.csv"

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
    db=env('DATABASE_NAME'))
cursor = mydb.cursor()

# Create Table
# cursor.execute('CREATE TABLE crawl_product(product_id int(20), sku varchar(50), product_title varchar(200), url_key varchar(200), url_path varchar(200), vendor varchar(20), short_desc varchar(1000), price int(20))')

def crawl_product_id():
    product_id_list = []
    i = 1
    while (i<2):
        print("Crawl page: ", i)
        response = requests.get(laptop_page_url.format(i), params=params, headers=headers)
        parser = BeautifulSoup(response.text, 'html.parser')
        product_box = parser.findAll(class_="product-item")

        if (len(product_box) == 0):
            break

        for product in product_box:
            href = product.get("href").rsplit("-p", 1)[1]
            product_id = href.split(".html")[0]
            product_id_list.append(product_id)

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
        response = requests.get(product_url.format(product_id), params=params, headers=headers)
        if (response.status_code == 200):
            product_detail_list.append(response.text)
            print("Crawl product: ", product_id, ": ", response.status_code)
    return product_detail_list

flatten_field = [ "badges", "inventory", "categories", "rating_summary",
                      "brand", "seller_specifications", "current_seller", "other_sellers",
                      "configurable_options",  "configurable_products", "specifications", "product_links",
                      "services_and_promotions", "promotions", "stock_item", "installment_info" ]

def adjust_product(product):
    e = json.loads(product)
    if not e.get("id", False):
        return None

    for field in flatten_field:
        if field in e:
            e[field] = json.dumps(e[field], ensure_ascii=False)

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

def save_product_list(product_json_list):
    file = open(product_file, "w", encoding="utf-8")
    csv_writer = csv.writer(file)

    count = 0
    for p in product_json_list:
        if p is not None:
            if count == 0:
                header = p.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(p.values())
    file.close()
    print("Save file: ", product_file)


# crawl product id
product_id_list, page = crawl_product_id()

print("No. Page: ", page)
print("No. Product ID: ", len(product_id_list))

# save product id for backup
save_product_id(product_id_list)

# crawl detail for each product id
product_list = crawl_product(product_id_list)

# save product detail for backup
save_raw_product(product_list)

# product_list = load_raw_product()
# flatten detail before converting to csv
product_json_list = [adjust_product(p) for p in product_list]
# save product to csv
# save_product_list(product_json_list)

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
    # product_id = validate_string(item.get("id", None))
    # sku = validate_string(item.get("sku", None))
    # product_title = validate_string(item.get("name", None))
    # url_key = validate_string(item.get("url_key", None))
    # url_path = validate_string(item.get("url_path", None))
    # vendor = "tiki"
    # type = validate_string(item.get("type", None))
    # short_desc = validate_string(item.get("short_description", None))
    # price = validate_string(item.get("price", None))
    product_id = item.get("id", None)
    sku = item.get("sku", None)
    product_title = item.get("name", None).encode('utf-8')
    url_key = item.get("url_key", None).encode('utf-8')
    url_path = item.get("url_path", None).encode('utf-8')
    vendor = "tiki"
    short_desc = item.get("short_description", None).encode('utf-8')
    price = item.get("price", None)

    cursor.execute('INSERT INTO crawl_product(product_id, sku, product_title, url_key, url_path, vendor, short_desc, price) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)', (product_id, sku, product_title, url_key, url_path, vendor, short_desc, price))
mydb.commit()
cursor.close()
