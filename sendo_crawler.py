import requests
import json
import unicodecsv as csv

laptop_page_url = " https://mapi.sendo.vn/mob/product/cat/laptop?gtprice=7000000&p={}"
product_url = "https://mapi.sendo.vn/mob/product/{}/detail/"
params = {'q':'goog'}
headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

product_id_file = "./data/sendo_data/product-id.txt"
product_data_file = "./data/sendo_data/product.txt"
product_file = "./data/sendo_data/product.csv"

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
# cursor.execute('CREATE TABLE tiki_product(id int(20) NOT NULL AUTO_INCREMENT, product_id int(20), sku varchar(50), product_title varchar(200), url_key varchar(200), url_path varchar(200), vendor varchar(20), short_desc varchar(1000), price int(20), PRIMARY KEY (id))')
cursor.execute('DROP TABLE IF EXISTS `sendo_product`; CREATE TABLE sendo_product(id int(20) NOT NULL AUTO_INCREMENT, pid int(20), sku varchar(255), name varchar(255), url_key varchar(255), cat_path varchar(255), sales_page varchar(20), short_desc varchar(5000), price int(20), category varchar(255), brand varchar(255), shop_id int(20), shop_name varchar(255), PRIMARY KEY (id))')

def crawl_product_id():
    product_list = []
    i = 1
    while (True):
        print("Crawl page: ", i)
        print(laptop_page_url.format(i))
        response = requests.get(laptop_page_url.format(i), headers=headers)

        if (response.status_code != 200):
            break

        if response.text == "{}":
            break

        products = json.loads(response.text)["data"]

        if (len(products) == 0):
            break

        for product in products:
            product_id = str(product["id"])
            print("Product ID: ", product_id)
            product_list.append(product_id)

        i += 1

    return product_list, i

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

def adjust_product(product):
    e = json.loads(product)
    if not e.get("id", False):
        return None
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
    file = open(product_file, "wb")
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

# # crawl product id
# product_id_list, page = crawl_product_id()
#
# print("No. Page: ", page - 1)
# print("No. Product ID: ", len(product_id_list))
#
# # save product id for backup
# save_product_id(product_id_list)
#
# # crawl detail for each product id
# product_list = crawl_product(product_id_list)
#
# # save product detail for backup
# save_raw_product(product_list)

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

def validate_json(val):
    if val != None:
        return json.loads(val)

# json_obj = json.loads(product_json_list)
for i, item in enumerate(product_json_list):

    pid = validate_string(item['id'])
    print(pid)
    sku = validate_string(item.get("sku", None))
    name = validate_string(item.get("name", None))
    url_key = validate_string(item.get("url_key", None))
    cat_path = validate_string(item.get("cat_path", None))
    sales_page = "sendo"
    short_desc = validate_string(item.get("short_description", None)).replace('🎈🎈','')
    print(short_desc)
    price = validate_string(item['price'])
    if item['categories']:
        category = item['categories']['category_level1_name']
    else:
        category = item['category']
    brand = validate_string(item['brand_name'])
    shop_id = validate_string(item['shop_info']['shop_id'])
    shop_name = validate_string(item['shop_info']['shop_name'])
    cursor.execute(
        'INSERT INTO sendo_product(pid, sku, name, url_key, cat_path, sales_page, short_desc, price, category, brand, shop_id, shop_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
        (pid, sku, name, url_key, cat_path, sales_page, short_desc, price, category, brand, shop_id, shop_name))
    print("Done!")
mydb.commit()
cursor.close()
