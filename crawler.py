from bs4 import BeautifulSoup
import requests
import json
import unicodecsv as csv

# laptop_page_url = "https://tiki.vn/laptop/c8095?src=c.8095.hamburger_menu_fly_out_banner&_lc=&page={}"
laptop_page_url = "https://tiki.vn/api/v2/products?limit=48&include=advertisement&aggregations=1&category=8095&page={}&urlKey=laptop"
#laptop_page_url = "https://tiki.vn/api/v2/products?limit=48&include=advertisement&aggregations=1&category=8095&page={}&urlKey=laptop&brand=18805&&price=10000000%2C25000000"
# laptop_page_url = "https://tiki.vn/dien-thoai-smartphone/c1795?src=c.1789.hamburger_menu_fly_out_banner{}"
product_url = "https://tiki.vn/api/v2/products/{}"
params = {'q':'goog'}
headers = {"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"}

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
    db=env('DATABASE_NAME'),
    use_unicode=True, charset="utf8")
cursor = mydb.cursor()

# Create Table
# cursor.execute('CREATE TABLE tiki_product(id int(20) NOT NULL AUTO_INCREMENT, product_id int(20), sku varchar(50), product_title varchar(200), url_key varchar(200), url_path varchar(200), vendor varchar(20), short_desc varchar(1000), price int(20), PRIMARY KEY (id))')
cursor.execute('DROP TABLE IF EXISTS `tiki_product`; CREATE TABLE tiki_product(id int(20) NOT NULL AUTO_INCREMENT, pid int(20), sku varchar(255), name varchar(255), url_key varchar(255), url_path varchar(255), sales_page varchar(20), short_desc varchar(5000), price int(20), category varchar(255), brand varchar(255), store_id int(20), store_name varchar(255), PRIMARY KEY (id))')

# def crawl_product_id():
#     product_id_list = []
#     i = 1
#     while (i<3):
#         print("Crawl page: ", i)
#         print(laptop_page_url.format(i))
#         response = requests.get(laptop_page_url.format(i), params=params, headers=headers)
#         parser = BeautifulSoup(response.content, 'html.parser')
#         product_box = parser.findAll(class_="product-item")
#
#         if (len(product_box) == 0):
#             break
#
#         for product in product_box:
#             href = product.get("href").rsplit("-p", 1)[1]
#             product_id = href.split(".html")[0]
#             product_id_list.append(product_id)
#
#         i += 1
#
#     return product_id_list, i


def crawl_product_id():
    product_list = []
    i = 1
    while (True):
        print("Crawl page: ", i)
        print(laptop_page_url.format(i))
        response = requests.get(laptop_page_url.format(i), headers=headers)

        if (response.status_code != 200):
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
# for pj in product_json_list:
#     print(pj)
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

def validate_json(val):
    if val != None:
        return json.loads(val)

# json_obj = json.loads(product_json_list)
for i, item in enumerate(product_json_list):

    sku = validate_string(item.get("sku", None))
    name = validate_string(item.get("name", None))
    url_key = validate_string(item.get("url_key", None))
    url_path = validate_string(item.get("url_path", None))
    sales_page = "tiki"
    short_desc = validate_string(item.get("short_description", None))
    category = validate_json(validate_string(item['categories']))['name']
    brand = validate_json(validate_string(item['brand']))['name']

    other_sellers = validate_json(validate_string(item['other_sellers']))
    if other_sellers:
        for other_seller in other_sellers:
            pid = other_seller['product_id']
            price = other_seller['price']
            store_id = other_seller['store_id']
            store_name = other_seller['name']
            cursor.execute(
                'INSERT INTO tiki_product(pid, sku, name, url_key, url_path, sales_page, short_desc, price, category, brand, store_id, store_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                (pid, sku, name, url_key, url_path, sales_page, short_desc, price, category, brand, store_id,
                 store_name))

    current_seller = validate_json(validate_string(item['current_seller']))
    if current_seller != None:
        pid = current_seller['product_id']
        price = current_seller['price']
        store_id = current_seller['store_id']
        store_name = current_seller['name']

    # id = validate_string(product['id'])
    # sku = validate_string(product['sku'])
    # name = validate_string(product['name'])
    # print(id, sku, name)
    # url_key = validate_string(item.get("url_key", None))
    # url_path = validate_string(item.get("url_path", None))
    # sales_page = "tiki"
    # short_desc = validate_string(item.get("short_description", None))
    # price = validate_string(item.get("price", None))
    # category = validate_string(item['categories'][0])

    # brand = validate_string()
    # thumbnail_url = validate_string((item.get("thumbnail_url", None)))
    # code = item.get("badges", None)
    # product_id = item.get("id")
    # sku = item.get("sku")
    # product_title = item.get("name").encode(encoding='utf-8')
    # url_key = item.get("url_key")
    # url_path = item.get("url_path")
    # vendor = "tiki"
    # short_desc = "No desc"
    # price = item.get("price")
    if price > 3000000:
        cursor.execute('INSERT INTO tiki_product(pid, sku, name, url_key, url_path, sales_page, short_desc, price, category, brand, store_id, store_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (pid, sku, name, url_key, url_path, sales_page, short_desc, price, category, brand, store_id, store_name))
#     print("Done!")
mydb.commit()
cursor.close()
