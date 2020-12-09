import pandas as pd
import MySQLdb
import environ

# Reading .env file
env = environ.Env()
environ.Env.read_env()

# Import CSV
data = pd.read_csv (r'pricerunner_aggregate.csv')
df = pd.DataFrame(data, columns= ['product_id', 'product_title', 'vendor_id', 'cluster_id', 'cluster_label', 'category_id', 'category_label'])
print(df)

# Connect to SQL Server
mydb = MySQLdb.connect(
    host=env('DATABASE_HOST'),
    user=env('DATABASE_USER'),
    passwd=env('DATABASE_PASSWORD'),
    db=env('DATABASE_NAME'))
cursor = mydb.cursor()

# Create Table
cursor.execute('DROP TABLE IF EXISTS `product`; CREATE TABLE product(product_id int(20), product_title varchar(200), vendor_id int(10), cluster_id int(10), cluster_label varchar(100), category_id int(10), category_label varchar(50))')

# Insert DataFrame to Table
for row in df.itertuples():
    cursor.execute('INSERT INTO product(product_id, product_title, vendor_id, cluster_id, cluster_label, category_id, category_label) VALUES(%s, %s, %s, %s, %s, %s, %s)', [row.product_id, row.product_title, row.vendor_id, row.cluster_id, row.cluster_label, row.category_id, row.category_label])
mydb.commit()
cursor.close()
print("Done!")
