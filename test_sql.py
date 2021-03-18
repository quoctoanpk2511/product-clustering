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

listCluster = []

query = [
    'select product_title, product.cluster_id '
    'from product join '
    '(select cluster_id, floor(rand()*(100-1)+1) as ran '
    'from product '
    'where product.cluster_id < 11 '
    'group by cluster_id having count(*) > 10 ' #product quality in cluster > 10
    'order by ran limit 7) tb2 ' #limit cluster quality
    'on product.cluster_id = tb2.cluster_id '
    'order by tb2.ran;'
]

cursor = mydb.cursor()
cursor.execute(query[0])
results = cursor.fetchall()
for result in results:
    print(result)