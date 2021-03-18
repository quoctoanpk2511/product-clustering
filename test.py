import nltk
# nltk.download('stopwords')
import pandas as pd
from nltk.stem.snowball import SnowballStemmer
import MySQLdb
import environ
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import time
from sklearn.metrics.pairwise import cosine_similarity
from  scipy.cluster import hierarchy
from scipy.cluster.hierarchy import ward, dendrogram, linkage, fcluster
import matplotlib.pyplot as plt
from sklearn.cluster import AgglomerativeClustering
import re

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

cursor.execute('SELECT product_id, product_title, vendor FROM `product-clustering`.crawl_product')
# cursor.execute('SELECT product_title FROM `product-clustering`.crawl_product')
results = cursor.fetchall()
list_id = []
list_title = []
list_vendor = []
# print(type(results))
for result in results:
    list_id.append(result[0])
    list_title.append(re.sub('[^A-Za-z0-9]&%', '', result[1]))
    list_vendor.append(result[2])

stopwords = ['hàng', 'nhập', 'khẩu', 'chính', 'hãng', '-', 'sale', 'model', 'rẻ', '+', '(', ')', '–']

def tokenize(text):
    tokens = [word.lower() for sent in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sent)]
    return tokens
tfidf_vectorizer = TfidfVectorizer(max_df=0.7, max_features=20000, min_df=10, stop_words=stopwords, ngram_range=(1,3), tokenizer=tokenize)
tfidf_matrix = tfidf_vectorizer.fit_transform(list_title)
print(tfidf_matrix.shape)
terms = tfidf_vectorizer.get_feature_names()
print(terms)
dist = cosine_similarity(tfidf_matrix)
linkage_matrix = linkage(dist, 'complete', metric='cosine')
for i in range(0, len(linkage_matrix)-1):
    print(i, ": ", linkage_matrix[i])
clusters = fcluster(linkage_matrix, t=0.3, criterion='distance')
print("Len clusters: ", len(clusters))
print(clusters)
clus_set = list(set(clusters))
print(clus_set)

clus_list = []
for i in range(0, len(clusters)-1):
    clus_dict = {'id': list_id[i], 'cluster': clusters.item(i), 'title': list_title[i], 'vendor': list_vendor[i]}
    clus_list.append(clus_dict)
clus_list.sort(key=lambda item: item.get("cluster"))
for cl in clus_list:
    print(cl)

# product_id = "1"
# sku = "sku"
# product_title = 'Apple Macbook Air 2020 - 13 Inchs (i3-10th/ 8GB/ 256GB)'
# url_key = "url_key"
# url_path = "url_path"
# vendor = "tiki"
# short_desc = "No desc"
# price = "1"
#
# cursor.execute('INSERT INTO crawl_product(product_id, sku, product_title, url_key, url_path, vendor, short_desc, price) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)', (product_id, sku, product_title, url_key, url_path, vendor, short_desc, price))
# print("Done!")
# mydb.commit()
# cursor.close()


# Test replace space
# import re
# my_string = "my phone is iphone 6 64 gb"
# my_string = re.sub('(?<=\d) (?=gb)', '', my_string)
# print(my_string)
# from scipy.cluster.hierarchy import linkage, fcluster
# from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
# from sklearn.metrics.pairwise import cosine_similarity
#
# corpus = [
#     'This is the first document.',
#     'These are second title',
#     'And this is the third one.',
#     'Is this the first document?',
# ]
# vectorizer = CountVectorizer()
# X = vectorizer.fit_transform(corpus)
# print(vectorizer.get_feature_names())
# print(X.toarray())
# dist = cosine_similarity(X)
# print(dist)
# linkage_matrix = linkage(dist, 'complete')
# print(linkage_matrix)
# clusters = fcluster(linkage_matrix, t=0.3, criterion='distance')
# print(clusters)


