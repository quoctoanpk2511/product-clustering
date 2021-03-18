import nltk
# nltk.download('stopwords')
import pandas as pd
from nltk.stem.snowball import SnowballStemmer
import MySQLdb
import environ
from sklearn.feature_extraction.text import TfidfVectorizer
import time
from sklearn.metrics.pairwise import cosine_similarity
from  scipy.cluster import hierarchy
from scipy.cluster.hierarchy import ward, dendrogram, linkage
import matplotlib.pyplot as plt
from sklearn.cluster import AgglomerativeClustering
import re
from sklearn.feature_extraction.text import CountVectorizer

# Reading .env file
env = environ.Env()
environ.Env.read_env()

# Connect to SQL Server
mydb = MySQLdb.connect(
    host=env('DATABASE_HOST'),
    user=env('DATABASE_USER'),
    passwd=env('DATABASE_PASSWORD'),
    db=env('DATABASE_NAME'))

query = [
    'select product_id, product_title, product.cluster_id '
    'from product join '
    '(select cluster_id, floor(rand()*(100-1)+1) as ran '
    'from product '
    # 'where product.cluster_id < 48 '
    # 'where product.cluster_id in (4, 10, 14, 17, 22) '
    # 'and product.product_id not in (68, 69, 122, 169) '
    'group by cluster_id having count(*) > 10 ' #product quality in cluster > 10
    'order by ran limit 5) tb2 ' #limit cluster quality
    'on product.cluster_id = tb2.cluster_id '
    'order by tb2.ran;'
]

cursor = mydb.cursor()
cursor.execute('SELECT product_id, product_title, product.cluster_id FROM `product-clustering`.product WHERE category_id = 2612 AND cluster_id < 11')
# cursor.execute('SELECT product_title FROM `product-clustering`.product WHERE category_id = 2612 AND cluster_id < 11 AND product_id NOT IN (68, 69, 122, 169)')
# cursor.execute('SELECT product_title FROM `product-clustering`.product WHERE category_id = 2612 AND cluster_id < 11 AND product_id NOT IN (68, 69, 122, 169) ORDER BY rand() limit 100')
# cursor.execute(query[0])
results = cursor.fetchall()
list_id = []
list_title = []
list_old_cluster = []
for result in results:
    list_id.append(result[0])
    list_title.append(re.sub('(?<=\d) (?=gb)', '', result[1]))
    list_old_cluster.append(result[2])
list_old_cluster_set = list(set(list_old_cluster))
print(list_old_cluster_set)
# print(list_title)

# stopwords = nltk.corpus.stopwords.words('english')
# print(type(stopwords))
# stopwords = []
stopwords = ['black', 'white', 'grey', 'silver', 'unlocked', 'sim', 'free', 'gold', 'rose', 'space', 'handset', 'only', 'mobile phone', 'phone', 'smartphone', 'in', 'mobile', 'single', 'cm', '4g', '4.7', '5.5', '5.8']
# stopwords = ['black', 'white', 'grey', 'silver', 'gold', 'rose']
# stopwords.append(stopwords1)
stemmer = SnowballStemmer('english')
# titles = ['apple iphone 8 plus 64gb silver', 'apple iphone 7']

# Tokenizes and stems each token
def tokenize_and_stem(text):
    tokens = [word for sent in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sent)]
    stems = [stemmer.stem(t) for t in tokens]
    return stems

    # Filter out any tokens not containing letters
    # filtered_tokens = []
    # for token in tokens:
    #     if re.search('[a-zA-Z]', token):
    #         filtered_tokens.append(token)
    # stems = [stemmer.stem(t) for t in filtered_tokens]
    # return stems

# Tokenizes only
def tokenize(text):
    tokens = [word.lower() for sent in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sent)]
    return tokens

    # Filter out any tokens not containing letters
    # filtered_tokens = []
    # for token in tokens:
    #     if re.search('[a-zA-Z]', token):
    #         filtered_tokens.append(token)
    # return filtered_tokens

# totalvocab_stemmed = []
# totalvocab_tokenized = []
# for i in list_title:
#     allwords_stemmed = tokenize_and_stem(i)
#     totalvocab_stemmed.extend(allwords_stemmed)
#
#     allwords_tokenized = tokenize(i)
#     totalvocab_tokenized.extend(allwords_tokenized)
#
# vocab_frame = pd.DataFrame({'words': totalvocab_tokenized}, index = totalvocab_stemmed)
# print('there are ' + str(vocab_frame.shape[0]) + ' items in vocab_frame')
# print(vocab_frame.head())

# tokenizer
def tokenizer(text):
    tokens = [word.lower() for word in text.split(' ')]
    return tokens

# tfidf_vectorizer = TfidfVectorizer(max_df=0.6, max_features=20000, min_df=10, stop_words=stopwords, use_idf=True, ngram_range=(1,3), tokenizer=tokenize)
# tfidf_vectorizer = TfidfVectorizer(max_df=0.6, max_features=20000, min_df=10, stop_words=stopwords, use_idf=False, ngram_range=(1,3), tokenizer=tokenize)
tfidf_vectorizer = TfidfVectorizer(max_df=0.7, max_features=20000, min_df=0.02, stop_words=stopwords, ngram_range=(1,3), tokenizer=tokenize)
start = time.time()
tfidf_matrix = tfidf_vectorizer.fit_transform(list_title)
print("Time: ", time.time() - start)
print(tfidf_matrix.shape)
# print(tfidf_matrix[0])

terms = tfidf_vectorizer.get_feature_names()
print(terms)

# tfidf matrix
# df = pd.DataFrame(data=tfidf_matrix.toarray(), index=list_title,
#                   columns=terms)
# print(df)

dist = cosine_similarity(tfidf_matrix)
# print(dist[6][2])
# print(dist[159][149])

# print(cosine_similarity(tfidf_matrix)[0])

# linkage_matrix = ward(dist) #define the linkage_matrix using ward clustering pre-computed distances
linkage_matrix = linkage(dist, 'complete', metric='cosine') #define the linkage_matrix using single-linkage clustering pre-computed distances
# for i in range(0, len(linkage_matrix)-1):
#     print(i, ": ", linkage_matrix[i])

# linkage_matrix1 = hierarchy.linkage(dist, metric='cosine')
# cluster1 = hierarchy.fcluster(linkage_matrix1, 90, criterion='distance')
# print(cluster1)


fig, ax = plt.subplots(figsize=(15, 20)) # set size
ax = dendrogram(linkage_matrix, orientation="right", labels=list_title);

plt.tick_params(\
    axis= 'x',          # changes apply to the x-axis
    which='both',      # both major and minor ticks are affected
    bottom='off',      # ticks along the bottom edge are off
    top='off',         # ticks along the top edge are off
    labelbottom='off')

plt.tight_layout() #show plot with tight layout
plt.axvline(x=3.7, color='r', linestyle='--')

# uncomment below to save figure
plt.savefig('cluster_result.png', dpi=200) #save figure as ward_clusters
# plt.close()

# https://stackoverflow.com/questions/23856002/python-get-clustered-data-hierachical-clustering
# n = len(list_title)
# cluster_dict = dict()
# for i in range(0, 196):
#     new_cluster_id = n+i
#     old_cluster_id_0 = linkage_matrix[i, 0]
#     old_cluster_id_1 = linkage_matrix[i, 1]
#     combined_ids = list()
#     if old_cluster_id_0 in cluster_dict:
#         combined_ids += cluster_dict[old_cluster_id_0]
#         del cluster_dict[old_cluster_id_0]
#     else:
#         combined_ids += [old_cluster_id_0]
#     if old_cluster_id_1 in cluster_dict:
#         combined_ids += cluster_dict[old_cluster_id_1]
#         del cluster_dict[old_cluster_id_1]
#     else:
#         combined_ids += [old_cluster_id_1]
#     cluster_dict[new_cluster_id] = combined_ids
# # print(cluster_dict)
# print("Len dict: ", len(cluster_dict))

# test cluster in 104 times loop
# for i in cluster_dict.keys():
#     print("Cluster: ", i ,cluster_dict.get(i))

# combine to new list title
# clusted_list_title = dict()
# count = 0
# for i in cluster_dict.keys():
#     list_cluster = []
#     for t in cluster_dict.get(i):
#         list_cluster.append(list_title[int(t)])
#     clusted_list_title[count] = list_cluster
#     count+= 1

# print(len(clusted_list_title))
# dem = 0
# for i in clusted_list_title:
#     print("Cluser: ", i, "Quality: ", len(clusted_list_title.get(i)))
#     for title in clusted_list_title.get(i):
#         # print(title)
#         dem+= 1
# print("Title Quality: ", dem)

# https://stackabuse.com/hierarchical-clustering-with-python-and-scikit-learn/
model = AgglomerativeClustering(n_clusters=12, affinity='cosine', linkage='complete')
# cluster = AgglomerativeClustering(n_clusters=None, affinity='cosine', linkage='single', distance_threshold=3)
clusted = model.fit_predict(linkage_matrix)
# for i in range(0, 209):
#     print(i, ": ", linkage_matrix[i])
# cluster.fit(linkage_matrix)
print("n cluster: ", model.n_clusters_)
print("Clusted array: ", len(model.labels_))
# clust = pd.DataFrame(model)
# print(clust.count())

# in row_dict we store actual meanings of rows, in my case it's russian words
# clusters = {}
# n = 0
# for item in clusted:
#     if item in clusters:
#         clusters[item].append(list_title[n])
#     else:
#         clusters[item] = [list_title[n]]
#     n +=1
# for item in clusters:
#     print("Cluster ", item)
#     for i in clusters[item]:
#         print(i)

plt.figure(figsize=(10, 7))
plt.scatter(linkage_matrix[:,0], linkage_matrix[:,1], c=model.labels_, cmap='rainbow')
plt.savefig('pic.png', dpi=200)
plt.close()

from scipy.cluster.hierarchy import fcluster
max_d = 10
# clusters = fcluster(linkage_matrix, max_d, criterion='maxclust')
clusters = fcluster(linkage_matrix, t=0.65, criterion='distance')
print("Len clusters: ", len(clusters))
print(clusters)
clus_set = list(set(clusters))
print(clus_set)

clus_list = []
for i in range(0, len(clusters)):
    clus_dict = {'id': list_id[i], 'old_cluster': list_old_cluster[i], 'new_cluster': clusters.item(i), 'title': list_title[i]}
    clus_list.append(clus_dict)
clus_list.sort(key=lambda item: item.get("new_cluster"))
for cl in clus_list:
    print(cl)

