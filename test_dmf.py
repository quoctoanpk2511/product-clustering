from matchingframework.structures.data import MappingFeature, Feature
from matchingframework.preprocess.data_preprocessor import DataPreprocessor
from matchingframework.preprocess.tokenizers import Tokenizer
from pmt.scores.vectorizers import TFIDFVectorizer
from pmt.scores.similarities import CosineSimilarityScorer
from pmt.scores.clusters import HierarchicalClustering, HierarchicalClustering2
from matchingframework.match.matcher import Matcher
from pmt.utils.readers import MySQLReader
from pmt.utils.writers import MySQLWriter
import environ
import re

env = environ.Env()
environ.Env.read_env()


class TitleTokenizer(Tokenizer):
    """
    Tokenize product title.
    """

    def tokenize_record(self, record):
        tokens = [word.lower() for word in record.split(' ')]
        return tokens

    def normalize_record(self, record):
        nomalized_record = re.sub('(?<=\d) (?=gb)', '', record)
        return nomalized_record

class Title(Feature):

    def __init__(self, field_name, weight):
        super().__init__(field_name=field_name, weight=weight)
        self.tokenizer = TitleTokenizer()

class Title1Tokenizer(Tokenizer):
    """
    Tokenize product title.
    """

    def tokenize_record(self, record):
        tokens = [word.lower() for word in record.split(' ')]
        return tokens

    def normalize_record(self, record):
        nomalized_record = record
        nomalized_record = re.sub('([0-9])([A-Za-z])', r'\1 \2', nomalized_record)
        return nomalized_record

class Title1(Feature):

    def __init__(self, field_name, weight):
        super().__init__(field_name=field_name, weight=weight)
        self.tokenizer = Title1Tokenizer()

class Brand1Tokenizer(Tokenizer):
    """
    Tokenize product title.
    """

    def tokenize_record(self, record):
        tokens = [record.lower()]
        return tokens

    def normalize_record(self, record):
        return record

class Brand1(Feature):

    def __init__(self, field_name, weight):
        super().__init__(field_name=field_name, weight=weight)
        self.tokenizer = Brand1Tokenizer()

left_table = 'test_tiki_product'
query1 = "SELECT * FROM {} WHERE id != 137".format(left_table)
mysql = MySQLReader(env('DATABASE_HOST'),
                 env('DATABASE_USER'),
                 env('DATABASE_PASSWORD'),
                 env('DATABASE_NAME'),
                 query1)
dataset1 = mysql.read()
right_table = 'test_sendo_product'
query2 = "SELECT * FROM {}".format(right_table)
mysql = MySQLReader(env('DATABASE_HOST'),
                 env('DATABASE_USER'),
                 env('DATABASE_PASSWORD'),
                 env('DATABASE_NAME'),
                 query2)
dataset2 = mysql.read()

# left_table = right_table = 'kaggle_product'
# query = "SELECT * FROM {} WHERE category_id = 2612 AND product_id % 2 = 1 AND cluster_id < 35".format(left_table)
# mysql = MySQLReader(env('DATABASE_HOST'), env('DATABASE_USER'), env('DATABASE_PASSWORD'), env('DATABASE_NAME'), query)
# dataset1 = mysql.read()
# query = "SELECT * FROM {} WHERE category_id = 2612 AND product_id % 2 = 0 AND cluster_id < 35".format(right_table)
# mysql = MySQLReader(env('DATABASE_HOST'), env('DATABASE_USER'), env('DATABASE_PASSWORD'), env('DATABASE_NAME'), query)
# dataset2 = mysql.read()

title = Title1(field_name='title', weight=0.5)
brand = Brand1(field_name='brand', weight=0.5)

mapping_features = MappingFeature()
mapping_features.join_features = [title, brand]
# mapping_features.left_features = ['product_title']
# mapping_features.right_features = ['product_title']
mapping_features.left_features = ['name', 'brand']
mapping_features.right_features = ['name', 'brand']

# stopwords = []
# stopwords = ['black', 'white', 'grey', 'silver', 'gold', 'rose']
stopwords = ['freeship', 'hàng', 'nhập', 'khẩu', 'chính', 'hãng', 'sale', 'model', 'rẻ']

data_preprocessor = DataPreprocessor()
# tokenizer = DefaultTokenizer()
vectorizer = TFIDFVectorizer(max_df=0.7, min_df=0.01, stop_words=stopwords, ngram_range=(1,3))
# vectorizer = COUNTVectorizer(max_df=0.7, min_df=0.01, ngram_range=(1,3))
similarity_scorer = CosineSimilarityScorer()
# cluster = HierarchicalClustering(threshold=0.0008)
threshold = 0.42
cluster = HierarchicalClustering2(threshold=threshold)
# cluster = KMeansClustering(n_clusters=11)

m = Matcher(data_preprocessor=data_preprocessor,
            vectorizer=vectorizer,
            similarity=similarity_scorer,
            cluster=cluster)
m.add_data(dataset1, dataset2, mapping_features)
m.match()

left_label = list(m.left_data.df['id'])
right_label = list(m.right_data.df['id'])
for name in right_label:
    left_label.append(name)

from scipy.cluster.hierarchy import dendrogram
import matplotlib.pyplot as plt
fig, ax = plt.subplots(figsize=(15, 20)) # set size
ax = dendrogram(m.linkage_matrix, orientation="right", labels=left_label);

plt.tick_params(\
    axis= 'x',          # changes apply to the x-axis
    which='both',      # both major and minor ticks are affected
    bottom='off',      # ticks along the bottom edge are off
    top='off',         # ticks along the top edge are off
    labelbottom='off')

plt.tight_layout() #show plot with tight layout
plt.axvline(x=threshold, color='r', linestyle='--')

# uncomment below to save figure
plt.savefig('cluster_result2.png', dpi=200) #save figure as ward_clusters
plt.close()

# from matchingframework.utils.writers import CSVWriter
# csv = CSVWriter(dataset=m.results, csv_file='results.csv')
# csv.write()

# print(m.left_data.df[['product_id', 'cluster_id']])
# print(m.right_data.df)


# mysql = MySQLWriter(env('DATABASE_HOST'),
#                     env('DATABASE_USER'),
#                     env('DATABASE_PASSWORD'),
#                     env('DATABASE_NAME'),
#                     dataset=m.left_data)
# mysql.insert('test_tiki_product')
# from matchingframework.utils.writers import MySQLWriter
# mysql = MySQLWriter(env('DATABASE_HOST'),
#                     env('DATABASE_USER'),
#                     env('DATABASE_PASSWORD'),
#                     env('DATABASE_NAME'),
#                     dataset=m.right_data)
# mysql.insert('test_sendo_product')

mysql = MySQLWriter(env('DATABASE_HOST'),
                    env('DATABASE_USER'),
                    env('DATABASE_PASSWORD'),
                    env('DATABASE_NAME'),
                    dataset=m.left_data)
# mysql.update(table_name='kaggle_product', pk='product_id')
mysql.update(table_name='test_tiki_product', pk='pid')

mysql = MySQLWriter(env('DATABASE_HOST'),
                    env('DATABASE_USER'),
                    env('DATABASE_PASSWORD'),
                    env('DATABASE_NAME'),
                    dataset=m.right_data)
# mysql.update(table_name='kaggle_product', pk='product_id')
# mysql.update(table_name='test_sendo_product', pk='pid')
mysql.update(table_name='test_tiki_product', pk='pid')

print("Done!!!")

