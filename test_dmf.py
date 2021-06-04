from base.structures.data import MappingFeature, Feature
from base.utils.readers import MySQLReader
from base.preprocess.data_preprocessor import DefaultDataPreprocessor
from base.preprocess.tokenizers import DefaultTokenizer
from base.scores.vectorizers import TFIDFVectorizer
from base.scores.similarities import Cosine_Similarity
from base.scores.clusters import HierarchicalClustering
from base.match.matcher import Matcher
import environ
import re

env = environ.Env()
environ.Env.read_env()

class TitleTokenizer(DefaultTokenizer):
    """
    Tokenize product title.
    """

    def normalize_record(self, record):
        nomalized_record = re.sub('(?<=\d) (?=gb)', '', record)
        return nomalized_record

class Title(Feature):

    def __init__(self, field_name, weight):
        super().__init__(field_name=field_name, weight=weight)
        self.tokenizer = TitleTokenizer()


query1 = "SELECT * FROM `product-clustering`.tiki_product"
mysql = MySQLReader(env('DATABASE_HOST'),
                    env('DATABASE_USER'),
                    env('DATABASE_PASSWORD'),
                    env('DATABASE_NAME'),
                    query1)
dataset1 = mysql.read()
query2 = "SELECT * FROM `product-clustering`.sendo_product"
mysql = MySQLReader(env('DATABASE_HOST'),
                    env('DATABASE_USER'),
                    env('DATABASE_PASSWORD'),
                    env('DATABASE_NAME'),
                    query2)
dataset2 = mysql.read()

title = Title(field_name='product_title', weight=1)

mapping_features = MappingFeature()
mapping_features.join_features = [title]
mapping_features.left_features = ['name']
mapping_features.right_features = ['name']

stopwords = []

data_preprocessor = DefaultDataPreprocessor()
# tokenizer = DefaultTokenizer()
vectorizer = TFIDFVectorizer(max_df=0.8, min_df=5, stop_words=stopwords, ngram_range=(1,3))
# vectorizer = COUNTVectorizer(max_df=0.7, min_df=0.01, ngram_range=(1,3))
similarity_scorer = Cosine_Similarity()
cluster = HierarchicalClustering(threshold=0.55)
# cluster = KMeansClustering(n_clusters=11)

m = Matcher(data_preprocessor=data_preprocessor,
            vectorizer=vectorizer,
            similarity=similarity_scorer,
            cluster=cluster)
m.add_data(dataset1, dataset2, mapping_features)
m.match()


# from base.utils.writers import CSVWriter
# csv = CSVWriter(dataset=m.results, csv_file='results.csv')
# csv.write()

print(m.left_data.df)
print(m.right_data.df)

# from base.utils.writers import MySQLWriter
# mysql = MySQLWriter(env('DATABASE_HOST'),
#                     env('DATABASE_USER'),
#                     env('DATABASE_PASSWORD'),
#                     env('DATABASE_NAME'),
#                     dataset=m.left_data)
# mysql.insert('test_tiki_product')
# from base.utils.writers import MySQLWriter
# mysql = MySQLWriter(env('DATABASE_HOST'),
#                     env('DATABASE_USER'),
#                     env('DATABASE_PASSWORD'),
#                     env('DATABASE_NAME'),
#                     dataset=m.right_data)
# mysql.insert('test_sendo_product')
