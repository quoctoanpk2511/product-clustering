import nltk
import pandas as pd
from nltk.stem.snowball import SnowballStemmer
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
cursor.execute('SELECT product_title FROM `product-clustering`.product')
results = cursor.fetchall()
list_title = []
for result in results:
    list_title.append(result[0])
# print(list_title)

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

totalvocab_stemmed = []
totalvocab_tokenized = []
for i in list_title:
    allwords_stemmed = tokenize_and_stem(i)
    totalvocab_stemmed.extend(allwords_stemmed)

    allwords_tokenized = tokenize(i)
    totalvocab_tokenized.extend(allwords_tokenized)

vocab_frame = pd.DataFrame({'words': totalvocab_tokenized}, index = totalvocab_stemmed)
print('there are ' + str(vocab_frame.shape[0]) + ' items in vocab_frame')
print(vocab_frame.head())
