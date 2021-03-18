import pandas as pd
import MySQLdb
import environ
import recordlinkage

# Reading .env file
env = environ.Env()
environ.Env.read_env()

# Import CSV
df = pd.read_csv (r'pricerunner_aggregate.csv', index_col='product_id', nrows=206)
# df = pd.DataFrame(data, columns= ['product_id', 'product_title', 'vendor_id', 'cluster_id', 'cluster_label', 'category_id', 'category_label'])
# print(df)

dupe_indexer = recordlinkage.Index()
dupe_indexer.sortedneighbourhood(left_on='cluster_id')
dupe_candidate_links = dupe_indexer.index(df)

compare_dupes = recordlinkage.Compare()
compare_dupes.string('product_title', 'product_title', threshold=0.85, label='product_title')
dupe_features = compare_dupes.compute(dupe_candidate_links, df)
print(dupe_features.sum(axis=1).value_counts().sort_index(ascending=False))
potential_dupes = dupe_features[dupe_features.sum(axis=1) >= 1].reset_index()
potential_dupes['Score'] = potential_dupes.loc[:, 'product_title': 'product_title'].sum(axis=1)
print(potential_dupes.sort_values(by=['Score'], ascending=True))

