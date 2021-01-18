import pandas as pd
from pathlib import Path
import recordlinkage

# Re-read in the data using the index_col
hospital_accounts = pd.read_csv(
    'https://github.com/chris1610/pbpython/raw/master/data/hospital_account_info.csv',
    index_col='Account_Num'
)
hospital_reimbursement = pd.read_csv(
    'https://raw.githubusercontent.com/chris1610/pbpython/master/data/hospital_reimbursement.csv',
    index_col='Provider_Num'
)
# Build the indexer
indexer = recordlinkage.Index()
# Can use full or block
#indexer.full()
#indexer.block(left_on='State', right_on='Provider State')

# Use sortedneighbor as a good option if data is not clean
indexer.sortedneighbourhood(left_on='State', right_on='Provider State')
candidates = indexer.index(hospital_accounts, hospital_reimbursement)
# Let's see how many matches we want to do
print("Candidates Len: ", len(candidates))
# Takes 3 minutes using the full index.
# 14s using sorted neighbor
# 7s using blocking
compare = recordlinkage.Compare()
compare.exact('City', 'Provider City', label='City')
compare.string('Facility Name',
               'Provider Name',
               threshold=0.85,
               label='Hosp_Name')
compare.string('Address',
               'Provider Street Address',
               method='jarowinkler',
               threshold=0.85,
               label='Hosp_Address')
features = compare.compute(candidates, hospital_accounts,
                           hospital_reimbursement)
# What are the score totals?
features.sum(axis=1).value_counts().sort_index(ascending=False)
# Get the potential matches
potential_matches = features[features.sum(axis=1) > 1].reset_index()
potential_matches['Score'] = potential_matches.loc[:, 'City':'Hosp_Address'].sum(axis=1)
# Add some convenience columns for comparing data
hospital_accounts['Acct_Name_Lookup'] = hospital_accounts[[
    'Facility Name', 'Address', 'City', 'State'
]].apply(lambda x: '_'.join(x), axis=1)
hospital_reimbursement['Reimbursement_Name_Lookup'] = hospital_reimbursement[[
    'Provider Name', 'Provider Street Address', 'Provider City',
    'Provider State'
]].apply(lambda x: '_'.join(x), axis=1)
reimbursement_lookup = hospital_reimbursement[['Reimbursement_Name_Lookup']].reset_index()
account_lookup = hospital_accounts[['Acct_Name_Lookup']].reset_index()
account_merge = potential_matches.merge(account_lookup, how='left')
final_merge = account_merge.merge(reimbursement_lookup, how='left')
cols = [
    'Account_Num', 'Provider_Num', 'Score', 'Acct_Name_Lookup',
    'Reimbursement_Name_Lookup'
]
print(final_merge[cols].sort_values(by=['Account_Num', 'Score'], ascending=False))
