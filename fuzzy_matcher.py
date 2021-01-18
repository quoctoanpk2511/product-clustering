import pandas as pd
from pathlib import Path
import fuzzymatcher
hospital_accounts = pd.read_csv(
    'https://github.com/chris1610/pbpython/raw/master/data/hospital_account_info.csv'
)
hospital_reimbursement = pd.read_csv(
    'https://raw.githubusercontent.com/chris1610/pbpython/master/data/hospital_reimbursement.csv'
)
# print(hospital_accounts.head())
# Columns to match on from df_left
left_on = ["Facility Name", "Address", "City", "State"]

# Columns to match on from df_right
right_on = [
    "Provider Name", "Provider Street Address", "Provider City",
    "Provider State"
]
# Now perform the match
# It will take several minutes to run on this data set
matched_results = fuzzymatcher.fuzzy_left_join(hospital_accounts,
                                               hospital_reimbursement,
                                               left_on,
                                               right_on,
                                               left_id_col='Account_Num',
                                               right_id_col='Provider_Num')
# Reorder the columns to make viewing easier
cols = [
    "best_match_score", "Facility Name", "Provider Name", "Address", "Provider Street Address",
    "Provider City", "City", "Provider State", "State"
]
# Look at the matches around 1
print(matched_results[cols].sort_values(by=['best_match_score'],
                                  ascending=False).head(5))

