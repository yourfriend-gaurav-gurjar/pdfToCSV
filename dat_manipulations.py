import pdfplumber
import pandas as pd
import glob
import re as re


cmb_df = pd.read_csv("Growers_20210427_combined.csv")
#print(cmb_df)
# text = "Trade"
#
# x = re.split("\s", text, 1)

# for line in re.findall("Trade.*", cmb_df['0']):
#     print(line)

#print(cmb_df['0'].str.extract(r'(^Trade.)'))


cmb_df['Trade'] =  [re.sub(r'Trade.*','', str(x)) for x in cmb_df['0']]
# print(cmb_df['Trade'].str.replace(r'\n$', ''))
print(cmb_df['Trade'].str.extract(r'\n$', ''))
#print(cmb_df['Trade'])