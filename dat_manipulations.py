import pdfplumber
import pandas as pd
import os
import glob
import re as re

pdf = pdfplumber.open("dispensaries_20210503.pdf")
# pdf = pdfplumber.open("waste_disposal_20210503.pdf")
#pdf = pdfplumber.open("transporter_20210503.pdf")
# page = pdf.pages[0]
pages = pdf.pages
counter = 0

# Dumping the CSVs of every page of the PDF
for i in pages:
    table = i.extract_table()
    df = pd.DataFrame(table[0:])
    #    df = pd.DataFrame(table[0:], columns=table[0])
    counter = counter + 1
    df.to_csv(str(counter) + "_test.csv", index=False)

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames], join="outer")
# combined_csv = combined_csv.replace('\n','', regex=True)
data=combined_csv['0'].str.split(r'Trade Name:', expand=True)
# print(data[1]);
# combined_csv['Trade'] = combined_csv['0'].str.split(r'Trade Name:', expand=True)
combined_csv['Trade'] = data[1]
combined_csv['0'] = data[0]
combined_csv.rename(columns={'0':'company_name'}, inplace=True)
# combined_csv = combined_csv['company_name'] != ""
# combined_csv[(combined_csv.company_name != "")]
print(combined_csv)
combined_csv = combined_csv[combined_csv['company_name'].notnull()]
print(combined_csv)

#combined_csv.to_csv("waste_disposal_20210503_combined.csv", index=False, encoding='utf-8-sig')

# all_filenames= [i for i in glob.glob('*.{}'.format(extension))]
for j in all_filenames:
    if 'test' in j:
        print(j)
        os.remove(j)