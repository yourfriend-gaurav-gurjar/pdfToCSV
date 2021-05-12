import pdfplumber
import pandas as pd
import os
import glob
import re as re

# pdf = pdfplumber.open("dispensaries_20210503.pdf")
# pdf = pdfplumber.open("waste_disposal_20210503.pdf")
# pdf = pdfplumber.open("transporter_20210503.pdf")
# pdf = pdfplumber.open("Growers_20210427.pdf")
# pdf = pdfplumber.open("growers_20210503.pdf")
pdf = pdfplumber.open("processor_20210503.pdf")


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
#
# print(combined_csv['0'])
# print(combined_csv['1'])
#combined_csv['Trade'] = [re.split(r'Trade.*','', str(x)) for x in combined_csv['0']]
# combined_csv['Trade'] = combined_csv['Trade'].str.replace(r'\n$', '')
combined_csv.to_csv("processor_20210503.csv", index=False, encoding='utf-8-sig')

# all_filenames= [i for i in glob.glob('*.{}'.format(extension))]
for j in all_filenames:
    if 'test' in j:
        print(j)
        os.remove(j)
#combined_csv.to_csv("combined_csv.csv", ignore_index = True, encoding='utf-8-sig')

# print(combined_csv)

# Checking the dimensions
# for f in all_filenames:
#     df = pd.read_csv(f)
# computing number of rows
# rows = len(df.axes[0])
#
# # computing number of columns
# cols = len(df.axes[1])
#
# print(df)
# print("Number of Rows: ", rows)
# print("Number of Columns: ", cols)

# complete_cases and errors_df
# df2 = pd.DataFrame()
# result = pd.DataFrame()
# for f in all_filenames:
#     df = pd.read_csv(f)
#     # print(df.shape)
#     if df.shape == (15, 8):
#         df2 = [df]
#         df2 = pd.concat(df2, join="outer", axis=0)
#         result = result.append(df2)
#
# result.to_csv("df2.csv", index=False)
# colnames = []
# inputs = []
# counter = 0
#
# key = 'M' + str(counter)
# counter += 1
# colnames.append(key)
# inputs.append(value)
#
