import pdfplumber
import pandas as pd
import os
import glob
import re as re

# pdf = pdfplumber.open("data_raw/dispensaries_20210503.pdf")
# pdf = pdfplumber.open("data_raw/waste_disposal_20210503.pdf")
# pdf = pdfplumber.open("data_raw/transporter_20210503.pdf")
# pdf = pdfplumber.open("data_raw/processor_20210503.pdf")
# pdf = pdfplumber.open("data_raw/growers_20210503.pdf")


# pages = pdf.pages
# counter = 0
#
# # Dumping the CSVs of every page of the PDF
# for i in pages:
#     table = i.extract_table()
#     df = pd.DataFrame(table[0:])
#     #    df = pd.DataFrame(table[0:], columns=table[0])
#     counter = counter + 1
#     df.to_csv(str(counter) + "_test.csv", index=False)

# print(df)
# Renaming the file names
# i = 0
# path="data_processed/waste_disposal_page_data/"
# for filename in os.listdir(path):
#     my_dest ="waste_disposal_" + str(i) + ".csv"
#     my_source =path + filename
#     my_dest =path + my_dest
#     # rename() function will
#     # rename all the files
#     os.rename(my_source, my_dest)
#     i += 1

extension = 'csv'
all_filenames = [i for i in glob.glob('data_processed/*.{}'.format(extension))]
# print(all_filenames)
#
# combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames], join="outer")
# combined_csv = combined_csv.replace('\n','', regex=True)
# print(combined_csv)

# Checking every
# column for mismatch data validation
# combined_csv_0thColumnIndex = combined_csv['0'].isnull()
# missing_data = combined_csv[combined_csv_0thColumnIndex]
# print(missing_data)
#
# combined_csv_1thColumnIndex = combined_csv['1'].isnull()
# missing_data1 = combined_csv[combined_csv_1thColumnIndex]
# print(missing_data1)

# combined_csv_2thColumnIndex = combined_csv['2'].isnull()
# missing_data2 = combined_csv[combined_csv_2thColumnIndex]
# print(missing_data2)

# combined_csv_3thColumnIndex = combined_csv['3'].isnull()
# missing_data3 = combined_csv[combined_csv_3thColumnIndex]
# print(missing_data3)

# for i in all_filenames:
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
# print(all_filenames)
dictionary = {}
for x in all_filenames:
    # print(x)
    # ^ [-+]?[0 - 9] +$ .
    key = re.split('_[0-9]+.csv',x) # The key is the first 16 characters of the file name
    # print(key[0])
    group = dictionary.get(key[0],[])
    group.append(x)
    dictionary[key[0]] = group
#
print(dictionary)
# Read all the files while checking their initials and perform sets of logic onto them
for i in dictionary:
    for j in dictionary[i]:
        dispensaries = pd.read_csv(j).drop(['Unnamed: 0', 'Unnamed: 0.1'], axis=1)
        dispensaries['License_type'] = i
        dispensaries = dispensaries[dispensaries['0'].notnull()]
        dispensaries_7thColIndex = dispensaries['7'].notnull()
        missing_data = dispensaries[dispensaries_7thColIndex]
        missing_data.drop('1', inplace=True, axis=1)
        missing_data.drop('2', inplace=True, axis=1)
        missing_data.columns = ['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
        missing_data = missing_data.replace('\n', '', regex=True)
        missing_data['zip'] = missing_data['zip'].astype("int64")
        data = missing_data['business'].str.split(r'Trade Name:', expand=True)
        missing_data['trade_name'] = data[1]
        missing_data['business'] = data[0]

        index_names_dispensaries = dispensaries[dispensaries['7'].notnull()].index
        dispensaries.drop(index_names_dispensaries, inplace=True)
        dispensaries.drop('7', inplace=True, axis=1)
        dispensaries.drop('8', inplace=True, axis=1)
        dispensaries.columns = ['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
        dispensaries = dispensaries.replace('\n', '', regex=True)
        dispensaries['zip'] = dispensaries['zip'].astype("int64")
        data = dispensaries['business'].str.split(r'Trade Name:', expand=True)
        dispensaries['trade_name'] = data[1]
        dispensaries['business'] = data[0]

        frame = [dispensaries, missing_data]
        dispensaries = pd.concat(frame)
        # print(dispensaries)
        dispensaries.to_csv("dispensaries_df.csv", index=False)

# data=combined_csv['0'].str.split(r'Trade Name:', expand=True)
# # print(data[1]);
# combined_csv['Trade'] = data[1]
# combined_csv['0'] = data[0]
# combined_csv.rename(columns={'0':'company_name'}, inplace=True)
# # combined_csv = combined_csv['company_name'] != ""
# # combined_csv[(combined_csv.company_name != "")]
# print(combined_csv)
# combined_csv = combined_csv[combined_csv['company_name'].notnull()]
# print(combined_csv)
#
# #combined_csv.to_csv("waste_disposal_20210503_combined.csv", index=False, encoding='utf-8-sig')
#
# # all_filenames= [i for i in glob.glob('*.{}'.format(extension))]
# for j in all_filenames:
#     if 'test' in j:
#         print(j)
#         os.remove(j)