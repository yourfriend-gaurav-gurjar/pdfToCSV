import os
import glob
import pandas as pd
os.chdir("s3_data/")

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

#combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
#combined_csv.to_csv("combined_csv.csv", index=False, encoding='utf-8-sig')
#combined_csv.to_csv("combined_csv.csv", ignore_index = True, encoding='utf-8-sig')

#print(combined_csv)

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
df2 = pd.DataFrame()
result = pd.DataFrame()
for f in all_filenames:
    df = pd.read_csv(f)
    #print(df.shape)
    if df.shape == (15,8):
        df2 = [df]
        df2 = pd.concat(df2, join="outer", axis=0)
        result = result.append(df2)

result.to_csv("df2.csv", index=False)
# colnames = []
# inputs = []
# counter = 0
#
# key = 'M' + str(counter)
# counter += 1
# colnames.append(key)
# inputs.append(value)
#
