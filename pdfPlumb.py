import pdfplumber
import pandas as pd

pdf = pdfplumber.open("dispensaries_20210503.pdf")
# page = pdf.pages[0]
pages = pdf.pages
counter = 0
# Dumping the CSVs of every page of the PDF

for i in pages:
    table = i.extract_table()
    df = pd.DataFrame(table[0:], columns=table[0])
    counter= counter+1
    df.to_csv(str(counter) + "_test.csv", index=False)

# Manipulating df
# for i in pages:
#     table = i.extract_table()
#     df = pd.DataFrame(table[0:], columns=["company_name", "license_number", "email", "phone", "city", "zip", "country"]).fillna("missing")
#     counter= counter+1
#     df.to_csv(str(counter) + "test.csv", index=False)


# Grouping dataframe
# for i in pages:
#     table = i.extract_table()
#     df = pd.DataFrame(table[0:], columns=table[0])
# #    df.columns("id","company_name", "license_number", "email", "phone", "city", "zip","country")
#     df2 = pd.DataFrame()
#     df2 = df2.append(df, ignore_index=True)
#     # counter= counter+1
#     # df2.to_csv(str(counter) + "test.csv")
#     # df2.to_csv("grouped.csv")
#
# df2.to_csv("grouped.csv")