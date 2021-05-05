import pdfplumber
import pandas as pd
# with pdfplumber.open("path/to/file.pdf") as pdf:
#     first_page = pdf.pages[0]
#     print(first_page.chars[0])

pdf = pdfplumber.open("dispensaries_20210503.pdf")
# page = pdf.pages[0]
pages = pdf.pages
for i in pages:
    table = i.extract_table()
    df = pd.DataFrame(table[0:], columns=table[0])
    df = df.append(df)

#page.extract_table()

#im = page.to_image()
# table = page.extract_table()
# print(table[:3])


# for column in ["EMAIL", "PHONE"]:
#     df[column] = df[column].str.replace(" ", "")
# df = pd.DataFrame(table[0:], columns=table[0])
print(df)
