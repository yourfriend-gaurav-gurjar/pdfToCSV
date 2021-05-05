import pdfplumber
import pandas as pd


pdf = pdfplumber.open("dispensaries_20210503.pdf")
pages = pdf.pages
counter = 0


# Dumping the CSVs of every page of the PDF

for i in pages:
    table = i.extract_table()
    df = pd.DataFrame(table[0:], columns=table[0])
    counter= counter+1
    df.to_csv(str(counter) + "_Page.csv")
