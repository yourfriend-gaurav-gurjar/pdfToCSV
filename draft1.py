import PyPDF2
# import camelot
# import pandas as pd
# import numpy as np
import tabula
from tabula import read_pdf
from tabulate import tabulate
import json

pdfFileObj = open("dispensaries_20210503.pdf", 'rb')
pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
read_pdf = PyPDF2.PdfFileReader(pdfFileObj)

# pages = pdfReader.numPages
# print(pages)
page = read_pdf.getPage(0)
page_content = page.extractText()
data = json.dumps(page_content)
formatj = json.loads(data)
print(formatj)
table = tabula.read_pdf(pdfFileObj, pages=1)
print(table[0])
#df1 = pd.DataFrame(pd.np.empty((0, 2)))
tables = tabula.read_pdf(pdfFileObj, pages="all")
print(tables[0])
print(tables[1])
print(tabulate(tables))

# df = tabula.convert_into("dispensaries_20210503.pdf", "dispensaries_20210503.csv",pages='all', output_format="csv")

# camelot_df = camelot.read_pdf('dispensaries_20210503.pdf', flavor="stream", suppress_stdout=True, pages="all")[0].df
# camelot_df = camelot.read_pdf(pdfFileObj, flavor="stream", suppress_stdout=True, pages="all")[0].df

# camelot_df

# df = camelot.read_pdf('dispensaries_20210503.pdf')
# df
