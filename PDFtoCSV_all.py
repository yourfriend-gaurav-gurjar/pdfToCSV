import pdfplumber
import pandas as pd
import glob

extension_pdf = 'pdf'
all_filenames = [i for i in glob.glob('*.{}'.format(extension_pdf))]

for i in all_filenames:
    print(i)
    pdf = pdfplumber.open(i)
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
    combined_csv.to_csv("combined.csv", index=False, encoding='utf-8-sig')
