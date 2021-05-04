import tabula

# Read pdf into list of DataFrame
# df = tabula.read_pdf("test.pdf", pages='all')

# Read remote pdf into list of DataFrame
# df2 = tabula.read_pdf("dispensaries_20210503.pdf")

# convert PDF into CSV file
tabula.convert_into("dispensaries_20210503.pdf", "output.csv", output_format="csv", pages='all')