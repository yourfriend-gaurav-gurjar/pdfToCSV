# Libraries
import pdfquery
import pandas_read_xml as pdx

# Convert PDF to XML format
pdf = pdfquery.PDFQuery("dispensaries_20210503.pdf")
#pdf.load()
#print(pdf.tree)
#pdf.tree.write("dispensaries_20210503.xml", pretty_print=True, encoding="utf-8")
pdf.tree.xpath('//*/LTPage')
print(pdf.pq('LTPage[LTTextBoxHorizontal]'))
# df = pdx.read_xml("test_1.xml", ['pdfxml', 'LTPage', 'LTTextLineHorizontal', 'LTTextBoxHorizontal'])
# print(df)
# Meta Data of the PDF
# metadata_df = pdx.read_xml("test_1.xml", ['pdfxml', 'LTPage', 'LTTextLineHorizontal'], root_is_rows=False)
# print(metadata_df)

# df = pdx.read_xml("test_1.xml", ['pdfxml', 'LTPage', 'LTTextLineHorizontal', 'LTTextBoxHorizontal'], root_is_rows=False)

# pdf.extract([
#      ('with_parent','LTPage[pageid="1"]'),
#      ('with_formatter', 'text'),
#      ('last_name', 'LTTextBoxHorizontal:in_bbox("0, 0, 792, 612")'),
#      ('oath', 'LTTextBoxHorizontal:contains("DAAA-4YLJ-MSMT")', lambda match: match.text()[:30]+"...")
#  ])
#      ('spouse', 'LTTextLineHorizontal:in_bbox("170,650,220,680")'),
#
#      ('with_parent','LTPage[pageid=2]'),
#
#
#      ('year', 'LTTextLineHorizontal:contains("Form 1040A (")', lambda match: int(match.text()[-5:-1]))
#pdf.extract([('font_height', 'LTTextBoxHorizontal:in_height("12.0")')])

# label = pdf.pq('LTTextLineHorizontal:contains("NAME")')
# left_corner = float(label.attr('x0'))
# bottom_corner = float(label.attr('y0'))
# cname = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (left_corner, bottom_corner, left_corner+41.76, bottom_corner)).text()
# print(cname)