from PyPDF2 import PdfFileReader

pdf_document = "dispensaries_20210503.pdf"
with open(pdf_document, "rb") as filehandle:
    pdf = PdfFileReader(filehandle)
    info = pdf.getDocumentInfo()
    pages = pdf.getNumPages()

    #print (info)
    print("number of pages: %i" % pages)

    page1 = pdf.getPage(0)

    for page in pdf.pages:
        print(page)
#        print(''.join([c for c in page if c.isupper()]))
        # page = {'Page_{}'.format(counter): text}
        # data['Pages'].append(page)
        # counter += 1
    # print(page1)
    # print(page1.extractText())