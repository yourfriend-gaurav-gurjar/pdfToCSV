## PLEASE PUT ALL THE FOLLOWING FILES INTO THE CURRENT FOLDER
# dispensaries20210507.csv
# growers20210507.csv
# transporter20210507.csv
# processor20210507.csv
# laboratory20210507.csv
# waste_disposal20210507.csv

import pandas as pd
import glob
import numpy as np
import matplotlib.pyplot as plt
import numpy

##
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
license_type = "license_type"

## DISPENSARIES
# Manipulating dispensaries20210507.csv
dispensaries = pd.read_csv("dispensaries20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
dispensaries['License_type'] = "dispensaries"
dispensaries = dispensaries[dispensaries['0'].notnull()]

dispensaries_7thColIndex = dispensaries['7'].notnull()
missing_data = dispensaries[dispensaries_7thColIndex]
missing_data.drop('1', inplace=True, axis=1)
missing_data.drop('2', inplace=True, axis=1)
missing_data.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
missing_data = missing_data.replace('\n','', regex=True)
missing_data['zip'] = missing_data['zip'].astype("int64")
data=missing_data['business'].str.split(r'Trade Name:', expand=True)
missing_data['trade_name'] = data[1]
missing_data['business'] = data[0]

index_names_dispensaries = dispensaries[dispensaries['7'].notnull()].index
dispensaries.drop(index_names_dispensaries, inplace=True)
dispensaries.drop('7', inplace=True, axis=1)
dispensaries.drop('8', inplace=True, axis=1)
dispensaries.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
dispensaries = dispensaries.replace('\n','', regex=True)
dispensaries['zip'] = dispensaries['zip'].astype("int64")
data=dispensaries['business'].str.split(r'Trade Name:', expand=True)
dispensaries['trade_name'] = data[1]
dispensaries['business'] = data[0]

frame = [dispensaries, missing_data]
dispensaries = pd.concat(frame)
# print(dispensaries)
dispensaries.to_csv("dispensaries_df.csv", index=False)


## GROWERS
# Manipulating growers20210507.csv
growers = pd.read_csv("growers20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
growers['License_type'] = "growers"
growers = growers[growers['0'].notnull()]

growers_7thColIndex = growers['7'].notnull()
missing_data = growers[growers_7thColIndex]

missing_data.drop('1', inplace=True, axis=1)
missing_data.drop('2', inplace=True, axis=1)
missing_data.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
missing_data = missing_data.replace('\n','', regex=True)
missing_data['zip'] = missing_data['zip'].astype("int64")
data=missing_data['business'].str.split(r'Trade Name:', expand=True)

missing_data['trade_name'] = data[1]
missing_data['business'] = data[0]

index_names_growers = growers[growers['7'].notnull()].index
growers.drop(index_names_growers, inplace=True)
growers.drop('7', inplace=True, axis=1)
growers.drop('8', inplace=True, axis=1)
growers.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
growers = growers.replace('\n','', regex=True)
growers['zip'] = growers['zip'].astype("int64")
data=growers['business'].str.split(r'Trade Name:', expand=True)
growers['trade_name'] = data[1]
growers['business'] = data[0]

frame = [growers, missing_data]
growers = pd.concat(frame)
# print(growers)
growers.to_csv("growers_df.csv", index=False)

## TRANSPORTER
# Manipulating transporter20210507.csv
transporter = pd.read_csv("transporter20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
transporter['License_type'] = "transporter"
transporter = transporter[transporter['0'].notnull()]
transporter_7thColIndex = transporter['7'].notnull()

missing_data = transporter[transporter_7thColIndex]
missing_data.drop('1', inplace=True, axis=1)
missing_data.drop('2', inplace=True, axis=1)
missing_data.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
missing_data = missing_data.replace('\n','', regex=True)
missing_data['zip'] = missing_data['zip'].astype("int64")
data=missing_data['business'].str.split(r'Trade Name:', expand=True)
missing_data['trade_name'] = data[1]
missing_data['business'] = data[0]

index_names_transporter = transporter[transporter['7'].notnull()].index
transporter.drop(index_names_transporter, inplace=True)
transporter.drop('7', inplace=True, axis=1)
transporter.drop('8', inplace=True, axis=1)
transporter.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
transporter = transporter.replace('\n','', regex=True)
transporter['zip'] = transporter['zip'].astype("int64")
data=transporter['business'].str.split(r'Trade Name:', expand=True)

transporter['trade_name'] = data[1]
transporter['business'] = data[0]

frame = [transporter, missing_data]
transporter = pd.concat(frame)
# print(transporter)
transporter.to_csv("transporter_df.csv", index=False)


## PROCESSORS
# Manipulating processor20210507.csv
processor = pd.read_csv("processor20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
processor['License_type'] = "processor"
processor = processor[processor['0'].notnull()]

processor_7thColIndex = processor['7'].notnull()
missing_data = processor[processor_7thColIndex]
missing_data.drop('1', inplace=True, axis=1)
missing_data.drop('2', inplace=True, axis=1)
missing_data.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
missing_data = missing_data.replace('\n','', regex=True)
missing_data['zip'] = missing_data['zip'].astype("int64")
data=missing_data['business'].str.split(r'Trade Name:', expand=True)
missing_data['trade_name'] = data[1]
missing_data['business'] = data[0]

index_names_processor = processor[processor['7'].notnull()].index
processor.drop(index_names_processor, inplace=True)
processor.drop('7', inplace=True, axis=1)
processor.drop('8', inplace=True, axis=1)
processor.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
processor = processor.replace('\n','', regex=True)
processor['zip'] = processor['zip'].astype("int64")
data=processor['business'].str.split(r'Trade Name:', expand=True)
processor['trade_name'] = data[1]
processor['business'] = data[0]

frame = [processor, missing_data]
processor = pd.concat(frame)
processor.to_csv("processor_df.csv", index=False)


## LABORATORY
# Manipulating laboratory20210507.csv
laboratory = pd.read_csv("laboratory20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
laboratory['License_type'] = "laboratory"
laboratory = laboratory[laboratory['0'].notnull()]

laboratory.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
laboratory = laboratory.replace('\n','', regex=True)
laboratory['zip'] = laboratory['zip'].astype("int64")
data=laboratory['business'].str.split(r'Trade Name:', expand=True)

laboratory['trade_name'] = data[1]
laboratory['business'] = data[0]
# print(laboratory)
laboratory.to_csv("laboratory_df.csv", index=False)


## WASTE DISPOSAL
# Manipulating waste_disposal20210507.csv
waste_disposal = pd.read_csv("waste_disposal20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
waste_disposal['License_type'] = "waste_disposal"
waste_disposal = waste_disposal[waste_disposal['0'].notnull()]
waste_disposal.drop('1', inplace=True, axis=1)
waste_disposal.drop('2', inplace=True, axis=1)
waste_disposal.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
waste_disposal = waste_disposal.replace('\n','', regex=True)
waste_disposal['zip'] = waste_disposal['zip'].astype("int64")
waste_disposal['phone'] = waste_disposal['phone'].astype("int64")
data=waste_disposal['business'].str.split(r'Trade Name:', expand=True)
# print(data)
waste_disposal['trade_name'] = data[1]
waste_disposal['business'] = data[0]
print(waste_disposal)
waste_disposal.to_csv("waste_disposal_df.csv", index=False)






## UNCOMMENT the following code to have one combined file of all the liceses.
## To combine all licenses data into one combined csv
# frame = [waste_disposal, laboratory, dispensaries, transporter, growers, processor]
# result = pd.concat(frame)
# print(result)
# result.to_csv("combined_data.csv", index=False)