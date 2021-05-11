import pandas as pd
import glob

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
license_type = "license_type"


# Manipulating waste_disposal20210507.csv
waste_disposal = pd.read_csv("waste_disposal20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
waste_disposal['License_type'] = "waste_disposal"
waste_disposal = waste_disposal[waste_disposal['0'].notnull()]
waste_disposal.drop('1', inplace=True, axis=1)
waste_disposal.drop('2', inplace=True, axis=1)
waste_disposal.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
waste_disposal = waste_disposal.replace('\n','', regex=True)
waste_disposal['zip'] = waste_disposal['zip'].astype("int64")
data=waste_disposal['business'].str.split(r'Trade Name:', expand=True)
# print(data)
waste_disposal['trade_name'] = data[1]
waste_disposal['business'] = data[0]
print(waste_disposal)
# waste_disposal.to_csv("waste_disposal.csv", index=False)

# Manipulating dispensaries20210507.csv
dispensaries = pd.read_csv("dispensaries20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
dispensaries['License_type'] = "dispensaries"
dispensaries = dispensaries[dispensaries['0'].notnull()]
# print(dispensaries)
dispensaries.drop('7', inplace=True, axis=1)
dispensaries.drop('8', inplace=True, axis=1)
dispensaries.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
dispensaries = dispensaries.replace('\n','', regex=True)
dispensaries['zip'] = dispensaries['zip'].astype("int64")
data=dispensaries['business'].str.split(r'Trade Name:', expand=True)
# print(data)
dispensaries['trade_name'] = data[1]
dispensaries['business'] = data[0]
# # print(dispensaries)
# dispensaries.to_csv("dispensaries.csv", index=False)


# Manipulating growers20210507.csv
growers = pd.read_csv("growers20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
growers['License_type'] = "growers"
growers = growers[growers['0'].notnull()]
growers.drop('7', inplace=True, axis=1)
growers.drop('8', inplace=True, axis=1)
growers.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
growers = growers.replace('\n','', regex=True)
growers['zip'] = growers['zip'].astype("int64")
data=growers['business'].str.split(r'Trade Name:', expand=True)
# print(data)
growers['trade_name'] = data[1]
growers['business'] = data[0]
print(growers)
# growers.to_csv("growers.csv", index=False)

# Manipulating transporter20210507.csv
transporter = pd.read_csv("transporter20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
transporter['License_type'] = "transporter"
transporter = transporter[transporter['0'].notnull()]
transporter.drop('7', inplace=True, axis=1)
transporter.drop('8', inplace=True, axis=1)
transporter.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
transporter = transporter.replace('\n','', regex=True)
transporter['zip'] = transporter['zip'].astype("int64")
data=transporter['business'].str.split(r'Trade Name:', expand=True)
# print(data)
transporter['trade_name'] = data[1]
transporter['business'] = data[0]
print(transporter)
# transporter.to_csv("transporter.csv", index=False)


# Manipulating processor20210507.csv
processor = pd.read_csv("processor20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)
processor['License_type'] = "processor"
processor = processor[processor['0'].notnull()]
processor.drop('7', inplace=True, axis=1)
processor.drop('8', inplace=True, axis=1)
processor.columns=['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']
processor = processor.replace('\n','', regex=True)
processor['zip'] = processor['zip'].astype("int64")
data=processor['business'].str.split(r'Trade Name:', expand=True)
# print(data)
processor['trade_name'] = data[1]
processor['business'] = data[0]
print(processor)
# processor.to_csv("processor.csv", index=False)


# Manipulating laboratory20210507.csv
laboratory = pd.read_csv("laboratory20210507.csv").drop(['Unnamed: 0', 'Unnamed: 0.1'],axis=1)

laboratory['License_type'] = "laboratory"

laboratory = laboratory[laboratory['0'].notnull()]

laboratory.columns = ['business', 'license', 'email', 'phone', 'city', 'zip', 'county', 'license_type']

laboratory = laboratory.replace('\n','', regex=True)

laboratory['zip'] = laboratory['zip'].astype("int64")

data=laboratory['business'].str.split(r'Trade Name:', expand=True)
# print(data)
laboratory['trade_name'] = data[1]

laboratory['business'] = data[0]

print(laboratory)
# laboratory.to_csv("laboratory.csv", index=False)

frames = [waste_disposal, processor, transporter, growers, dispensaries, laboratory]
result = pd.concat(frames)
print(result)

# result.to_csv("combined_data.csv", index=False)

