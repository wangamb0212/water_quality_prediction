import pandas as pd
import numpy as np
from geopy.distance import geodesic
from fancyimpute import KNN
df2 = pd.read_csv('Drinking_Water_Quality_Distribution_Monitoring_Data.csv')
df1 = pd.read_csv('DrinkingFountains.csv')
df = pd.read_excel('Water_Quality_Sampling_Sites.xlsx')
df11 = df1[['SIGNNAME','lon','lat']]
df22 = df[['Sample Site','lat','lon']]


result = pd.DataFrame([],columns=['SIGNNAME','lon','lat','Sample Site','lat','lon','dis'])
print(df11.shape, df22.shape)

# iterate over all points to calculate distance
for i in range(df11.shape[0]):
    for j in range(df22.shape[0]):
        p1 = df11.iloc[i,:]
        p2 = df22.iloc[j,:]
        # calculate distance
        dis = geodesic((p1['lon'], p1['lat']), (p2['lon'],p2['lat'])).km

        pp = pd.concat([p1.T,p2.T], axis=0)
        pp['dis'] = dis
        ppp = pd.DataFrame(pp)

        result = result.append(ppp.T)

# set an empty dataFrame to store result
ddd = pd.DataFrame(columns=result.columns)

# select the point with minimal distance
dd = df.groupby(['SIGNNAME','lon','lat'], as_index=False)['dis'].min()
print(dd)

for i in range(dd.shape[0]):
    dd1 = dd.iloc[i,:]
    print(dd1)
    dd2 = df[(result['SIGNNAME']==dd1['SIGNNAME'])&(result['dis']==dd1['dis'])]
    print(dd2)
    ddd = ddd.append(dd2)
    print(ddd.tail)
ddd.to_excel('result.xlsx')

df2['Sample Date'] = pd.to_datetime(df2['Sample Date'])
df2 = df2.sort_values(by='Sample Date')
dff = df2[['Sample Date', 'Sample Site','Residual Free Chlorine (mg/L)', 'Turbidity (NTU)','Fluoride (mg/L)']]
dff.columns = ['date','Site','RFC','TUR','FLU']
# pair sampling site with drinking fountains
dfff = pd.merge(dff, ddd, left_on='Site', right_on='Sample Site')
# join lat and long
dfff['SIGNNAME'] = dfff['SIGNNAME'] + '-' + dfff['lon'].astype(str) + '_' + dfff['lat'].astype(str)

dfff = dfff[['date','SIGNNAME', 'RFC', 'TUR', 'FLU']]
# change dtype to float
dfff[['RFC','TUR', 'FLU']] = dfff[['RFC','TUR', 'FLU']].astype(float)

# pivot data
dff2 = pd.pivot_table(dfff,index='date',columns='SIGNNAME')
dff2.columns = ['_'.join(col) for col in dff2.columns.values]

# add average temperature and prcp
df1 = pd.read_csv('df_prcp.csv')
df2 = pd.read_csv('df_tavg.csv')
df = pd.merge(df1, df2, left_on='date', right_on='date')
df = df[['date', 'prcp', 'tavg']]
df['date'] = pd.to_datetime(df['date'])
data = pd.merge(dff2, df, left_on=dff2.index, right_on='date')
data.index = data['date']

# imputation with KNN

# Get list of columns with missing values
missing_cols = data.columns[data.isna().any()].tolist()

# Create an instance of the KNN imputer
imputer = KNN()

# Fit and transform the imputer to the dataset
imputed_array = imputer.fit_transform(data[missing_cols])

# # Replace the missing values in the original dataset
# data[missing_cols] = imputed_array

data.to_csv('DrinkingDataFinal.csv')
