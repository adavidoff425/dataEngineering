import pandas as pd

census_df = pd.read_csv('2017census.csv', usecols=['State', 'County', 'TotalPop', 'IncomePerCap', 'Poverty'])
covid_df = pd.read_csv('COVID_county_data.csv', usecols=['date', 'County', 'State', 'cases', 'deaths'])
covid_df.set_index('date', inplace=True)

totalPop_df = census_df.groupby(['State', 'County']).agg({'TotalPop':'sum'})
'''
for i in census_df.index:
  loc = (census_df['State'][i], census_df['County'][i])
  weight = census_df['TotalPop'][i]/(totalPop_df.loc[loc]['TotalPop'])
  census_df['IncomePerCap'][i] = census_df['IncomePerCap'][i] * weight
  povNum = (census_df['Poverty'][i] / 100) * census_df['TotalPop'][i]
  census_df['Poverty'][i] = 100 * (povNum / totalPop_df.loc[loc]['TotalPop'])
'''
acs_df = census_df.groupby(['State', 'County']).agg({'TotalPop':'sum', 'IncomePerCap':'sum', 'Poverty':'sum'})

covidTotal_df = covid_df.groupby(['State', 'County']).agg({'cases':'sum', 'deaths':'sum'})

dec_df = covid_df.loc['2020-12-01':'2020-12-31']
decTotal_df = dec_df.groupby(['State', 'County']).agg({'cases':'sum', 'deaths':'sum'})

cov_df = covidTotal_df.join(decTotal_df, lsuffix='_total', rsuffix='_dec')

perHun = 100000 / acs_df['TotalPop'] 
cov_df['cases_total'] = cov_df['cases_total'] * perHun
cov_df['cases_dec'] = cov_df['cases_dec'] * perHun
cov_df['deaths_total'] = cov_df['deaths_total'] * perHun
cov_df['deaths_dec'] = cov_df['deaths_dec'] * perHun
df = acs_df.join(cov_df)
or_df = df.loc['Oregon']

print('a: ' + str(df['cases_total'].corr(df['Poverty'])))
print('b: ' + str(df['deaths_total'].corr(df['Poverty'])))
print('c: ' + str(df['cases_total'].corr(df['IncomePerCap'])))
print('d: ' + str(df['deaths_total'].corr(df['IncomePerCap'])))
print('e: ' + str(df['cases_dec'].corr(df['Poverty'])))
print('f: ' + str(df['deaths_dec'].corr(df['Poverty'])))
print('g: ' + str(df['cases_dec'].corr(df['IncomePerCap'])))
print('h: ' + str(df['deaths_dec'].corr(df['IncomePerCap'])))

print('a: ' + str(or_df['cases_total'].corr(or_df['Poverty'])))
print('b: ' + str(or_df['deaths_total'].corr(or_df['Poverty'])))
print('c: ' + str(or_df['cases_total'].corr(or_df['IncomePerCap'])))
print('d: ' + str(or_df['deaths_total'].corr(or_df['IncomePerCap'])))
print('e: ' + str(or_df['cases_dec'].corr(or_df['Poverty'])))
print('f: ' + str(or_df['deaths_dec'].corr(or_df['Poverty'])))
print('g: ' + str(or_df['cases_dec'].corr(or_df['IncomePerCap'])))
print('h: ' + str(or_df['deaths_dec'].corr(or_df['IncomePerCap'])))
