import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
# %matplotlib inline
from urllib.request import urlopen
from bs4 import BeautifulSoup
from pylab import rcParams
rcParams['figure.figsize'] = 15, 5

url = "http://www.hubertiming.com/results/2017GPTR10K"
html = urlopen(url)

soup = BeautifulSoup(html, 'lxml')
type(soup)

title = soup.title
print(title)

text = soup.get_text()
#print(soup.text)

soup.find_all('a')
#all_links = soup.find_all("a")
#for link in all_links:
#  print(link.get("href"))
rows = soup.find_all('tr')
#print(rows[:10])
list_rows = []
for row in rows:
  cells = row.find_all('td')
  str_cells = str(cells)
  clean = re.compile('<.*?>')
  clean2 = (re.sub(clean, '', str_cells))
  list_rows.append(clean2)
#print(list_rows)
type(list_rows)
#cleantext = BeautifulSoup(str_cells, 'lxml').get_text()
#print(cleantext)

df = pd.DataFrame(list_rows)
#print(df.head(10))

df1 = df[0].str.split(',', expand=True)
#print(df1.head(10))

col_labels = soup.find_all('th')
all_header = []
col_str = str(col_labels)
cleantext = BeautifulSoup(col_str, "lxml").get_text()
all_header.append(cleantext)
print(all_header)

df2 = pd.DataFrame(all_header)
print(df2.head())

df3 = df2[0].str.split(',', expand=True)
print(df3.head())

frames = [df3, df1]
df4 = pd.concat(frames)
print(df4.head(10))

df5 = df4.rename(columns=df4.iloc[0])
print(df5.head())
print(df5.info())
print(df5.shape)

df6 = df5.dropna(axis=0, how='any')
print(df6.info())
print(df6.shape)

df7 = df6.drop(df6.index[0])
print(df7.head())

df7.rename(columns={'[Place': 'Place'}, inplace=True)
df7.rename(columns={' Team]': 'Team'}, inplace=True)
print(df7.head())

df7['Team'] = df7['Team'].str.strip(']')
print(df7.head())

time_list = df7[' Chip Time'].tolist()
#print(time_list)
time_mins = []
for i in time_list:
  time = i.split(':')
  if len(time) == 2:
    m, s = time
    h = 0
  elif len(time) == 3:
    h, m, s = time
  math = (int(h) * 3600 + int(m) * 60 + int(s))/60
  time_mins.append(math)
#print(time_mins)

df7['Runner_mins'] = time_mins
print(df7.head())

print(df7.describe(include=[np.number]))

df7.boxplot(column='Runner_mins')
plt.grid(True, axis='y')
plt.ylabel('Chip Time')
plt.xticks([1], ['Runners'])
plt.show()

x = df7['Runner_mins']
ax = sns.distplot(x, hist=True, kde=True, rug=False, color='m', bins=25, hist_kws={'edgecolor':'black'})
plt.show()

f_fuko = df7.loc[df7[' Gender']==' F']['Runner_mins']
m_fuko = df7.loc[df7[' Gender']==' M']['Runner_mins']
sns.distplot(f_fuko, hist=True, kde=True, rug=False, hist_kws={'edgecolor':'black'}, label='Female')
sns.distplot(m_fuko, hist=False, kde=True, rug=False, hist_kws={'edgecolor':'black'}, label='Male')
plt.legend()

g_stats = df7.groupby(" Gender", as_index=True).describe()
print(g_stats)

df7.boxplot(column='Runner_mins', by=' Gender')
plt.ylabel('Chip Time')
plt.suptitle("")

