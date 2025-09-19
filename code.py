import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# read dataset
df=pd.read_csv('dirty_cafe_sales.csv')
print(df.to_string())

# check dataset before
print(df.head(10))
print('----------------------')
print(df.tail(10))
print('----------------------')
print(df.info())
print('----------------------')
print(df.isna().sum())
print('----------------------')
print(df.duplicated())
print('----------------------')

# cleaning & transformations
df['Quantity']=pd.to_numeric(df['Quantity'],errors='coerce')
df['Price Per Unit']=pd.to_numeric(df['Price Per Unit'],errors='coerce')
df['Total Spent']=pd.to_numeric(df['Total Spent'],errors='coerce')
df['Transaction Date']=pd.to_datetime(df['Transaction Date'],errors='coerce')
     
# standardization     
df['Transaction ID']=df['Transaction ID'].str.upper().str.strip()
df['Item']=df['Item'].str.title().str.strip()
df['Payment Method']=df['Payment Method'].str.title().str.strip()
df['Location']=df['Location'].str.title().str.strip()

# replace ['unknown','error'] values by nan
df['Item']=df['Item'].replace(['Unknown','Error'],np.nan)
df['Payment Method']=df['Payment Method'].replace(['Unknown','Error'],np.nan)
df['Location']=df['Location'].replace(['Unknown','Error'],np.nan)

# fill nan of items depend on price per unit
price_to_items = {
    3.0: ["Cake", "Juice"],
    2.0: ["Coffee"],
    1.0: ["Cookie"],
    5.0: ["Salad"],
    4.0: ["Sandwich", "Smoothie"],
    1.5: ["Tea"]
}
counters = {k: 0 for k in price_to_items}
def assign_item(row):
    if pd.isna(row["Item"]):
        price = row["Price Per Unit"]
        if price in price_to_items:
            items = price_to_items[price]
            idx = counters[price] % len(items)
            counters[price] += 1
            return items[idx]
    return row["Item"]
df["Item"] = df.apply(assign_item, axis=1)

# fill nan of item
np.random.seed(42)

prob_item = df['Item'].value_counts(normalize=True)

nan_count = df['Item'].isna().sum()

fill_items = np.random.choice(prob_item.index, size=nan_count, p=prob_item.values)

df.loc[df['Item'].isna(), 'Item'] = fill_items


# filling nan values
df['Quantity'] = df['Quantity'].fillna(df.groupby('Item')['Quantity'].transform('mean')).round().astype(int)

df['Price Per Unit'] = df['Price Per Unit'].fillna(df.groupby('Item')['Price Per Unit'].transform(lambda x: x.mode()[0]))

df['Total Spent']=df['Total Spent'].fillna(df['Quantity']*df['Price Per Unit']).round(1)

df['Total Spent']=df['Quantity']*df['Price Per Unit']


# fill nan of payment method
np.random.seed(42)

prob_per_item = df.groupby('Item')['Payment Method'].value_counts(normalize=True)

prob_dict = {item: prob_per_item[item] for item in prob_per_item.index.levels[0]}

nan_counts = df[df['Payment Method'].isna()]['Item'].value_counts()

fill_values = []

for item, count in nan_counts.items():
    dist = prob_dict[item]
    fill_values.extend(np.random.choice(dist.index, size=count, p=dist.values))

df.loc[df['Payment Method'].isna(), 'Payment Method'] = fill_values


# fill nan of location
np.random.seed(42)

prob_per_item_loc = df.groupby('Item')['Location'].value_counts(normalize=True)

prob_dict_loc = {item: prob_per_item_loc[item] for item in prob_per_item_loc.index.levels[0]}

nan_counts_loc = df[df['Location'].isna()]['Item'].value_counts()

fill_values_loc = []

for item, count in nan_counts_loc.items():
    dist = prob_dict_loc[item]
    fill_values_loc.extend(np.random.choice(dist.index, size=count, p=dist.values))

df.loc[df['Location'].isna(), 'Location'] = fill_values_loc


# cleaning
df=df.dropna(subset='Transaction Date')


# read dataset after cleaning & transformations
print(df.to_string())
print('----------------------')

# check dataset after cleaning
print(df.head(10))
print('----------------------')
print(df.tail(10))
print('----------------------')
print(df.info())
print('----------------------')
print(df.isna().sum())
print('----------------------')
print(df.duplicated())
print('----------------------')

# analysis
print('Total revenue for each item')
print(df.groupby('Item')['Total Spent'].sum().sort_values(ascending=False))
print('------------------------------------------')
print('Most popular products')
print(df.groupby('Item')['Quantity'].sum().sort_values(ascending=False))
print('------------------------------------------')
print('The highest item price ')
print(df.groupby('Item')['Price Per Unit'].unique().sort_values(ascending=False).head(1))
print('------------------------------------------')
print('The lowest item price ')
print(df.groupby('Item')['Price Per Unit'].unique().sort_values().head(1))
print('------------------------------------------')
print('The total revenue')
print(df['Total Spent'].sum())
print('------------------------------------------')
print('The most payment method used')
print(df['Payment Method'].value_counts().head(1))
print('--------------------------------------------')
print('The most location used')
print(df['Location'].value_counts().head(1))
print('--------------------------------------------')
print('This is most day has a high revenue')
print(df.groupby('Transaction Date')['Total Spent'].sum().sort_values(ascending=False).head(1))
print('---------------------------------------------')
print('The number of location for each item')
print(df.groupby('Item')['Location'].value_counts())
print('-----------------------------------------------')


# visualization
print('The total revenue for each item')
df.groupby('Item')['Total Spent'].sum().sort_values(ascending=False).plot(kind='bar')
plt.title('Total revenue for each item')
plt.xlabel('Item')
plt.ylabel('Total revenue')
plt.tight_layout()
plt.grid(True)
plt.show()

print('The popular products for each item')
df.groupby('Item')['Quantity'].sum().sort_values(ascending=False).plot(kind='pie',colors=['red','green','blue','orange','grey','yellow','pink','cyan'],autopct="%.1f%%")
plt.title('Most popular products')
plt.ylabel('')
plt.tight_layout()
plt.grid(True)
plt.show()


