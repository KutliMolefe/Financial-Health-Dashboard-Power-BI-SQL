import pandas as pd
import numpy as np


df = pd.read_csv("FinancialSample.csv", sep=';', encoding='utf-8')
df.columns = [col.strip() for col in df.columns]
print("Cleaned column names:", df.columns.tolist())
numeric_cols = ['Units Sold', 'Manufacturing Price', 'Sale Price', 'Gross Sales',
                'Discounts', 'Sales', 'COGS', 'Profit']
def clean_numeric(value):
    if pd.isna(value):
        return np.nan
    value = str(value).strip()

    if value in ['-', '$-', '']:
        return np.nan

    value = value.replace('$', '').replace(' ', '')
    value = value.replace(',', '.')
    if value.startswith('(') and value.endswith(')'):
        value = '-' + value[1:-1]
    try:
        return float(value)
    except ValueError:
        return np.nan

for col in numeric_cols:
    df[col] = df[col].apply(clean_numeric)


df.to_csv("cleaned_financial_data.csv", index=False)
print("Data cleaned and saved to 'cleaned_financial_data.csv'")
