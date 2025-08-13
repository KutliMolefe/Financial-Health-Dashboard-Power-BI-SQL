import pandas as pd
import numpy as np
from locale import setlocale, LC_NUMERIC, atof
import locale


try:
    setlocale(LC_NUMERIC, 'en_ZA.UTF-8')
except:
    setlocale(LC_NUMERIC, '')

def clean_numeric(value):
    """Convert string with comma decimal to float"""
    if pd.isna(value):
        return np.nan
    if isinstance(value, str):
        
        value = value.replace('NA ', '').strip()
       
        value = value.replace(' ', '')
        
        return float(value.replace(',', '.'))
    return float(value)


df = pd.read_csv('financial_health_unclean.csv')  


df['region'] = df['region'].str.replace('Westrn Cape', 'Western Cape')


df['customer_id'] = df['customer_id'].fillna('ANONYMOUS')


numeric_cols = ['budget_amount', 'actual_amount', 'cost', 'revenue', 'claim_amount']

for col in numeric_cols:
    
    df[col] = df[col].astype(str)
   
    df[col] = df[col].apply(clean_numeric)
   
    df[col] = df[col].fillna(df[col].median())


df['date'] = pd.to_datetime(df['date'], errors='coerce')

most_recent_date = df['date'].max()
df['date'] = df['date'].fillna(most_recent_date)


categorical_cols = ['segment', 'product_category', 'country']
for col in categorical_cols:
    df[col] = df[col].str.strip().str.title()  
    df[col] = df[col].fillna(df[col].mode()[0])  


df['location'] = df['region'] + ', ' + df['country']


df['claim_flag'] = df['claim_flag'].astype(int)
df['churn_flag'] = df['churn_flag'].astype(int)


df = df.drop_duplicates(subset='transaction_id', keep='first')


for col in ['budget_amount', 'actual_amount', 'cost', 'revenue']:
    df[col] = df[col].abs()  


def format_sa_number(x):
    try:
        return locale.format_string('%.2f', x, grouping=True)
    except:
        return x


df.to_csv('cleaned_financial_data.csv', index=False)


display_df = df.copy()
for col in numeric_cols:
    display_df[col] = display_df[col].apply(format_sa_number)

display_df.to_csv('display_financial_data.csv', index=False)

