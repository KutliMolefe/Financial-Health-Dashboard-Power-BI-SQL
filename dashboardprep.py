import pandas as pd
import numpy as np
import json

def convert_to_float(value):
    
    if pd.isna(value):
        return np.nan
    try:
        if isinstance(value, str):
         
            value = value.replace(' ', '').replace(',', '.')
        return float(value)
    except:
        return np.nan

def create_star_schema(df):
    
    numeric_cols = ['budget_amount', 'actual_amount', 'cost', 'revenue', 'claim_amount']
    for col in numeric_cols:
        df[col] = df[col].apply(convert_to_float)
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    
    fact_transactions = df[[
        'transaction_id', 'date', 'customer_id', 'product_category',
        'budget_amount', 'actual_amount', 'cost', 'revenue',
        'claim_flag', 'claim_amount', 'churn_flag', 'orders_count'
    ]].copy()
    
   
    try:
        fact_transactions['profit'] = fact_transactions['revenue'] - fact_transactions['cost']
        fact_transactions['variance'] = fact_transactions['actual_amount'] - fact_transactions['budget_amount']
        fact_transactions['variance_pct'] = np.where(
            fact_transactions['budget_amount'] != 0,
            fact_transactions['variance'] / fact_transactions['budget_amount'],
            np.nan
        )
    except Exception as e:
        print(f"Error in calculations: {e}")
        
        fact_transactions['profit'] = 0
        fact_transactions['variance'] = 0
        fact_transactions['variance_pct'] = 0
    
    dim_customers = df[[
        'customer_id', 'segment', 'region', 'country'
    ]].drop_duplicates()
    
    dim_date = pd.DataFrame({
        'date': pd.to_datetime(df['date'].unique()),
    })
    dim_date['day'] = dim_date['date'].dt.day
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['quarter'] = dim_date['date'].dt.quarter
    dim_date['month_year'] = dim_date['date'].dt.to_period('M')
    
    dim_products = pd.DataFrame({
        'product_category': df['product_category'].unique(),
    })
    dim_products['product_type'] = np.where(
        dim_products['product_category'].isin(['Insurance', 'Loans']),
        'Risk Products', 
        'Transactional Products'
    )
    
    return fact_transactions, dim_customers, dim_date, dim_products

def calculate_dax_metrics(fact_df, dim_customers):
    """Calculate all key metrics that will become DAX measures in Power BI"""
    
    try:
       
        insurance_mask = fact_df['product_category'] == 'Insurance'
        insurance_revenue = fact_df.loc[insurance_mask, 'revenue'].sum()
        loss_ratio = (fact_df.loc[insurance_mask, 'claim_amount'].sum() / 
                     insurance_revenue if insurance_revenue > 0 else 0)
        
        
        total_customers = max(1, dim_customers['customer_id'].nunique())
        
        metrics = {
            
            'Total Revenue': fact_df['revenue'].sum(),
            'Total Cost': fact_df['cost'].sum(),
            'Total Profit': fact_df['profit'].sum(),
            
            
            'Total Budget': fact_df['budget_amount'].sum(),
            'Total Actual Spend': fact_df['actual_amount'].sum(),
            'Avg Budget Variance': fact_df['variance'].mean(),
            'Avg Budget Variance %': fact_df['variance_pct'].mean(),
            
            
            'Loss Ratio': loss_ratio,
            
          
            'Total Customers': total_customers,
            'Customer Retention Rate': 1 - (fact_df['churn_flag'].sum() / total_customers),
            'Avg Orders per Customer': fact_df['orders_count'].sum() / total_customers,
            
           
            'Regional Risk Exposure': (fact_df.merge(dim_customers, on='customer_id')
                                     .groupby('region')['variance'].std().mean())
        }
        
        
        metrics['_dax_formulas'] = {
            'Loss Ratio': """
            Loss Ratio = 
            VAR total_claims = CALCULATE(SUM(fact_transactions[claim_amount]), 
                                  fact_transactions[product_category] = "Insurance")
            VAR total_premiums = CALCULATE(SUM(fact_transactions[revenue]), 
                                     fact_transactions[product_category] = "Insurance")
            RETURN DIVIDE(total_claims, total_premiums, 0)""",
            
            'Customer Retention Rate': """
            Retention Rate = 
            VAR total_customers = DISTINCTCOUNT(fact_transactions[customer_id])
            VAR churned_customers = CALCULATE(DISTINCTCOUNT(fact_transactions[customer_id]), 
                                       fact_transactions[churn_flag] = 1)
            RETURN 1 - DIVIDE(churned_customers, total_customers, 0)""",
            
            'Budget Variance %': """
            Budget Variance % = 
            DIVIDE(
                SUM(fact_transactions[actual_amount]) - SUM(fact_transactions[budget_amount]),
                SUM(fact_transactions[budget_amount]),
                0
            )"""
        }
        
        return metrics
    
    except Exception as e:
        print(f"Error calculating metrics: {e}")
        return {}

def main():
    
    try:
        df = pd.read_csv('cleaned_financial_data.csv')
        
        
        numeric_cols = ['budget_amount', 'actual_amount', 'cost', 'revenue', 'claim_amount']
        for col in numeric_cols:
            df[col] = df[col].apply(convert_to_float)
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
    except FileNotFoundError:
        print("Error: 'cleaned_financial_data.csv' not found. Please run the data cleaning script first.")
        return
    except Exception as e:
        print(f"Error loading data: {e}")
        return
    
    
    try:
        fact_transactions, dim_customers, dim_date, dim_products = create_star_schema(df)
    except Exception as e:
        print(f"Error creating star schema: {e}")
        return
   
    metrics = calculate_dax_metrics(fact_transactions, dim_customers)
    
    
    try:
        fact_transactions.to_csv('powerbi_fact_transactions.csv', index=False)
        dim_customers.to_csv('powerbi_dim_customers.csv', index=False)
        dim_date.to_csv('powerbi_dim_date.csv', index=False)
        dim_products.to_csv('powerbi_dim_products.csv', index=False)
        
        with open('powerbi_metrics.json', 'w') as f:
            json.dump(metrics, f, indent=2)
    except Exception as e:
        print(f"Error saving files: {e}")
        return
    
    

if __name__ == "__main__":
    main()