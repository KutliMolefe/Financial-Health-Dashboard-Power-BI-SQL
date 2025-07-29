import pandas as pd
import numpy as np
import re

def debug_file_structure(filepath):
    """Helper function to examine file structure"""
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        print("First 3 lines of file:")
        for i in range(3):
            print(f"Line {i+1}: {f.readline().strip()}")
    
    try:
        test_df = pd.read_csv(filepath, delimiter=';', nrows=5, encoding='utf-8-sig')
        print("\nDataFrame columns:", test_df.columns.tolist())
        print("\nDataFrame head:")
        print(test_df.head())
    except Exception as e:
        print(f"\nError reading file: {e}")

def clean_currency(value):
    """Helper function to clean currency values"""
    if pd.isna(value) or value == '':
        return 0.0
    
    
    is_negative = '-' in str(value)
    
   
    cleaned = re.sub(r'[^\d.,]', '', str(value))
    
    
    if ',' in cleaned and '.' in cleaned:
       
        cleaned = cleaned.replace('.', '').replace(',', '.')
    elif ',' in cleaned:
      
        cleaned = cleaned.replace(',', '.')
  
    try:
        result = float(cleaned)
        return -result if is_negative else result
    except:
        return 0.0

def clean_financial_data(df):
    """Robust cleaning function with error handling"""
    try:
       
        df.columns = df.columns.str.strip()
        
       
        col_standardization = {
            'Manufacturing Price': ['Manufacturing Price', 'ManufacturingPrice', 'Manuf Price'],
            'Sale Price': ['Sale Price', 'SalePrice', 'Sales Price'],
            'Gross Sales': ['Gross Sales', 'GrossSales'],
            'Discounts': ['Discounts', 'Discount'],
            'Sales': ['Sales', 'Net Sales'],
            'COGS': ['COGS', 'Cost of Goods Sold'],
            'Profit': ['Profit', 'Net Profit'],
            'Units Sold': ['Units Sold', 'UnitsSold', 'Quantity'],
            'Date': ['Date', 'Sale Date'],
            'Discount Band': ['Discount Band', 'DiscountBand'],
            'Month Number': ['Month Number', 'MonthNum'],
            'Month Name': ['Month Name', 'Month'],
            'Year': ['Year', 'Calendar Year']
        }
        
        for standard_name, variants in col_standardization.items():
            for variant in variants:
                if variant in df.columns:
                    df.rename(columns={variant: standard_name}, inplace=True)
        
      
        required_cols = ['Segment', 'Country', 'Product', 'Discount Band', 
                        'Units Sold', 'Manufacturing Price', 'Sale Price', 
                        'Gross Sales', 'Discounts', 'Sales', 'COGS', 'Profit',
                        'Date', 'Month Number', 'Month Name', 'Year']
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            available_cols = df.columns.tolist()
            raise ValueError(
                f"Missing required columns: {missing_cols}\n"
                f"Available columns: {available_cols}"
            )
        
      
        text_cols = ['Segment', 'Country', 'Product', 'Discount Band', 'Month Name']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        
      
        numeric_cols = ['Units Sold', 'Month Number', 'Year']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
        
       
        currency_cols = ['Manufacturing Price', 'Sale Price', 'Gross Sales', 
                        'Discounts', 'Sales', 'COGS', 'Profit']
        
        for col in currency_cols:
            df[col] = df[col].apply(clean_currency)
        
        
        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
        
        
        if 'Discount Band' in df.columns:
            df['Discount Band'] = df['Discount Band'].replace(['', 'nan', 'NaN'], 'None')
        
        return df
    
    except Exception as e:
        print(f"Error during cleaning: {e}")
        raise

def load_to_mysql(cleaned_df, host, user, password, database):
    from sqlalchemy import create_engine
    
    
    engine = create_engine(
        "mysql+pymysql://root:Kutli%402002@127.0.0.1/financialhealthdb"
    )
    
    
    print("\n=== Loading Dimension Tables ===")
    
    
    products = cleaned_df[['Product']].drop_duplicates()
    products = products.rename(columns={'Product': 'ProductName'})
    products.to_sql('dimproduct', engine, if_exists='append', index=False)
    print(f"Loaded {len(products)} products")
    
   
    segments = cleaned_df[['Segment']].drop_duplicates()
    segments = segments.rename(columns={'Segment': 'SegmentName'})
    segments.to_sql('dimsegment', engine, if_exists='append', index=False)
    print(f"Loaded {len(segments)} segments")
    
   
    countries = cleaned_df[['Country']].drop_duplicates()
    countries = countries.rename(columns={'Country': 'CountryName'})
    countries.to_sql('dimcountry', engine, if_exists='append', index=False)
    print(f"Loaded {len(countries)} countries")
    
    
    discount_bands = cleaned_df[['Discount Band']].drop_duplicates()
    discount_bands = discount_bands.rename(columns={'Discount Band': 'BandName'})
    discount_bands.to_sql('dimdiscountband', engine, if_exists='append', index=False)
    print(f"Loaded {len(discount_bands)} discount bands")
    
    
    dates = cleaned_df[['Date', 'Month Number', 'Month Name', 'Year']].drop_duplicates()
    dates['DateKey'] = dates['Date'].dt.strftime('%Y%m%d').astype(int)
    dates['Day'] = dates['Date'].dt.day
    dates['Quarter'] = dates['Date'].dt.quarter
    dates['DayOfWeek'] = dates['Date'].dt.dayofweek
    dates['DayOfWeekName'] = dates['Date'].dt.day_name()
    dates['IsWeekend'] = dates['DayOfWeek'].isin([5,6])
    dates = dates.rename(columns={
        'Month Number': 'MonthNumber',
        'Month Name': 'MonthName'
    })
    
    with engine.connect() as conn:
        existing_dates = pd.read_sql("SELECT DateKey FROM dimdate", conn)['DateKey'].tolist()
    
    
    new_dates = dates[~dates['DateKey'].isin(existing_dates)]
    
    if not new_dates.empty:
        new_dates[['DateKey', 'Date', 'Day', 'MonthNumber', 'MonthName', 
                 'Quarter', 'Year', 'DayOfWeek', 'DayOfWeekName', 'IsWeekend']]\
            .to_sql('dimdate', engine, if_exists='append', index=False)
        print(f"Loaded {len(new_dates)} new dates")
    else:
        print("All dates already exist in dimdate table")
    
    
    print("\n=== Loading Fact Table ===")
    
   
    fact_data = []
    for _, row in cleaned_df.iterrows():
        date_key = pd.to_datetime(row['Date']).strftime('%Y%m%d')
        
        
        product_key = pd.read_sql(
            f"SELECT ProductKey FROM DimProduct WHERE ProductName = '{row['Product']}'", 
            engine
        ).iloc[0,0]
        
        segment_key = pd.read_sql(
            f"SELECT SegmentKey FROM DimSegment WHERE SegmentName = '{row['Segment']}'", 
            engine
        ).iloc[0,0]
        
        country_key = pd.read_sql(
            f"SELECT CountryKey FROM DimCountry WHERE CountryName = '{row['Country']}'", 
            engine
        ).iloc[0,0]
        
        discount_band_key = pd.read_sql(
            f"SELECT DiscountBandKey FROM DimDiscountBand WHERE BandName = '{row['Discount Band']}'", 
            engine
        ).iloc[0,0]
        
        fact_data.append({
            'DateKey': date_key,
            'ProductKey': product_key,
            'SegmentKey': segment_key,
            'CountryKey': country_key,
            'DiscountBandKey': discount_band_key,
            'UnitsSold': row['Units Sold'],
            'ManufacturingPrice': row['Manufacturing Price'],
            'SalePrice': row['Sale Price'],
            'GrossSales': row['Gross Sales'],
            'Discounts': row['Discounts'],
            'NetSales': row['Sales'],
            'COGS': row['COGS'],
            'Profit': row['Profit']
        })
    
  
    fact_df = pd.DataFrame(fact_data)
    fact_df.to_sql('FactSales', engine, if_exists='append', index=False)
    print(f"Loaded {len(fact_df)} fact records")
    
    print("\n=== Data Loading Complete ===")

if __name__ == "__main__":
    input_file = 'financial_raw.csv'
    
    print("=== Debugging File Structure ===")
    debug_file_structure(input_file)
    
    print("\n=== Attempting to Load and Clean Data ===")
    try:
       
        df = pd.read_csv(input_file, delimiter=';', encoding='utf-8-sig')
        print("File read successfully with semicolon delimiter")
        
       
        cleaned_df = clean_financial_data(df)
        
       
        cleaned_df.to_csv('cleaned_financial_data.csv', index=False)
        print("\nData cleaned and saved successfully!")
        
       
        print("\nCleaned data summary:")
        print(cleaned_df.head())
        print("\nData types:")
        print(cleaned_df.dtypes)
        print("\nMissing values per column:")
        print(cleaned_df.isnull().sum())

       
        try:
            load_to_mysql(
                cleaned_df,
                host='127.0.0.1',       
                user='root',            
                database='financialhealthdb',
                password='Kutli@2002' 
            )
        except Exception as mysql_error:
            print(f"\nMySQL loading error: {str(mysql_error)}")
            print("\nTroubleshooting steps:")
            print("1. Make sure MySQL server is running")
            print("2. Verify your connection parameters:")
            print("   - Host: localhost (try '127.0.0.1' if this fails)")
            print("   - User: root")
            print("   - Database: financialhealthdb")
            print("3. Check MySQL user permissions")
            print("4. Try connecting with MySQL Workbench first to verify credentials")
            
            
            cleaned_df.to_csv('cleaned_financial_data_fallback.csv', index=False)
            print("\nSaved cleaned data to 'cleaned_financial_data_fallback.csv'")
            
    except Exception as e:
        print(f"\nFailed to process file: {e}")
        print("\nSuggestions:")
        print("1. Verify the file uses semicolon (;) as delimiter")
        print("2. Check for hidden characters or BOM markers")
        print("3. Ensure all required columns are present")
        print("4. Check for consistent data formats in each column")