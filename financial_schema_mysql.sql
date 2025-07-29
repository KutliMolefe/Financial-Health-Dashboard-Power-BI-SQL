
CREATE DATABASE FinancialHealthDB;
USE FinancialHealthDB;


CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,
    Date DATE NOT NULL,
    Day INT NOT NULL,
    MonthNumber INT NOT NULL,
    MonthName VARCHAR(20) NOT NULL,
    Quarter INT NOT NULL,
    Year INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayOfWeekName VARCHAR(10) NOT NULL,
    IsWeekend BOOLEAN NOT NULL,
    INDEX (Date),
    INDEX (Year, MonthNumber)
);


CREATE TABLE DimProduct (
    ProductKey INT AUTO_INCREMENT PRIMARY KEY,
    ProductName VARCHAR(50) NOT NULL,
    ProductCategory VARCHAR(50),
    CreatedDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    ModifiedDate DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX (ProductName)
);


CREATE TABLE DimSegment (
    SegmentKey INT AUTO_INCREMENT PRIMARY KEY,
    SegmentName VARCHAR(50) NOT NULL,
    SegmentDescription VARCHAR(100),
    CreatedDate DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX (SegmentName)
);


CREATE TABLE DimCountry (
    CountryKey INT AUTO_INCREMENT PRIMARY KEY,
    CountryName VARCHAR(50) NOT NULL,
    CountryCode VARCHAR(3),
    Region VARCHAR(50),
    INDEX (CountryName),
    INDEX (Region)
);


CREATE TABLE DimDiscountBand (
    DiscountBandKey INT AUTO_INCREMENT PRIMARY KEY,
    BandName VARCHAR(20) NOT NULL,
    MinDiscount DECIMAL(5,2) DEFAULT 0.00,
    MaxDiscount DECIMAL(5,2) DEFAULT 0.00,
    INDEX (BandName)
);


CREATE TABLE FactSales (
    SalesKey INT AUTO_INCREMENT PRIMARY KEY,
    DateKey INT NOT NULL,
    ProductKey INT NOT NULL,
    SegmentKey INT NOT NULL,
    CountryKey INT NOT NULL,
    DiscountBandKey INT NOT NULL,
    UnitsSold DECIMAL(18,2) NOT NULL,
    ManufacturingPrice DECIMAL(18,2) NOT NULL,
    SalePrice DECIMAL(18,2) NOT NULL,
    GrossSales DECIMAL(18,2) NOT NULL,
    Discounts DECIMAL(18,2) NOT NULL DEFAULT 0.00,
    NetSales DECIMAL(18,2) NOT NULL,
    COGS DECIMAL(18,2) NOT NULL,
    Profit DECIMAL(18,2) NOT NULL,
    RecordCreated DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    FOREIGN KEY (SegmentKey) REFERENCES DimSegment(SegmentKey),
    FOREIGN KEY (CountryKey) REFERENCES DimCountry(CountryKey),
    FOREIGN KEY (DiscountBandKey) REFERENCES DimDiscountBand(DiscountBandKey),
    INDEX (DateKey, ProductKey),
    INDEX (SegmentKey, CountryKey)
);


CREATE VIEW vw_FinancialKPIs AS
SELECT 
    dd.Year,
    dd.MonthName,
    ds.SegmentName,
    dc.CountryName,
    dp.ProductName,
    SUM(fs.NetSales) AS TotalSales,
    SUM(fs.Profit) AS TotalProfit,
    SUM(fs.COGS) AS TotalCOGS,
    SUM(fs.Discounts) AS TotalDiscounts,
    CASE 
        WHEN SUM(fs.NetSales) = 0 THEN 0 
        ELSE SUM(fs.Profit)/SUM(fs.NetSales) 
    END AS ProfitMargin,
    CASE 
        WHEN SUM(fs.GrossSales) = 0 THEN 0 
        ELSE SUM(fs.Discounts)/SUM(fs.GrossSales) 
    END AS DiscountPercentage,
    CASE 
        WHEN SUM(fs.NetSales) = 0 THEN 0 
        ELSE SUM(fs.COGS)/SUM(fs.NetSales) 
    END AS LossRatio
FROM 
    FactSales fs
    JOIN DimDate dd ON fs.DateKey = dd.DateKey
    JOIN DimSegment ds ON fs.SegmentKey = ds.SegmentKey
    JOIN DimCountry dc ON fs.CountryKey = dc.CountryKey
    JOIN DimProduct dp ON fs.ProductKey = dp.ProductKey
GROUP BY 
    dd.Year, dd.MonthName, ds.SegmentName, dc.CountryName, dp.ProductName;