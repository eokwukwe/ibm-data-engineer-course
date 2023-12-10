-- Create a grouping sets query using the columns country, category, totalsales.
SELECT
    dcnt.countryid,
    dcat.categoryid,
    SUM(fsl.amount) AS TotalSales
FROM
    "FactSales" fsl
    INNER JOIN "DimCountry" dcnt ON fsl.countryid = dcnt.countryid
    INNER JOIN "DimCategory" dcat ON fsl.categoryid = dcat.categoryid
GROUP BY
    GROUPING SETS(
    	dcnt.countryid,
    	dcat.categoryid
    );


-- Create a rollup query using the columns year, country, and totalsales.
SELECT 
    dd.Year, 
    dc.country,
    SUM(fsl.amount) AS TotalSales
FROM 
    "FactSales" fsl
    INNER JOIN "DimDate" dd ON fsl.dateid = dd.dateid
    INNER JOIN "DimCountry" dc ON fsl.countryid = dc.countryid
GROUP BY ROLLUP (dd.Year, dc.country)
ORDER BY 
    dd.Year, 
    dc.country


-- Create a cube query using the columns year, country, and average sales.
SELECT 
    dd.Year, 
    dc.country,
    AVG(fsl.amount) AS AverageSales
FROM 
    "FactSales" fsl
    INNER JOIN "DimDate" dd ON fsl.dateid = dd.dateid
    INNER JOIN "DimCountry" dc ON fsl.countryid = dc.countryid
GROUP BY CUBE (dd.Year, dc.country)
ORDER BY 
    dd.Year, 
    dc.country


-- Create an MQT named total_sales_per_country that has the columns country and total_sales.
CREATE MATERIALIZED VIEW total_sales_per_country AS
SELECT
    dc.country,
    SUM(fsl.amount) AS TotalSales
FROM 
    "FactSales" fsl
    INNER JOIN "DimCountry" dc ON fsl.countryid = dc.countryid
GROUP BY
	dc.country
ORDER BY
    TotalSales DESC