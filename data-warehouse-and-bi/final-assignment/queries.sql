-- Create a grouping sets query using the columns stationid, trucktype, total waste collected.
SELECT
    s.City,
    t.TruckType,
    SUM(f.Wastecollected) AS TotalWastecollected
FROM
    "FactTrip" f
    LEFT JOIN "DimTruck" t ON f.Truckid = t.Truckid
    LEFT JOIN "DimStation" s ON f.Stationid = s.Stationid
GROUP BY
    GROUPING SETS(
        -- total waste collected for each combination of station and truck type.
    	(s.City, t.TruckType),
    	--  total waste collected for each station across all truck types.
    	(s.City),
    	-- total waste collected for each truck type across all stations
    	(t.TruckType),
    	--  total aggregate (sum) of Wastecollected across the entire dataset
    	()
    );

    -- OR--
SELECT
    s.Stationid,
    t.TruckType,
    SUM(f.Wastecollected) AS TotalWastecollected
FROM
    "FactTrip" f
    LEFT JOIN "DimTruck" t ON f.Truckid = t.Truckid
    LEFT JOIN "DimStation" s ON f.Stationid = s.Stationid
GROUP BY
    GROUPING SETS(
    	s.stationid,
    	t.trucktype
    );


-- Create a rollup query using the columns year, city, stationid, and total waste collected.
SELECT 
    d.Year, 
    s.City, 
    f.Stationid, 
    SUM(f.Wastecollected) AS TotalWasteCollected
FROM 
    "FactTrip" f
    LEFT JOIN "DimDate" d ON f.Dateid = d.dateid
    LEFT JOIN "DimStation" s ON f.Stationid = s.Stationid
GROUP BY ROLLUP (d.Year, s.City, f.Stationid)
ORDER BY 
    d.Year, 
    s.City, 
    f.Stationid;


-- Create a cube query using the columns year, city, stationid, and average waste collected.
SELECT 
    d.Year, 
    s.City, 
    f.Stationid, 
    AVG(f.Wastecollected) AS AverageWasteCollected
FROM 
    "FactTrip" f
    LEFT JOIN "DimDate" d ON f.Dateid = d.dateid
    LEFT JOIN "DimStation" s ON f.Stationid = s.Stationid
GROUP BY CUBE (d.Year, s.City, f.Stationid)
ORDER BY 
    d.Year, 
    s.City, 
    f.Stationid;


-- Create an MQT named max_waste_stats using the columns city, stationid, trucktype, and max waste collected.
CREATE MATERIALIZED VIEW max_waste_stats AS
SELECT
    s.City,
    f.Stationid,
    t.TruckType,
    MAX(f.Wastecollected) AS MaxWasteCollected
FROM
    "FactTrip" f
    LEFT JOIN "DimStation" s ON f.Stationid = s.Stationid
    LEFT JOIN "DimTruck" t ON f.Truckid = t.Truckid
GROUP BY
    s.City,
    f.Stationid,
    t.TruckType
ORDER BY
    s.City,
    f.Stationid,
    t.TruckType;
