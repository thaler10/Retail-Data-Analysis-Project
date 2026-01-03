/*
Employee Segmentation
Objective: Generate a detailed roster of all employees, including the total size of their respective departments.
*/

SELECT
DISTINCT device_id,
role,
-- Utilizing a Window Function to display the total department headcount alongside each individual employee record
COUNT(DISTINCT device_id) OVER(PARTITION BY role) as total_in_department

FROM `bqproj-435911.Final_Project_2025.geolocation`

WHERE role IN (
'manager', -- Branch Manager
'cashier', -- Checkout Staff
'butcher', -- Butchery Staff
'general_worker', -- General Operations Worker
'senior_general_worker', -- Senior Operations Worker
'security_guy', -- Security Personnel
'delivery_guy' -- Supplier / Delivery
)

-- Sorting the list by role (as required) and by employee ID
ORDER BY role, device_id;

-- Cashier Workforce Optimization: Demand vs. Capacity (Cashier Gap Analysis)

-----------------------------------------------------------------
-- Part 1: Average Checkout Time Calculation (Supply) - Session Logic
-----------------------------------------------------------------
WITH 
RawGeoData AS (
    SELECT 
        device_id, 
        timestamp,
        TIMESTAMP_DIFF(timestamp, LAG(timestamp) OVER(PARTITION BY device_id ORDER BY timestamp), MINUTE) as gap_minutes
    FROM 
        `bqproj-435911.Final_Project_2025.geolocation`
    WHERE 
        area = 'CASH_REGISTERS'
        AND role IN ('repeat_customer', 'one_time_customer', 'no_phone')
),

SessionFlags AS (
    SELECT
        device_id,
        timestamp,
        CASE WHEN gap_minutes > 20 OR gap_minutes IS NULL THEN 1 ELSE 0 END as is_new_session
    FROM RawGeoData
),

SessionIDs AS (
    SELECT
        device_id,
        timestamp,
        SUM(is_new_session) OVER(PARTITION BY device_id ORDER BY timestamp) as session_id
    FROM SessionFlags
),

SessionDurations AS (
    SELECT
        device_id,
        session_id,
        EXTRACT(DAYOFWEEK FROM MIN(timestamp)) as num_day,
        FORMAT_DATE('%A', MIN(timestamp)) as day_name,
        EXTRACT(HOUR FROM MIN(timestamp)) as hour,
        TIMESTAMP_DIFF(MAX(timestamp), MIN(timestamp), MINUTE) as duration_minutes
    FROM SessionIDs
    GROUP BY device_id, session_id
    HAVING duration_minutes >= 1 
),

AvgCheckoutTime AS (
    SELECT num_day, day_name, hour, AVG(duration_minutes) as avg_time_per_customer_min
    FROM SessionDurations
    GROUP BY 1, 2, 3
),

-----------------------------------------------------------------
-- Part 2: Sales Volume Calculation (Demand - Required)
-----------------------------------------------------------------
SalesVolume AS (
    SELECT 
        EXTRACT(DAYOFWEEK FROM timestamp) as num_day,
        FORMAT_DATE('%A', timestamp) as day_name,
        EXTRACT(HOUR FROM timestamp) as hour,
        COUNT(sale_id) as total_customers_per_hour
    FROM 
        `bqproj-435911.Final_Project_2025.log_sales`
    GROUP BY 1, 2, 3
),

-----------------------------------------------------------------
-- Part 3: Actual Staffing Status (Actual - Existing)
-----------------------------------------------------------------
ActualStaffing AS (
    SELECT
        EXTRACT(DAYOFWEEK FROM timestamp) as num_day,
        EXTRACT(HOUR FROM timestamp) as hour,
        -- Count unique cashier devices
        COUNT(DISTINCT device_id) as actual_registers_open
    FROM
        `bqproj-435911.Final_Project_2025.geolocation`
    WHERE
        role = 'cashier'
        -- AND area = 'CASH_REGISTERS' -- Recommended to verify they are stationed at the registers
    GROUP BY 1, 2
)

-----------------------------------------------------------------
-- Part 4: Final Join and Gap Calculation
-----------------------------------------------------------------
SELECT 
    s.num_day,
    s.day_name,
    s.hour,
    
    -- Auxiliary Data
    s.total_customers_per_hour AS customers_demand,
    ROUND(t.avg_time_per_customer_min, 2) AS avg_process_time,

    -- 1. Required (Optimal number based on model)
    CEIL(s.total_customers_per_hour / (60 / t.avg_time_per_customer_min)) AS optimal_registers_needed,
    
    -- 2. Actual (How many actually worked)
    -- Use COALESCE to return 0 instead of NULL if no cashiers are found
    COALESCE(a.actual_registers_open, 0) AS actual_registers_open,
    
    -- 3. The Gap (Actual minus Required)
    -- Positive = Overstaffing (Waste), Negative = Understaffing (Shortage)
    COALESCE(a.actual_registers_open, 0) - 
    CEIL(s.total_customers_per_hour / (60 / t.avg_time_per_customer_min)) AS staffing_gap

FROM 
    SalesVolume s
JOIN 
    AvgCheckoutTime t ON s.num_day = t.num_day AND s.hour = t.hour
LEFT JOIN
    ActualStaffing a ON s.num_day = a.num_day AND s.hour = a.hour

ORDER BY 
    s.num_day, s.hour;

/*: Missed Delivery Detection
Objective: Find active business days where no warehouse activity occurred during the supply window.
*/

SELECT DISTINCT
DATE(t1.timestamp) as missed_date,
FORMAT_DATE('%A', t1.timestamp) as day_name
FROM `bqproj-435911.Final_Project_2025.geolocation` t1
WHERE
-- 1. Filter for the Expected Schedule (Mon/Thu)
EXTRACT(DAYOFWEEK FROM t1.timestamp) IN (2, 5)

-- 2. "Heartbeat Check": Ensure the store was open (filter out holidays/closed days)
-- We assume if non-security staff were present, the store was operational.
AND t1.role != 'security_guy'

-- 3. The Negative Filter: Exclude days where a delivery actually happened
AND DATE(t1.timestamp) NOT IN (
SELECT DISTINCT DATE(t2.timestamp)
FROM `bqproj-435911.Final_Project_2025.geolocation` t2
WHERE t2.area = 'WAREHOUSE'
AND EXTRACT(HOUR FROM t2.timestamp) BETWEEN 5 AND 7
)
ORDER BY missed_date;
