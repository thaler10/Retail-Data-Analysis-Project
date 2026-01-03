# 🛒 Retail Data Analysis: Shefa Issachar
### Data-Driven Solutions for Operational & Business Challenges

## 📝 Introduction
This project presents an end-to-end data analysis of the "Shefa Issachar" supermarket branch in Yavne. By integrating raw **Geolocation signals** with **Point-of-Sale (POS) transaction logs**, I conducted a deep-dive investigation into consumer behavior and store operations. 

The primary objective was to transform fragmented data into actionable business strategies—ranging from workforce management to market expansion. This project demonstrates the application of advanced SQL techniques, statistical modeling, and data visualization to solve real-world retail challenges.


## 🔍 Business Challenges & Solutions

### 1. [Customer & Entity Segmentation](sql_queries/customer_segmentation_logic.sql)
* **The Challenge:** Distinguishing between staff, suppliers, and customers within raw geolocation data, and further segmenting customers by purchasing power.
* **Technical Solution:** * Developed a **Classification Engine** to identify roles based on dwell time (>3 hours) and access to operational zones (Warehouse/Office).
    * Applied **IQR (Interquartile Range) analysis** on transaction data to statistically define "Whale" customers.
* **Business Impact:** Identified that high-value **"Whale" transactions** (spending above **₪1,139**) are exclusively driven by returning customers, validating the high financial stakes of customer loyalty.

---

### 2. [Workforce Optimization & Gap Analysis](sql_queries/operations_workforce_optimization.sql)
* **The Challenge:** Addressing long wait times and staffing mismatches during peak demand.
* **Technical Solution:** Performed a **Gap Analysis** comparing real-time customer demand against active cashier devices stationed in the registers zone.
* **Business Impact:** Detected a critical shortage on **Thursdays and Fridays** (where demand requires up to 56 registers vs. 15 available). Recommended **Self-Checkout** systems to handle extreme peaks without increasing labor costs.

---

### 3. [Predictive Behavior: Dwell Time vs. Revenue](sql_queries/visitor_behavior_predictive_models.sql)
* **The Challenge:** Quantifying the direct financial impact of in-store customer experience and dwell time.
* **Technical Solution:** Built a linear regression model (**$R^2 = 0.758$**) to correlate visit duration with basket size.
* **Business Impact:** Proved that **"Time = Money."** Every additional minute in-store correlates with higher revenue, justifying investments in **Sensory Marketing** to encourage longer shopping sessions.

---

### 4. [Market Potential & Competitive Analysis](sql_queries/market_potential_geospatial.sql)
* **The Challenge:** Navigating a highly competitive market with a current low market share (**2.7% of local households**).
* **Technical Solution:** Integrated **CBS (Lamas) demographic data** with competitor location mapping in Yavne.
* **Business Impact:** * **Market Expansion:** Identified a significant untapped potential in Yavne, necessitating a targeted marketing campaign to increase the current **2.7% market share**.
    * **Retention Maximization:** Given the high competitor density, I recommended a strategy to maximize **Retention Value** for existing customers while simultaneously seeking growth opportunities.

---

## 💻 Tech Stack & Key Methods
* **SQL (BigQuery):** Window Functions (`LAG`, `OVER`), Complex Joins, and Statistical Quantiles.
* **Looker Studio:** Data visualization for Traffic Heatmaps and Staffing Fit ratios.
* **Sessionization:** Custom algorithm to define unique "visits" based on signal gaps.
