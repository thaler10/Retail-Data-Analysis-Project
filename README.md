# 🛒 Retail Data Analysis Project: 'Shefa Issachar'
### Operational Optimization & Business Intelligence Strategy

## 📝 Introduction
This project presents an end-to-end data analysis of the "Shefa Issachar" supermarket branch in Yavne. By integrating raw **Geolocation signals** (GPS/WiFi) with **Point-of-Sale (POS) transaction logs**, I conducted a deep-dive investigation into consumer behavior and store operations. 

The primary objective was to transform fragmented data into actionable business strategies—ranging from workforce management to market expansion. This project demonstrates the application of advanced SQL techniques, statistical modeling, and data visualization to solve real-world retail challenges.

---

## 🔍 Business Challenges & Solutions

### 1. [Customer & Entity Segmentation](sql_queries/customer_segmentation_logic.sql)
* **The Challenge:** Distinguishing between staff, suppliers, and customers within raw geolocation data, and further segmenting customers by purchasing power and payment habits.
* **Technical Solution:** * Developed a **Classification Engine** to identify roles based on dwell time (>3 hours) and access to operational zones.
    * Applied **IQR (Interquartile Range)** analysis to define high-value "Whale" customers.
* **Key Findings:** * Identified that "Whale" transactions (above **₪1,139**) are driven exclusively by returning customers.
    * **Payment Trends:** 87% of customers use physical cards, while only 6.4% utilize mobile payments, suggesting a gap in digital adoption.

<img width="1200" height="742" alt="image" src="https://github.com/user-attachments/assets/fa1336b9-0001-4955-bdca-0d1899416d4c" />

<img width="566" height="686" alt="image" src="https://github.com/user-attachments/assets/83559df7-7e3a-4f26-a15e-d3cc8d9f554b" />

---

### 2. [Workforce Optimization & Gap Analysis](sql_queries/operations_workforce_optimization.sql)
* **The Challenge:** Addressing long wait times and staffing mismatches during peak demand.
* **Technical Solution:** Performed a **Gap Analysis** comparing real-time customer demand against active cashier devices.
* **Business Impact:** Detected a critical shortage on **Thursdays and Fridays**, where demand requires up to 56 registers vs. the 15 available. Recommended **Self-Checkout** systems to handle extreme peaks.

#### Traffic Heatmap (Darker red means more customers traffic per hour in average):
<img width="1400" height="900" alt="image" src="https://github.com/user-attachments/assets/09cc1960-9493-4e04-83f6-099c5c0395d9" />

#### Cashier Gap Analysis: Red indicates a staffing shortage, green represents a labor surplus, and bar height quantifies the headcount delta.
<img width="1074" height="722" alt="image" src="https://github.com/user-attachments/assets/247b3d8d-4e44-4a17-b5af-f3451a92a565" />


---

### 3. [Predictive Behavior: Dwell Time vs. Revenue](sql_queries/visitor_behavior_predictive_models.sql)
* **The Challenge:** Quantifying the financial impact of customer dwell time and ensuring data reliability.
* **Technical Solution:** Built a linear regression model (**$R^2 = 0.758$**) to correlate visit duration with basket size.
* **Data Reliability:** The analysis confirmed an **App Capture Rate of 43.8%**, exceeding the 40% target for data validity.
* **Business Impact:** Proved that **"Time = Money."** Every additional minute in-store correlates with higher revenue, justifying investments in **Sensory Marketing** to encourage longer sessions.

<img width="862" height="575" alt="image" src="https://github.com/user-attachments/assets/df7d2391-1d80-4423-a00a-9ca470e08554" />

---

### 4. [Market Potential & Competitive Analysis](sql_queries/market_potential_geospatial.sql)
* **The Challenge:** Navigating a highly competitive market with a current low market share of **2.7% of local households**.
* **Technical Solution:** Integrated **CBS (Lamas) demographic data** with competitor location mapping.
* **Business Impact:** * **Market Expansion:** Identified significant untapped potential in Yavne, necessitating targeted marketing to increase the 2.7% market share.
    * **Retention Maximization:** Recommended a dual strategy: maximizing existing customer value while aggressively acquiring new ones.

<img width="1200" height="742" alt="image" src="https://github.com/user-attachments/assets/41c31c92-ea4f-447d-be7e-d3a368d0b64f" />

---

## 💻 Tech Stack & Key Methods
* **Advanced SQL (BigQuery):** Window Functions,CTE's, Complex Joins, Statistical Quantiles etc.
* **Looker Studio:** Professional dashboarding for operational KPIs and traffic trends.
* **Sessionization:** Custom algorithm to define unique "visits" based on signal gaps.

---

## 📊 Interactive Dashboard
The final phase of this project involved building a comprehensive dashboard in **Looker Studio** to allow management to monitor these KPIs in real-time.
👉 **[View the Live Interactive Dashboard here](https://lookerstudio.google.com/s/lIUa4DozWxw)**

<img width="1137" height="811" alt="image" src="https://github.com/user-attachments/assets/10a16b11-25b3-4999-96fd-0e5446d5cf0f" />
<img width="753" height="842" alt="image" src="https://github.com/user-attachments/assets/f8ecf5ba-0ee8-48bf-894a-7d4037581e01" />


---
*This project was completed as part of the Data Analyst program at Google and Reichman Tech School.*
