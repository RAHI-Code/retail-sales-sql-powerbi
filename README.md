# Online Retail Sales Analytics (SQL + Power BI)

End-to-end retail sales analytics project using:

- Python (data cleaning)
- SQLite (data warehouse)
- Star schema modeling
- Power BI (dashboard visualization)

---

## 📊 Dashboard Preview

![Dashboard Preview](dashboard_preview.png)

---

## 📂 Project Structure

- scripts/ → Python ETL
- online_retail.db → SQLite warehouse
- exports/ → Clean dimension + fact tables
- online_retail_dashboard.pbix → Final dashboard

---

## 🧠 Business Insights

- Total Net Sales: 9.7M
- Total Orders: 21,846
- Return Rate: 8.46%
- Top Market: United Kingdom
- Top Product: DOTCOM POSTAGE

---

## 🏗 Data Model

Star schema:
- fact_sales
- dim_date
- dim_product
- dim_customer
- dim_country
