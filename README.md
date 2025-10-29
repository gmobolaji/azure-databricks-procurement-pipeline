[README.md](https://github.com/user-attachments/files/23216763/README.md)
# Procurement Spend Intelligence Pipeline

A 3-tier Azure Databricks pipeline using the **Medallion Architecture (Bronze–Silver–Gold)** to analyze procurement spend patterns, supplier performance, and price efficiency.

## Architecture Overview
1. **Bronze Layer** → Raw CSV ingestion  
2. **Silver Layer** → Cleaned, joined datasets (orders + suppliers)  
3. **Gold Layer** → Aggregated supplier spend and performance  

## Technologies Used
- Azure Databricks (Delta Lake, Spark)
- Azure Data Factory (optional orchestration)
- GitHub Actions (CI/CD)
- Power BI / Tableau (Visualization)

## Key Metrics
- Total Spend per Supplier  
- Purchase Frequency by Category  
- Market Price Variance  
- Spend by Region
