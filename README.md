# Data_Warehouse_project

## ⚙️ Overview
This project demonstrates a full end-to-end Data Warehouse solution built using SQL Server. It follows the **Medallion Architecture** (Bronze → Silver → Gold) and ends with a clean star-schema ready for analytics and reporting.  


<img width="937" height="479" alt="Data architecture project" src="https://github.com/user-attachments/assets/a02a2da4-6bbb-4c55-8a50-eeb5259145a2" />


The goal is to consolidate data from CRM and ERP sources, clean and normalize it, and build a structured warehouse that supports reporting, business-insights and analytics.

--- 

## 🚀  What this project includes 
- **Data Sources**: Ingest data provided as CSV files from both CRM and ERP systems into the bronze layer.
- **Data Quality**: Data cleansing , normalization and standarization in the silver layer .
- **Dimensional Modeling (Gold Layer)**: Building the Gold layer: dimension and fact tables (star schema) — e.g. customers, products, sales fact. 
- **Documentation**: Documentation of data model, naming conventions, data flows and schema design.
- **Design for Analytics and Reporting**: Designed with best practices (user friendly) so that the final data model is ready for analytics and reporting



---

## 📝 How to Use  

### 1️⃣ Clone the Repository

git clone https://github.com/Anasbendahmane/Data_Warehouse_project.git

### 2️⃣ Initialize the Database & Schemas

Run the scripts located in `/Scripts/Bronze/` to create the **Datawarehousing** database and the three-tier schema architecture:

- **bronze**
- **silver**
- **gold**

---

### 3️⃣ Load the Bronze Layer (Ingestion)

1. Place your source CSV files into the designated source directory.
2. Execute the ingestion stored procedure to pull raw data into the warehouse:

EXEC bronze.load_bronze;


### 4️⃣ Load the Silver Layer (Cleansing)

Run the Silver scripts to process raw data into the quality layer.

Execute the transformation procedure to:
- Clean and standardize data
- Fix date ranges
- Correct sales calculations
- Ensure gender consistency

EXEC silver.load_silver;


### 5️⃣ Finalize the Gold Layer (Modeling)

Run the scripts located in `/Scripts/Gold/` to generate the final **Dimension** and **Fact** tables.

> **Note:**  
> These tables utilize **Surrogate Keys** to decouple the warehouse from source system changes and significantly improve query performance.

---

### 6️⃣ Analytics & Reporting

Connect your preferred BI tool to the **Gold layer views** for visualization and business intelligence analysis:

- Power BI  
- Tableau  
- Excel  
