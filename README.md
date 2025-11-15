# 🇧🇷 Olist E-commerce Data Pipeline on Microsoft Fabric

***

## 🌟 Project Overview

This repository documents the **Data Engineering** project for the Olist E-commerce dataset (Brazil). The solution was implemented entirely in **Microsoft Fabric**, utilizing **Dataflows Gen2** for the Extract, Transform, and Load (**ETL**) process to build a cleaned, analysis-ready **Star Schema Data Warehouse (DWH)**.

The optimized DWH feeds a **Semantic Model** and a Power BI report, enabling detailed business intelligence analysis.

***

## 📊 Dashboard and Data Model in Fabric

Explore the final ETL output, the **Semantic Model**, and the **Interactive Power BI Report** directly in your Microsoft Fabric environment.

### Link to the E-commerce Performance Report:

> [**🚀 Access Olist E-commerce Dashboard on Fabric**](https://app.fabric.microsoft.com/links/f5-BZT6Z_l?ctid=5153b8f5-97d1-4e1b-827f-2fb1bad4128f&pbi_source=linkShare)

***

## 💻 Architecture and ETL Structure

The core of this project lies in the `02_ETL_PowerQuery` folder, which contains the **Power Query M** language scripts that replicate the transformation logic from the Fabric Dataflows Gen2.

### 1. Dimension Tables (Cleaning and Enrichment)

The dimensions focus on data cleansing, text normalization, and geographical enrichment.

| M File | Dimension | Key Transformations (M-Code Logic) |
| :--- | :--- | :--- |
| `ETL_DimCustomer.m` | **DimCustomer** | 1. **Manual State Translation:** Converts state abbreviations (e.g., 'SP') to full names. 2. **Geocoding:** Joins with coordinates (lat/long) by ZIP code prefix. 3. Text cleaning and formatting. |
| `ETL_DimSeller.m` | **DimSeller** | 1. **State Enrichment:** Joins with an auxiliary table (`DimState`) to get the full state name. 2. **Geocoding:** Joins with coordinates. 3. Text cleaning (`Text.Proper`) on cities. |
| `ETL_DimProduct.m` | **DimProduct** | 1. **Category Translation:** Replaces Portuguese category names with their **English** equivalents. 2. Text formatting (`Text.Proper`). 3. Type conversion for weight/dimension metrics. |

### 2. Fact Tables (Metrics)

| M File | Fact Table | Combined Sources | Purpose |
| :--- | :--- | :--- | :--- |
| `ETL_FactOrders.m` | **FactOrders** | `olist_orders`, `olist_order_items`, `olist_order_payments`, `olist_order_reviews` | **(Planned)**. Combines all order-related transactions and calculates key metrics (Revenue, Payment Value, Review Score), establishing foreign keys to dimensions. |

***

## ⚙️ Technology Stack

| Component | Tool/Language | Primary Use |
| :--- | :--- | :--- |
| **Platform** | Microsoft Fabric | Unified Data & BI environment. |
| **ETL** | Dataflows Gen2 | Scalable data transformation flows. |
| **Language** | Power Query **M** | Detailed transformation and cleaning logic. |
| **Data Lake** | Lakehouse | Storage for raw data and the final DWH tables. |
| **BI** | Power BI | Semantic modeling and report visualization. |
