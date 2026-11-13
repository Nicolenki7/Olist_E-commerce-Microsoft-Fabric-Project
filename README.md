# 🛒 Olist E-commerce Data Pipeline

**Brazilian E-commerce BI | Power Query M | Dataflows Gen2 | Star Schema**

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-F34F21?logo=microsoft)](https://fabric.microsoft.com/)
[![Power Query](https://img.shields.io/badge/Power_Query_M-0078D4?logo=microsoft)](https://learn.microsoft.com/en-us/power-query/)
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?logo=powerbi)](https://powerbi.microsoft.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📊 Interactive report

**[Open the interactive report →](https://nicolenki7.github.io/Olist_E-commerce-Microsoft-Fabric-Project/report.html)** — monthly revenue, freight share and order status, rebuilt from the CSVs in `data/`.

---

## 📋 Overview

Complete data engineering solution for **Olist** (major Brazilian e-commerce platform) transforming raw transactional data into actionable insights on revenue performance, geographical distribution, and supply chain efficiency.

Features Power Query M ETL, manual state translation with geocoding, Portuguese→English category mapping, and Fact/Dimension modeling for comprehensive BI analytics.

---

## 💼 Business Impact

- **Revenue Performance**: Track sales trends, average order value, and customer lifetime value
- **Geographical Insights**: State-level analysis with ZIP-based geocoding
- **Supply Chain Efficiency**: Delivery time analysis and carrier performance
- **Category Intelligence**: Product category performance and demand patterns

---

## 🛠️ Technical Stack

| Category | Technologies |
| :--- | :--- |
| **Platform** | Microsoft Fabric |
| **ETL** | Power Query M, Dataflows Gen2 |
| **Data Modeling** | Star Schema (Fact/Dimensions) |
| **Data Storage** | OneLake, Delta Lake |
| **BI & Analytics** | Power BI, DAX |
| **Geocoding** | Manual ZIP-to-State mapping |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  OLIST DATA PIPELINE                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  RAW DATA (Portuguese)                                      │
│  └─→ Olist public dataset (CSV files)                       │
│      - Orders, Customers, Products, Payments, Reviews        │
│                                                              │
│  ETL LAYER (Power Query M)                                  │
│  └─→ Transformations:                                       │
│      - Portuguese → English translation                     │
│      - Category mapping & standardization                   │
│      - ZIP code → State geocoding                           │
│      - Date/time normalization                              │
│                                                              │
│  MODELING LAYER (Star Schema)                               │
│  └─→ Fact_Orders                                            │
│      → Dim_Customer, Dim_Product, Dim_Date, Dim_Geography   │
│                                                              │
│  CONSUMPTION LAYER (Power BI)                               │
│  └─→ DAX measures, interactive dashboard                    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Key Features

### Power Query M ETL
- **Language Translation**: Portuguese → English for all text fields
- **Category Mapping**: Product category standardization
- **Geocoding**: ZIP code → State manual mapping
- **Data Type Correction**: Consistent types across all columns

### Star Schema Model
| Table Type | Tables |
| :--- | :--- |
| **Fact Table** | Fact_Orders (transactions, amounts, dates) |
| **Dimension Tables** | Dim_Customer, Dim_Product, Dim_Date, Dim_Geography, Dim_Review |

### DAX Business Metrics
- **Total Revenue**: Sum of order values
- **Average Order Value**: Revenue per transaction
- **Delivery Time**: Average days from order to delivery
- **Review Score**: Average customer satisfaction

---

## 📊 Results & Metrics

| Metric | Value |
| :--- | :--- |
| **Data Source** | Olist Brazilian E-commerce (public dataset) |
| **Orders analysed** | 98,199 |
| **Gross merchandise value** | R$ 13.49 M |
| **Freight share of order value** | 16.6% |
| **Monthly series** | 24 months (2016–2018, both ends partial) |
| **Geographic Coverage** | All Brazilian states |
| **Product Categories** | 70+ categories mapped |

Figures come from `data/olist_resumen_mensual.csv` and `data/olist_estado_pedidos.csv`, aggregated from the public dataset; the raw files (~32 MB) are not committed.

---

## 📁 Project Structure

```
Olist_E-commerce-Microsoft-Fabric-Project/
├── data/                              # Raw CSV files + aggregated summaries
├── dataflows/                         # Power Query M scripts
├── powerbi/                           # Power BI report (.pbix)
├── docs/                              # Documentation + report.html
├── src/build_report.py                # builds docs/report.html from the CSVs
├── geocoding/                         # ZIP-to-State mapping files
└── README.md                          # Project documentation
```

---

## 🔧 Setup & Installation

### Prerequisites
- Microsoft Fabric capacity (F32 or higher)
- Power BI Desktop (latest version)
- Fabric workspace permissions

### Deployment Steps

```bash
# Clone the repository
git clone https://github.com/Nicolenki7/Olist_E-commerce-Microsoft-Fabric-Project.git
cd Olist_E-commerce-Microsoft-Fabric-Project

# 1. Create Fabric Workspace
# 2. Upload raw CSV files to Lakehouse
# 3. Create Dataflows Gen2 with Power Query M transformations
# 4. Build Star Schema model
# 5. Import Power BI report
# 6. Configure DAX measures and relationships
```

### Rebuild the report

```bash
pip install pandas
python src/build_report.py     # writes docs/report.html
```

---

## 📈 Usage

### Dashboard Features

| Visualization | Purpose |
| :--- | :--- |
| **Revenue by State** | Geographic sales distribution |
| **Delivery Performance** | On-time delivery by carrier/region |
| **Category Analysis** | Top-selling product categories |
| **Customer Satisfaction** | Review score trends |
| **Order Trends** | Monthly/weekly order volume |

### Interactive Filters
- **State Filter**: Filter by Brazilian state
- **Date Range**: Custom time period selection
- **Product Category**: Category-level analysis
- **Delivery Status**: Filter by delivery outcome

---

## 🎯 Key Learnings

- **Power Query M** excels at complex text transformations and translations
- **Manual geocoding** necessary when API limits prevent automated lookups
- **Star Schema** simplifies cross-domain analysis (sales + geography + products)
- **Cultural context** matters: Portuguese→English translation requires domain knowledge

---

## 🚧 Limitations

- **2016 and 2018 are partial years** in the dataset; the monthly series must not be read as full-year comparisons.
- **No product cost** in the data, so the report shows gross order value, not margin.
- Order status is a snapshot at extraction time, not a live feed.
- The aggregated CSVs are what the report rebuilds from; the original 32 MB dataset is fetched separately.

---

## 🔮 Future Enhancements

- [ ] Real-time order ingestion pipeline
- [ ] Customer segmentation (RFM analysis)
- [ ] Predictive delivery time estimation
- [ ] Sentiment analysis on product reviews
- [ ] Inventory optimization recommendations

---

## 🔗 Links

| Resource | URL |
| :--- | :--- |
| **Repository** | https://github.com/Nicolenki7/Olist_E-commerce-Microsoft-Fabric-Project |
| **Interactive report** | https://nicolenki7.github.io/Olist_E-commerce-Microsoft-Fabric-Project/report.html |
| **Live Dashboard** | [View in Fabric](https://app.fabric.microsoft.com/links/f5-BZT6Z_l) |
| **Dataset** | [Olist on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) |

---

## 📝 Resumen en Español

Solución completa de ingeniería de datos para **Olist** (plataforma brasileña de e-commerce) transformando datos transaccionales en insights sobre rendimiento de ingresos, distribución geográfica y eficiencia de supply chain.

Incluye ETL con **Power Query M**, traducción Portugués→Inglés, mapeo de categorías, geocodificación manual por ZIP, y modelado Fact/Dimension para análisis BI comprehensivo. El informe interactivo resume **98.199 pedidos**, **13,49 M R$** de valor bruto y un flete que representa el **16,6 %** del valor de los pedidos, con 2016 y 2018 incompletos.

---

## 📄 License

MIT License — Feel free to fork, modify, and use for personal or commercial projects.

---

## 👤 Author

**Nicolás Zalazar** | Data Analyst · Aspiring Data Engineer · Mallorca, Spain

- GitHub: [@Nicolenki7](https://github.com/Nicolenki7)
- LinkedIn: [nicolas-zalazar-63340923a](https://www.linkedin.com/in/nicolas-zalazar-63340923a)
- Portfolio: [nicolenki7.github.io/Portfolio](https://nicolenki7.github.io/Portfolio/)
- Email: zalazarn046@gmail.com
