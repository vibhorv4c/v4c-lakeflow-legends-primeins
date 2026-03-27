# 🛡️ PrimeInsurance AI-Powered Lakehouse & Command Center
**Team:** V4C Lakeflow Legends | **Event:** Databricks Hackathon

## 📖 Project Overview
PrimeInsurance faces massive data silos across 6 regional systems, severe data quality corruption, and millions in revenue leakage due to stagnant vehicle inventory. 

This project delivers a **Trust-Aware, AI-Native Data Product** built entirely on the Databricks Data Intelligence Platform. We engineered a resilient Medallion Architecture that not only cleanses and tracks historical data but actively utilizes **Large Language Models (LLMs)** to generate anomaly explanations, synthesize business insights, and power a RAG-based conversational agent.



## 🏗️ End-to-End Data Flow Architecture
![PrimeInsurance Architecture](https://github.com/vibhorv4c/v4c-lakeflow-legends-primeins/blob/5e4dcb311c93956d224edab66d0fed3eba87535e/Prime%20Insurance%20DBX%20Hackathon/Reference%20Notebooks/Data%20Flow%20Architecture.drawio.png) 

1. **Bronze (Raw Ingestion):** Ingests 14 raw files (7 Customer CSVs, 2 Claim JSONs, 3 Sales CSVs, Policy & Car CSVs) from 6 regional systems using Unity Catalog Volumes with schema inference and incremental loading.
2. **Silver (Cleansing & DQ Gatekeeper):** * Applies structural transformations (fixing data types, handling nulls) and Entity Resolution.
   * **Quarantine Routing:** Failed records are routed to domain-specific quarantine vaults (e.g., `customer_quarantine_table`).
   * **Clean Data:** Passes to clean tables (`cleaned_claims_table`, etc.).
   * **DQ Issues Master:** A rule-based aggregation table powering the DQ Dashboard.
3. **Gold (Star Schema & Aggregations):** * Fact and Dim tables (`fact_claims`, `fact_sales`, `dim_customer`, `dim_cars`, `dim_policy`).
   * **Aggregated Views:** `claim_performance`, `customer_metrics`, and `unsold_inventory`.
4. **Serving & Consumption:**
   * **Dashboards:** Dedicated UIs for Inventory Management and Compliance DQ.
   * **Databricks Alert**

## 🧠 GenAI & LLM Integration (The "Wow" Factor)
Our architecture goes beyond standard data engineering by embedding AI directly into the data workflow:
* **Claims Risk & Anomaly Engine:** Uses CRITIC weighted rules combined with LLMs to generate native AI explanations for flagged claims (`claim_anomaly_explanations`).
* **DQ Insights Engine:** Synthesizes statistical Data Quality rules into human-readable **AI Briefs** (`dq_explanation_report`), explaining to compliance teams *why* data failed.
* **Policy Intelligence (RAG):** A Retrieval-Augmented Generation pipeline (`rag_query_history`) allowing adjudicators to ask natural language questions about complex policy documents.
* **AI Business Insights:** Automatically generates Domain Executive Summaries for leadership, served alongside Databricks AI/BI Genie for ad-hoc natural language querying.
* **Automated Action:** Triggers daily Slackbot alerts for region redistribution and critical inventory aging.

## 📂 Repository Structure

Based on development workflow, the repository is structured as follows:

```text
v4c-lakeflow-legends-primeins/
│
├── Prime Insurance DBX Hackathon/      # Main Project Directory
│   ├── 01_PrimeIns_DE_Pipeline/            # Bronze, Silver (DQ Rules), and Gold (Star Schema) pipelines
│   ├── 02_Gen_AI_Intelligence_Layer/               # Policy RAG, CRITIC Anomaly Engine, and AI DQ Briefs
│   ├── 03_Dashboards/      # SQL for Inventory/Compliance Dashboards & AI Business Insights
│   └── 04_Reference_Notebooks/           # Webhooks and scripts for the Daily Slackbot Trigger
│
├── .gitignore                          # Standard Git ignore configurations
└── README.md                           # Project documentation (You are here)
