# DataEng 2025 INSArama Project

<center>

<span style="font-size: 150%;">

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

</span>

</center>

## Students: 
- **Diego LARRAZ MARTIN** — diego.larraz-martin@insa-lyon.fr
- **Doha ES-SOUFI** — doha.es-soufi@insa-lyon.fr  
- **Jassir HABBA** — jassir.habba@insa-lyon.fr  

## Abstract
This project presents the design and implementation of a complete data engineering system for cross-media recommendation across movies, TV series, and video games.  
Heterogeneous data from **IMDb** and **Metacritic** is ingested, processed, cleaned, and unified through automated pipelines orchestrated with Apache Airflow.

Shared attributes such as genres, themes, ratings, reviews, and contributors are structured to support:
- analytical studies using a **relational data warehouse**, and
- similarity‑based recommendations using a **graph database**.

The resulting platform enables cross‑domain discovery while highlighting audience preferences, critical reception, and market trends across different entertainment media.

## 1. Introduction

### 1.1 Project Context and Objectives

Movies, TV series, and video games are increasingly interconnected forms of entertainment. Users often transition between these media within the same genres, fictional universes, or narrative themes. Despite this convergence, the data describing these media types is typically stored and analyzed in isolation, which limits cross‑media analysis and recommendation capabilities.

From a data engineering perspective, this project addresses the challenge of integrating **heterogeneous multimedia datasets** that differ in format, structure, semantics, and update frequency. Such integration requires robust ingestion mechanisms, well‑defined transformation pipelines, and carefully designed data models.

The main objective of this project is to design and implement an end‑to‑end data engineering pipeline that:
- collects raw multimedia data from multiple sources,
- cleans, normalizes, and enriches this data,
- stores it in durable and analytics‑ready structures,
- supports both aggregated analytical queries and relationship‑based recommendations.

The architecture follows a layered approach composed of a **landing zone**, a **staging zone**, and a **production zone**, ensuring data durability, reproducibility, and clear separation of concerns. All data movements and transformations are orchestrated using **Apache Airflow**.

---

### 1.2 Domain Description

The domain of this project is **cross‑media entertainment analysis**, focusing on movies, TV series, and video games. These media types share many descriptive attributes, such as genres, themes, contributors, release dates, and audience ratings, which makes them suitable for comparative analysis and similarity‑based exploration.

By unifying datasets from different sources, the system enables exploration of links between media items across domains rather than restricting analysis to a single content type. This domain is particularly relevant for data engineering because it involves:
- multi‑source data integration,
- semantic alignment of attributes,
- hybrid analytical models combining relational and graph databases,
- real‑world recommendation and market analysis use cases.

---

### 1.3 Research Questions

The data pipelines and analytical models implemented in this project are designed to answer the following research questions:

- Which video games are most similar to a given movie or TV series based on shared genres, themes, and metadata?
- Which attributes (genres, keywords, contributors) contribute most to strong cross‑media links and high ratings?
- How do recommendation results differ between a relational, feature‑based analytical model and a graph‑based similarity model?

These questions directly guide the data modeling choices and the enrichment logic applied throughout the pipeline.

---

## 2. How to Run the Project

This project is fully containerized and can be executed locally using **Docker Compose**.  
All required services (Apache Airflow, PostgreSQL, MongoDB, Neo4j, analytics tools) are deployed as Docker containers.

No local installation of databases or Python dependencies is required, and the project can be executed entirely offline using the provided sample datasets.

---

### 2.1 Prerequisites

The only required tools on the host machine are:
- Docker
- Docker Compose
- Git

---

### 2.2 Project Execution

**Step 1 – Clone the repository**
```bash
git clone <repository-url>
cd Data-Eng-Project_INSArama_5IF
```

**Step 2 – Start the environment**
```bash
docker compose up -d
```

This command:
- builds all Docker images,
- starts all services,
- configures internal networking automatically.

To stop the environment:
```bash
docker compose down -v (to destroy volumes created directly by docker need to put a warning)
```

---

### 2.3 Access Airflow

- Airflow Web UI: http://localhost:8080

Airflow orchestrates the complete pipeline:
- Docker Operator Image Building
- Ingestion
- Staging

---

### 2.4 Environment Information

| Service     | Address               
|------------|-----------------------
| Airflow    | http://localhost:8080
| PostgreSQL (pgAdmin) | http://localhost:5050
| Grafana | http://localhost:3000        
| Neo4j (Bolt)     | http://localhost:7474

(Disclaimer) These are the default values in the environment; they can be changed in their respective .env files, except Airflow.

---

## 3. Datasets Description

### 3.1 Data Source 1 – Metacritic

Metacritic is a platform aggregating critic and user reviews for movies, TV series, and video games.

- **Access:** Web scraping (no public API)
- **Format:** JSON
- **Update Frequency:** On‑demand (when pipeline is executed)

**Data Content**
- Media metadata (title, summary, genres, release date, platforms)
- Critic reviews (author, source, score, date, text)
- User reviews (rating, comment, date)

The structure varies by media type:
- Video games: reviews per platform
- TV series: reviews per season
- Movies: global reviews

---

### 3.2 Data Source 2 – IMDb Non‑Commercial Datasets

IMDb provides structured metadata through downloadable datasets.

- **Access:** File download
- **Format:** TSV (`.tsv.gz`)
- **Encoding:** UTF‑8
- **Update Frequency:** Daily

**Datasets Used**
- `title.basics.tsv.gz`
- `title.principals.tsv.gz`
- `name.basics.tsv.gz`

These datasets enable precise linking between media items and contributors using unique identifiers.

---

### 3.3 Justification of Data Source Choices

Metacritic provides rich evaluation data across multiple media types, including both critic and user perspectives. IMDb complements this with high‑quality structured metadata and contributor relationships.

Together, these sources offer both depth and structure, making them well suited for cross‑media analytics and recommendation systems.

---

## 4. Data Architecture and Pipeline

![Pipeline](./Documents/Schemas/Pipeline.png)

### 4.1 Architecture Overview

The project follows a layered data architecture composed of:
- **Landing Zone:** Raw ingested data
- **Staging Zone:** Cleaned, normalized, and enriched data
- **Production Zone:** Analytics‑ready relational and graph models

---

### 4.2 Pipeline Phases

**Ingestion Phase**
- IMDb datasets downloaded using `curl`
- Metacritic data scraped using BeautifulSoup
- Raw data stored in MongoDB

**Staging Phase**
- Data cleaning and normalization with Pandas
- Schema alignment across media types
- Enrichment with contributors and roles
- Persistence into relational and graph databases

**Production Phase**
- PostgreSQL star schema for analytics
- Neo4j graph for similarity and recommendation
- Visualization via Grafana dashboards

---

## 5. Data Modeling

### 5.1 SQL Star Schema

![Star Schema](./Documents/Schemas/Media_Review_Star_Schema.png)

The analytical model is built around a **FACT_REVIEWS** table, linked to:

**Dimension Tables**
- DIM_MEDIA_INFO
- DIM_TIME
- DIM_REVIEWER
- DIM_SECTION

**Bridge Tables**
- BRIDGE_MEDIA_GENRE
- BRIDGE_MEDIA_COMPANY
- BRIDGE_MEDIA_ROLE

These bridge tables model many‑to‑many relationships and include weights to support similarity analysis.

---

## Staging

- ### <table> <tr><th><img src="./Documents/Images/SQLPostgresLogo.jpg" width="30" height="38" /></th> <th>SQL Media Star Schema</th> </tr></table>

<center>

![logo](./Documents/Schemas/Media_Review_Star_Schema.png)

</center>

- ### <table> <tr><th><img src="./Documents/Images/VectorDBLogo.jpg" width="40" height="38" /></th> <th>VectorDB Embedding Schema</th> </tr></table>

## Queries 

## Requirements

## Institute logo

<center>

![Insalogo](./Documents/Images/logo-insa_0.png)

</center>

