# Building-an-ETL-Pipeline-using-PySpark
# ETL Pipeline using PySpark

This project demonstrates the implementation of a scalable **ETL (Extract, Transform, Load) pipeline using PySpark***, a distributed computing framework designed for big data processing. The pipeline processes a global temperature dataset spanning from 1961 to 2022, transforming it from a wide-format structure into a normalized long-format schema optimized for analytical queries and machine learning workflows.

Key features of the pipeline include:

Distributed Data Processing: Leveraging PySpark's distributed computing capabilities to handle large-scale datasets efficiently.

Data Cleaning and Transformation: Handling missing values, reshaping the dataset, and converting data types for improved usability.

Optimized Storage: Saving the processed data in Parquet format, a columnar storage solution that enhances query performance and reduces storage costs.

Scalability: Designed to scale seamlessly across clusters, making it suitable for processing even larger datasets in production environments.

This project serves as a practical example of how to build robust, production-ready ETL pipelines for big data applications using PySpark. It highlights best practices in data engineering, including schema inference, data quality checks, and efficient storage formats.
---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Dataset Description](#dataset-description)
3. [Steps in the Pipeline](#steps-in-the-pipeline)
4. [How to Run the Code](#how-to-run-the-code)
5. [Dependencies](#dependencies)
6. [Results](#results)
7. [Profiles](#profiles)

---

## Project Overview

The goal of this project is to build an ETL pipeline using PySpark to process a dataset containing temperature data for various countries over several years. The pipeline performs the following tasks:

1. **Extract**: Load the dataset from a CSV file into a PySpark DataFrame.
2. **Transform**: Clean the data, handle missing values, and reshape the dataset to have a single column for years and their corresponding temperature values.
3. **Load**: Save the processed data into a Parquet file for efficient storage and querying.

PySpark is used for its distributed computing capabilities, making it ideal for processing large datasets efficiently.

---

## Dataset Description

The dataset contains the following columns:
- `ObjectId`: Unique identifier for each country.
- `Country`: Name of the country.
- `ISO2`: 2-letter country code.
- `ISO3`: 3-letter country code.
- `F1961` to `F2022`: Yearly temperature data from 1961 to 2022.

The dataset is in CSV format, with each year's temperature data stored in separate columns.

---

## Steps in the Pipeline

### 1. Extract
- The dataset is loaded from a CSV file into a PySpark DataFrame.
- The schema is inferred to ensure correct data types.

### 2. Transform
- **Data Cleaning**: Missing values in the `ISO2` column are replaced with "Unknown". Rows where all temperature values are missing are dropped.
- **Data Reshaping**: The dataset is transformed from a wide format (with separate columns for each year) to a long format, where each row represents a single year's temperature for a country.
- **Data Type Conversion**: The `Year` column is converted from a string (e.g., "F1961") to an integer (e.g., 1961).

### 3. Load
- The transformed data is saved in **Parquet format**, which is optimized for storage and querying in distributed systems.

---
### Dependencies
**PySpark**: For distributed data processing.

**Pandas**: For data manipulation (optional, used in the notebook).

**Jupyter Notebook**: For interactive execution (optional).

### Results
After running the pipeline:

- The dataset is cleaned and transformed into a long format.
- The processed data is saved as a Parquet file (processed_temperature.parquet).
- The final dataset has the following columns:

  - ObjectId
  - Country
  - ISO3
  - Year
  - Temperature


## Profiles
Feel free to explore more of my work and connect with me:

- **GitHub**: [https://github.com/mauzumshamil](https://github.com/mauzumshamil)
  Check out my other projects and code samples.

- **LinkedIn**: [http://linkedin.com/in/mauzum-shamil](http://linkedin.com/in/mauzum-shamil)
  Connect with me on LinkedIn.

- **Portfolio**: [https://linktr.ee/mauzum_shamil](https://linktr.ee/mauzum_shamil)
  A centralized hub for all my work, including projects, blogs, and social media profiles.


