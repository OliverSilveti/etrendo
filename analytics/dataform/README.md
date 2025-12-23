# Etrendo Dataform Project

This project contains the Dataform pipeline for Etrendo.

## Project Structure

- `definitions/`: This directory contains the SQLX files that define the tables, views, and operations in the pipeline.
  - `sources/`: Contains the source declarations.
  - `bronze/`: Contains the models for the bronze layer.
  - `silver/`: Contains the models for the silver layer.
  - `gold/`: Contains the models for the gold layer.
- `dataform.json`: The main configuration file for the Dataform project.
- `package.json`: The package configuration file for the Dataform project.
