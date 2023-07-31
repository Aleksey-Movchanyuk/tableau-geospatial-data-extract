# GitHub Repository: Data Extraction from Tableau Hyper Files

## Introduction
This repository contains Python scripts that demonstrate data extraction from Tableau Hyper files using the Tableau Hyper API. The scripts utilize Python libraries such as `numpy`, `pandas`, `geopandas`, `shapely`, and `fiona` to manipulate and export the data.

## Table of Contents
1. create_geo_test: This function demonstrates how to create a new Tableau Hyper file and insert geographical data into it.

2. get_list_of_tables: This function retrieves a list of schema names and table names from an existing Tableau Hyper file.

3. extract_all_data: This function extracts all data from an existing Tableau Hyper file and exports it to a CSV file.

4. extract_all_rows_geometry: This function extracts all rows with geometry data from a Shapefile stored in a Tableau Hyper file. It exports the data to a CSV file and creates a new Shapefile.

## Requirements
- Python 3.x
- `numpy`
- `tableauhyperapi`
- `pandas`
- `geopandas`
- `shapely`
- `fiona`

## How to Use
1. Install the required Python libraries using `pip install numpy tableauhyperapi pandas geopandas shapely fiona`.
2. Clone this repository to your local machine.
3. Run the desired script by executing `python script_name.py`. Ensure that you have the appropriate Tableau Hyper file with the required data.
4. The scripts will perform the specified data extraction tasks and export the results to CSV files and Shapefiles.

Please note that the scripts are intended to be used as examples and may require modification based on your specific use case.

Feel free to explore, modify, and utilize these scripts as needed. Happy data extraction!

---
Author: Alex M.
