from tableauhyperapi import HyperProcess, Connection, Inserter, Telemetry, CreateMode, escape_name, TableDefinition, TableName, SqlType, NOT_NULLABLE
from datetime import date
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import GeometryCollection
from shapely.geometry import shape, mapping, Polygon, MultiPolygon, GeometryCollection
from shapely.wkt import loads
import fiona
from fiona.crs import from_epsg

# Before your code
import os
os.environ['SHAPE_ENCODING'] = "utf-8"


def create_geo_test():
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, 'London_Ward.hyper', CreateMode.NONE) as connection:

            geo_table = TableDefinition(TableName('Extract','geo_test'), [
                TableDefinition.Column('Name', SqlType.text(), nullability=NOT_NULLABLE),
                TableDefinition.Column('Location',  SqlType.geography(), nullability=NOT_NULLABLE),
            ])
            connection.catalog.create_table(geo_table)

            data_to_insert = [
                [ 'Seattle', "point(-122.338083 47.647528)" ],
                [ 'Munich' , "point(11.584329 48.139257)"   ]
            ]

            inserter_definition = [
                TableDefinition.Column(name='Name', type=SqlType.text(), nullability=NOT_NULLABLE),
                TableDefinition.Column(name='Location_as_text', type=SqlType.text(), nullability=NOT_NULLABLE)]

            column_mappings = [
                'Name',
                Inserter.ColumnMapping('Location', f'CAST({escape_name("Location_as_text")} AS GEOGRAPHY)')
            ]

            with Inserter(connection, geo_table, column_mappings, inserter_definition = inserter_definition) as inserter:
                inserter.add_rows(rows=data_to_insert)
                inserter.execute()


def get_list_of_tables():
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database='Diaspora.hyper') as connection:

            print(connection.catalog.get_schema_names())
            print(connection.catalog.get_table_names('Extract'))



def extract_all_data():
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, 'Diaspora.hyper', CreateMode.NONE) as connection:
            result = connection.execute_query("""
                SELECT 
                    *
                FROM "Extract"."OCHA town names export _06AD206A7F494F0E8CF685CB084C9CC3"
            """)


            # Get column names from the schema of the result.
            column_names = [column.name for column in result.schema.columns]

            # Create a list of rows from the result.
            rows = list(result)

            # Create a pandas DataFrame from the rows and column names.
            df = pd.DataFrame(rows, columns=column_names)

            # Replace the quotes in the column names with a placeholder character.
            df.columns = [col.unescaped.replace('"', "'") for col in df.columns]         

            # Export the DataFrame to a CSV file.
            df.to_csv('OCHA town names export.csv', index=False)



def extract_all_rows_geometry():
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, 'Diaspora.hyper', CreateMode.NONE) as connection:
            # Read all rows from the table.
            all_data = connection.execute_list_query("""
                SELECT 
                    "OBJECTID",
                    "OBJECTID_1",
                    CAST("Geometry" AS text) as "Geometry"
                FROM "Extract"."lbn_admbnda_adm3_cdr_20200810.shp_4A76AD2F61704F848AD73DC98D6C556A"
            """)

            all_rows: pd.DataFrame = pd.DataFrame(all_data)
            all_rows.columns = ['OBJECTID', 'OBJECTID_1', 'Geometry']
            all_rows['Geometry'] = all_rows['Geometry'].apply(loads)

            # Export the DataFrame to a CSV file.
            all_rows.to_csv('lbn_admbnda_adm3_cdr_20200810.shp.csv', index=False)


            # Create a new Shapefile
            gdf = gpd.GeoDataFrame(all_rows, geometry='Geometry')
            gdf.to_file('lbn_admbnda_adm3_cdr_20200810.shp')



if __name__ == "__main__":
    #get_list_of_tables()
    #extract_all_data()
    extract_all_rows_geometry()
