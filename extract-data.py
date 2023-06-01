from tableauhyperapi import HyperProcess, Connection, Inserter, Telemetry, CreateMode, escape_name, TableDefinition, TableName, SqlType, NOT_NULLABLE
from datetime import date
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import GeometryCollection
from shapely.geometry import shape, mapping, Polygon, MultiPolygon, GeometryCollection
import fiona
from fiona.crs import from_epsg

# Before your code
import os
os.environ['SHAPE_ENCODING'] = "ISO-8859-1"


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


def extract_all_data():
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, 'Extract.hyper', CreateMode.NONE) as connection:
            
            #my_data = connection.execute_list_query("""
            #    SELECT CAST("Location" AS text) FROM "Extract"."geo_test"
            #""")

            my_data = connection.execute_list_query("""
                SELECT 
                    "OBJECTID_1", "OBJECTID", "admin2Name", "admin2Na_1", "admin2Pcod", "admin2RefN", "admin2AltN", "admin2Al_1", "admin2Al_2", "admin2Al_3", "admin1Name", "admin1Na_1", "admin1Pcod", "admin0Name", "admin0Na_1", "admin0Pcod", "date", "validOn", "ValidTo", "Shape_Leng", "Shape_Le_1", "Shape_Area",
                    CAST("Geometry" AS text) 
                FROM "Extract"."lbn_admbnda_adm2_cdr_20200810.shp_8B403FD1F98B45A493A2979C48554B17"
            """)

            columns = ["OBJECTID_1", "OBJECTID", "admin2Name", "admin2Na_1", "admin2Pcod", "admin2RefN", "admin2AltN", "admin2Al_1", "admin2Al_2", "admin2Al_3", "admin1Name", "admin1Na_1", "admin1Pcod", "admin0Name", "admin0Na_1", "admin0Pcod", "date", "validOn", "ValidTo", "Shape_Leng", "Shape_Le_1", "Shape_Area", "Geometry"]

            # Create a pandas DataFrame from the result.
            df = pd.DataFrame(my_data, columns=columns)

            # Export the DataFrame to an Excel file.
            df.to_excel('lbn_admbnda_adm2_cdr_20200810.xlsx', index=False)
            #df.to_csv('lbn_admbnda_adm2_cdr_20200810.csv', index=False)


def extract_all_rows_geometry():
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, 'Extract.hyper', CreateMode.NONE) as connection:
            # Read all rows from the table.
            all_rows = connection.execute_list_query("""
                SELECT 
                    "OBJECTID_1", "OBJECTID", "admin2Name", "admin2Na_1", "admin2Pcod", "admin2RefN", "admin2AltN", "admin2Al_1", "admin2Al_2", "admin2Al_3", "admin1Name", "admin1Na_1", "admin1Pcod", "admin0Name", "admin0Na_1", "admin0Pcod", "date", "validOn", "ValidTo", "Shape_Leng", "Shape_Le_1", "Shape_Area",
                    CAST("Geometry" AS text) 
                FROM "Extract"."lbn_admbnda_adm2_cdr_20200810.shp_8B403FD1F98B45A493A2979C48554B17"
            """)

            # Define the schema for the Shapefile.
            schema = {
                'geometry': 'GeometryCollection',
                'properties': {
                    'OBJECTID_1': 'int',
                    'OBJECTID': 'int',
                    'admin2Name': 'str',
                    'admin2Na_1': 'str',
                    'admin2Pcod': 'str',
                    'admin2RefN': 'str',
                    'admin2AltN': 'str',
                    'admin2Al_1': 'str',
                    'admin2Al_2': 'str',
                    'admin2Al_3': 'str',
                    'admin1Name': 'str',
                    'admin1Na_1': 'str',
                    'admin1Pcod': 'str',
                    'admin0Name': 'str',
                    'admin0Na_1': 'str',
                    'admin0Pcod': 'str',
                    'date': 'date',
                    'validOn': 'date',
                    'ValidTo': 'date',
                    'Shape_Leng': 'float',
                    'Shape_Le_1': 'float',
                    'Shape_Area': 'float'
                },
            }

            # Create a new Shapefile
            with fiona.open(
                "lbn_admbnda_adm2_cdr_20200810.geojson", "w", driver="GeoJSON", schema=schema, crs=from_epsg(4326)
            ) as output:
                # Write each row to the Shapefile
                for row in all_rows:

                    geom = wkt.loads(row[-1])  # If the WKB is in hex format

                    output.write({
                        'properties': {
                            'OBJECTID_1': row[0],
                            'OBJECTID': row[1],
                            'admin2Name': row[2],
                            'admin2Na_1': row[3],
                            'admin2Pcod': row[4],
                            'admin2RefN': row[5],
                            'admin2AltN': row[6],
                            'admin2Al_1': row[7],
                            'admin2Al_2': row[8],
                            'admin2Al_3': row[9],
                            'admin1Name': row[10],
                            'admin1Na_1': row[11],
                            'admin1Pcod': row[12],
                            'admin0Name': row[13],
                            'admin0Na_1': row[14],
                            'admin0Pcod': row[15],
                            'date': date(row[16].year, row[16].month, row[16].day) if row[16] else None,
                            'validOn': date(row[17].year, row[17].month, row[17].day) if row[17] else None,
                            'ValidTo': date(row[18].year, row[18].month, row[18].day) if row[18] else None,
                            'Shape_Leng': row[19],
                            'Shape_Le_1': row[20],
                            'Shape_Area': row[21]
                        },
                        'geometry': GeometryCollection([geom]).__geo_interface__,
                    })


def convert_geojson_to_shp():
    # Input GeoJSON source
    with fiona.open('lbn_admbnda_adm2_cdr_20200810.geojson', 'r') as source:

        # Output Shapefile schema
        schema = source.schema
        schema['geometry'] = 'Polygon'

        # Output Shapefile
        with fiona.open('lbn_admbnda_adm2_cdr_20200810.shp', 'w', 'ESRI Shapefile', schema) as output:
            for feat in source:
                try:
                    geom = shape(feat['geometry'])
                    if isinstance(geom, GeometryCollection):
                        for sub_geom in geom.geoms:
                            if isinstance(sub_geom, (Polygon, MultiPolygon)):
                                output.write({
                                    'properties': feat['properties'],
                                    'geometry': mapping(sub_geom)
                                })
                            else:
                                print(f"Skipping sub-geometry {sub_geom} as it's not a Polygon or MultiPolygon")
                    else:
                        print(f"Skipping geometry {geom} as it's not a GeometryCollection")
                except Exception as e:
                    print("Error processing feature %s:" % feat['id'], e)


def display_shape():
    # Load your WKB data from a file
    with open('geometry.wkb', 'rb') as f:
        wkb_data = f.read()

    # Convert the WKB data into a Shapely geometry object
    geometry = wkb.loads(wkb_data)

    # Create a GeoDataFrame with the geometry
    gdf = gpd.GeoDataFrame(geometry=[geometry])

    # Save the GeoDataFrame as a Shapefile
    gdf.to_file('geometry.shp')


def read_shapes():
    # Path to the shapefile
    shapefile_path = 'London_Ward.shp'

    # Read the shapefile
    data = gpd.read_file(shapefile_path)

    # Filter the data by Name
    filtered_data = data[data['Name'] == 'Chessington South']

    # Path to the CSV file
    csv_output_path = 'London_Ward_filtered.csv'

    # Export the filtered data to CSV
    filtered_data.to_csv(csv_output_path, index=False)

    # Export to CSV
    data.to_csv(csv_output_path, index=False)


if __name__ == "__main__":
    #extract_all_data()
    #extract_all_rows_geometry()
    convert_geojson_to_shp()
    #display_shape()
    #read_shapes()
