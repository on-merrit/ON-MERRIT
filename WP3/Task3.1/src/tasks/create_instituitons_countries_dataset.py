
import logging

import pandas as pd
import json

from src.utils.config_loader import ConfigLoader
from os.path import join, abspath, dirname
#from src.utils.butils import *


__author__ = 'bg3753'
__email__ = 'bikash.gyawali@open.ac.uk'


from shapely.geometry import mapping, shape
from shapely.prepared import prep
from shapely.geometry import Point



def create_instituitons_countries_dataset() -> None:
    """Creates a csv mapping latitude and longitude of institutions in MAG to their country names.

    Based on https://stackoverflow.com/a/46589405/530399
    
    :return: None
    :rtype: None
    """
    logger = logging.getLogger(__name__)
    logger.info(f'Running {create_instituitons_countries_dataset.__name__}')

    # setup
    logger.info('Loading config')
    app_cfg = ConfigLoader.load_config()
    

    logger.info('Mapping Latitude/longitude to country names')
    countries_geojson_filepath = join(app_cfg['paths']['data_dir'], 'external/countries.geojson')
    with open(countries_geojson_filepath) as json_file:
        data = json.load(json_file)

    countries = {}
    for feature in data["features"]:
        geom = feature["geometry"]
        country = feature["properties"]["ADMIN"]
        countries[country] = prep(shape(geom))

    def get_country(lon, lat):
        try:
            lon=float(lon)
            lat=float(lat)
            point = Point(lon, lat)
            for country, geom in countries.items():
                if geom.contains(point):
                    return country.lower()
            return None
        except:
            return None

    # print("\n\n\nThis is Austria "+str(get_country(10.0, 47.0)))
    # bprint(get_country(None,None))
    # bprint(get_country("",""))
    # bprint(get_country("text","nonsense"))
    # bprint(get_country("10.0", "47.0"))
    # bprint(get_country(10.0, 47.0))



    affiliations_lat_long_filepath = join(app_cfg['paths']['data_dir'], 'raw/dataset_affiliations_lat_long/affiliation_id_lat_long.csv')
    # bprint(affiliations_lat_long_filepath)
    affiliations_lat_long_df = pd.read_csv(affiliations_lat_long_filepath, header=0, sep=",")
    affiliations_lat_long_df = affiliations_lat_long_df.dropna()  # Delete all records where any lat/long value is NaN




    def get_country_for_df_row(row):
        # Based on http://jonathansoma.com/lede/foundations/classes/pandas%20columns%20and%20functions/apply-a-function-to-every-row-in-a-pandas-dataframe/
        return get_country(row['longitude'], row['latitude'])

    affiliations_lat_long_df['country'] = affiliations_lat_long_df.apply(get_country_for_df_row, axis=1)
    # print(affiliations_lat_long_df.head())
    affiliations_lat_long_df  = affiliations_lat_long_df.dropna() # Delete all records where any the country name could not be determined == NaN


    # Write the output as csv
    affiliations_countryname_filepath = join(app_cfg['paths']['data_dir'], 'processed/affiliations_country.csv')
    affiliations_lat_long_df.to_csv(affiliations_countryname_filepath, sep=',', index=False, header=True)
    logger.info('Wrote affiliations_countryname dataset to '+affiliations_countryname_filepath)
    
    logger.info(f'Done running {create_instituitons_countries_dataset.__name__}')
