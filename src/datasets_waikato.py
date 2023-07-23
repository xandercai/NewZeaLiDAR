# -*- coding: utf-8 -*-
"""
This module is used to download the datasets from waikato regional council website
and store the information to the local database.
"""
import logging
import os
import pathlib
from datetime import datetime
from typing import Union

import geopandas as gpd
import pandas as pd
from fiona.drvsupport import supported_drivers

from src import utils
from src.tables import DATASET, create_table, get_max_value, check_table_duplication

logger = logging.getLogger(__name__)

supported_drivers['LIBKML'] = 'rw'


def get_extent_info(extent_file: Union[str, pathlib.Path]) -> gpd.GeoDataFrame:
    """Get extent information from kml file."""
    if os.path.exists(extent_file):
        gdf = gpd.read_file(extent_file)
        gdf = gdf.to_crs(2193)
    else:
        raise ValueError(f'extent file {extent_file} is not exist.')
    gdf = gdf[['ID', 'METADATA', 'VDATUM', 'geometry']]
    gdf = gdf.reset_index()
    gdf = gdf.rename(columns={'ID': 'id', 'METADATA': 'meta_source', 'VDATUM': 'original_datum'})
    return gdf


def store_dataset_to_db(engine,
                        data_path: Union[str, pathlib.Path],
                        gdf: gpd.GeoDataFrame,
                        dataset_dict: dict) -> None:
    """
    store dataset information to database.
    :param engine: database engine
    :param data_path: dataset path
    :param gdf: dataset extent information from kml file
    :param dataset_dict: input dataset information
    """
    dataset_dir = pathlib.Path(data_path)
    tile_dir = [pathlib.PurePosixPath(d) for d in dataset_dir.iterdir() if d.is_dir()]
    tile_path_list = [str(pathlib.PurePosixPath(t / pathlib.Path(t.stem + '_TileIndex.zip')))
                      for t in tile_dir]
    dataset_dict['survey_end_date'] = [datetime.strptime(d, '%d/%m/%Y').strftime('%Y-%m-%d')
                                       for d in dataset_dict['survey_end_date']]
    dataset_dict['survey_start_date'] = dataset_dict['survey_end_date']
    df = pd.DataFrame(dataset_dict)
    gdf_to_db = gdf.merge(df, on=['id'], how='right')
    gdf_to_db = gdf_to_db[['name', 'survey_start_date', 'survey_end_date',
                           'original_datum', 'meta_source', 'geometry']]
    gdf_to_db['extent_path'] = ''
    gdf_to_db['tile_path'] = ''
    for index, row in gdf_to_db.iterrows():
        tile_path = [t for t in tile_path_list if '_'.join(row['name'].split('_')[0:2]) in t]
        assert len(tile_path) == 1, 'tile path is not unique or is not exist.'
        gdf_to_db.loc[index, 'tile_path'] = tile_path[0]
        gdf_to_db.loc[index, 'extent_path'] = str(
            pathlib.PurePosixPath(dataset_dir / pathlib.Path('LiDAR_Regional_Extent.kml'))
        )
    timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %X')
    gdf_to_db['updated_at'] = timestamp
    create_table(engine, DATASET)
    query = f"""SELECT id, name, created_at FROM {DATASET.__tablename__}
                WHERE name in {repr(tuple(map(str, gdf_to_db['name'].to_list())))} ;"""
    df_from_db = pd.read_sql(query, engine)
    if df_from_db.empty:
        _id = get_max_value(engine, 'dataset')
        init_id = _id + 1 if _id else 1
        gdf_to_db['id'] = range(init_id, init_id + len(gdf_to_db))
        gdf_to_db['created_at'] = timestamp
    else:
        query = f"""DELETE FROM {DATASET.__tablename__}
                    WHERE name in {repr(tuple(map(str, df_from_db['name'].to_list())))};"""
        engine.execute(query)
        gdf_to_db = gdf_to_db.merge(df_from_db, on=['name'], how='left')
        assert gdf_to_db['name'].to_list().sort() == df_from_db['name'].to_list().sort(), 'name is not unique.'
    gdf_to_db = gdf_to_db[['id',
                           'name',
                           'survey_start_date',
                           'survey_end_date',
                           'original_datum',
                           'meta_source',
                           'extent_path',
                           'tile_path',
                           'geometry',
                           'created_at',
                           'updated_at']]
    gdf_to_db.to_postgis(DATASET.__tablename__, engine, if_exists='append', index=False)


def run(dataset_info: dict = None) -> None:
    """
    Process Waikato dataset information and save to database.
    :param dataset_info: dataset information dictionary
    """
    # dataset_info = {'name': [...], 'id': [...], 'survey_end_date': [...]}
    # manually create a dictionary to store dataset information,
    # the id is from the kml file and only be used in kml file,
    # the survey_end_date is from the metadata webpage at the root directory of each dataset.
    if dataset_info is None:
        dataset_info = {
            'name': [
                'LiDAR_2014_Hipaua_Thermal_Area',
                'LiDAR_2012_2013_Coromandel',
                'LiDAR_2010_2011_Northern_Waikato',
                'LiDAR_2010_2011_Raglan_Harbour',
                'LiDAR_2007_2008_Area_1',
                'LiDAR_2007_2008_Area_1_Option_B',
                'LiDAR_2007_2008_Area_2',
                'LiDAR_2007_2008_Area_3',
                'LiDAR_2007_2008_Area_4',
                'LiDAR_2007_2008_Area_5',
                'LiDAR_2007_2008_Area_6',
                'LiDAR_2006_Lake_Taupo',
            ],
            'id': [
                16,
                1,
                20,
                21,
                3,
                11,
                4,
                5,
                6,
                7,
                8,
                13,
            ],
            'survey_end_date': [
                '09/05/2014',
                '06/08/2013',
                '31/08/2011',
                '31/08/2011',
                '10/07/2008',
                '10/07/2008',
                '10/07/2008',
                '10/07/2008',
                '10/07/2008',
                '10/07/2008',
                '10/07/2008',
                '18/01/2007',
            ]
        }
    engine = utils.get_database()
    data_path = (pathlib.Path(utils.get_env_variable("DATA_DIR")) /
                 pathlib.Path(utils.get_env_variable("WAIKATO_DIR")))
    kml_path = data_path / pathlib.Path('LiDAR_Regional_Extent.kml')
    gdf = get_extent_info(kml_path)
    store_dataset_to_db(engine, data_path, gdf, dataset_info)
    check_table_duplication(engine, DATASET, 'name')
    engine.dispose()
    logger.info('Waikato LiDAR dataset information is saved to database.')


if __name__ == '__main__':
    run()
