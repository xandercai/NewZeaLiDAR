# -*- coding: utf-8 -*-
"""
This module is used to download and save LiDAR files form Waikato regional concil to local storage and database.
"""
import logging

import os
import pathlib
import uuid
from typing import Union

import geopandas as gpd
import pandas as pd
from sqlalchemy.engine import Engine

from src import utils
from src.lidar import store_lidar_to_db, check_file_number
from src.tables import TILE, create_table, deduplicate_table

logger = logging.getLogger(__name__)


def gen_tile_data(gdf_in: gpd.GeoDataFrame, dataset: str) -> gpd.GeoDataFrame:
    """
    Generate tile table content based on the .shp tile file.
    gdf_in: input from reading the tile index file in the zip file.
    dataset: dataset name from the directory name.
    """
    gdf = gpd.GeoDataFrame(geometry=gpd.GeoSeries())
    # in general
    # columns = ['OBJECTID', 'NZTM_TILE', 'TILE_NUMBE', 'ID', 'SHAPE_Leng', 'SHAPE_Area', 'geometry']
    # for LiDAR_2014_Hipaua_Thermal_Area
    # columns = ['OBJECTID', 'TILENAME', 'PROJECTION', 'V_DATUM', 'AV_DATE',
    #            'ID', 'SHAPE_Leng', 'SHAPE_Area', 'geometry']
    # for LiDAR_2012_2013_Coromandel
    # columns = ['OBJECTID', 'NZTOPO50', 'TILE', 'SHEET', 'AV_DATE', 'PROJECTION', 'V_DATUM1', 'V_DATUM2',
    #            'ID', 'SHAPE_FILE', 'SHAPE_FI_1', 'SHAPE_Leng', 'SHAPE_Area', 'geometry']
    if 'TILE_NUMBE' in gdf_in.columns:
        gdf['file_name'] = gdf_in['TILE_NUMBE'].copy().apply(lambda x: x + '.laz')
    elif 'TILENAME' in gdf_in.columns:
        gdf['file_name'] = gdf_in['TILENAME'].copy().apply(lambda x: x + '.laz')
    elif 'SHEET' in gdf_in.columns:
        gdf['file_name'] = gdf_in['SHEET'].copy().apply(lambda x: x + '.laz')
    else:
        raise ValueError("No tile name column found in the tile index file.")
    gdf['source'] = ''
    # gdf['min_x'] = ''
    # gdf['min_y'] = ''
    # gdf['max_x'] = ''
    # gdf['max_y'] = ''
    gdf['geometry'] = gdf_in['geometry'].copy()
    gdf['uuid'] = gdf.apply(lambda x: uuid.uuid4(), axis=1)
    gdf['dataset'] = gdf.apply(lambda x: dataset, axis=1)
    duplicate_rows = gdf[gdf.duplicated(subset=['file_name'], keep=False)]
    assert len(duplicate_rows) == 0, (
        f"{len(duplicate_rows)} file have the same name.\n"
        f"{duplicate_rows['file_name'].tolist()}."
    )
    crs = gdf_in.crs
    if '2193' in str(crs):
        gdf = gdf.set_crs(crs)
    else:
        logger.warning(f"Tile index data of {dataset} has crs {crs}, converting to epsg:2193.")
        gdf = gdf.to_crs(crs)
    return gdf


def store_tile_to_db(engine: Engine, dataset: str, tile_path: str) -> gpd.GeoDataFrame:
    """
    Store tile information to tile table.
    Load the zip file where tile info are stored as shape file,
    then tile info are stored in the database, as metadata to lidar table.
    """
    zip_file = utils.get_files('_TileIndex.zip', tile_path, expect=1)
    gdf_from_zip = gpd.GeoDataFrame.from_file('zip://' + zip_file)
    gdf_to_db = gen_tile_data(gdf_from_zip, dataset)
    gdf_to_db['created_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %X')
    # gdf_to_db = gdf_to_db[['uuid', 'dataset', 'file_name', 'source',
    #                        'min_x', 'min_y', 'max_x', 'max_y', 'geometry',
    #                        'created_at']]
    gdf_to_db = gdf_to_db[['uuid', 'dataset', 'file_name', 'source', 'geometry', 'created_at']]
    create_table(engine, TILE)
    gdf_to_db.to_postgis('tile', engine, index=False, index_label='uuid', if_exists="append")
    deduplicate_table(engine, TILE, 'dataset', 'file_name')
    return gdf_to_db


def store_data_to_db(engine: Engine, data_path: Union[str, pathlib.Path], dataset_info: dict) -> None:
    """ store tile and lidar data into database. """
    count = 0
    for dataset, tile_dir, dataset_dir in zip(dataset_info["name"],
                                              dataset_info["tile_dir"],
                                              dataset_info["dataset_dir"]):
        logger.info(f"*** Processing {dataset} dataset ***")
        gdf = store_tile_to_db(engine, dataset, tile_dir)
        count += store_lidar_to_db(engine, dataset_dir, gdf, file_type='.laz')
    check_file_number(data_path, count)


def check_file_identity(dataset_info: dict, filetype: str = '.laz') -> None:
    """
    check if there are duplicated files in the tile index file.
    if there are duplicated file name in the same tile index file,
    the same tile index file will be saved in tile table for multiple datasets,
    which will generate large number redundant but can avoid uuid conflict for lidar file.
    """
    total = 0
    for data_path in list(set(dataset_info["tile_dir"])):
        logger.info(f"*** Checking duplication {data_path} ***")
        duplicates = {}
        sub = 0
        file_list = []
        duplicates_files = []
        for (_, _, files) in os.walk(data_path):
            for file in files:
                if file.endswith(filetype):
                    file_list.append(file)
        for file in file_list:
            count = file_list.count(file)  # count = 1: no duplicate, 2: duplicate
            if count > 1 and file not in duplicates:
                duplicates.update({file: count})
                sub += count
        # for debug, can be removed
        if sub > 0:
            for (path, _, files) in os.walk(data_path):
                for file in files:
                    if file in duplicates.keys():
                        duplicates_files.append(str(pathlib.PurePosixPath(os.path.join(path, file))))
            logger.debug(f"{sub} .laz files have duplicate file name for the same tile index file:\n" +
                         '\n'.join(map(str, duplicates_files)))
            total += sub
    # assert total == 0, f"{total} .laz files have duplicate file."
    logger.info(f'Total {total} .laz files have duplicate file name for the same tile index file.')


def run(dataset_info: dict = None) -> None:
    """
    Run the process store Waikato LiDAR datasets to database.
    :param dataset_info: dictionary of dataset information
    """
    data_dir = utils.get_env_variable("DATA_DIR")
    # dataset_info = {'name': [...], 'dataset_dir': [...]}
    # manually create a dictionary to store dataset information,
    # zip_dir is the directory where the tile index zip file is stored.
    if dataset_info is None:
        dataset_info = {
            "name": [
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
            "dataset_dir": [
                rf'{data_dir}/lidar_waikato/LiDAR_2014_Hipaua_Thermal_Area',
                rf'{data_dir}/lidar_waikato/LiDAR_2012_2013_Coromandel',
                rf'{data_dir}/lidar_waikato/LiDAR_2010_2011/Northern_Waikato',
                rf'{data_dir}/lidar_waikato/LiDAR_2010_2011/Raglan_Harbour',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008/Area_1',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008/Area_1_Option_B',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008/Area_2',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008/Area_3',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008/Area_4',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008/Area_5',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008/Area_6',
                rf'{data_dir}/lidar_waikato/LiDAR_2006_Lake_Taupo',
            ],
            "tile_dir": [
                rf'{data_dir}/lidar_waikato/LiDAR_2014_Hipaua_Thermal_Area',
                rf'{data_dir}/lidar_waikato/LiDAR_2012_2013_Coromandel',
                rf'{data_dir}/lidar_waikato/LiDAR_2010_2011',
                rf'{data_dir}/lidar_waikato/LiDAR_2010_2011',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008',
                rf'{data_dir}/lidar_waikato/LiDAR_2007_2008',
                rf'{data_dir}/lidar_waikato/LiDAR_2006_Lake_Taupo',
            ]
        }
    data_path = pathlib.Path(data_dir) / pathlib.Path(utils.get_env_variable("WAIKATO_DIR"))
    engine = utils.get_database()
    check_file_identity(dataset_info, '.laz')
    store_data_to_db(engine, data_path, dataset_info)
    engine.dispose()


if __name__ == '__main__':
    run()
