# -*- coding: utf-8 -*-
"""
This module is used to download the lidar data by API from opentopography website and save to lidar and tile table.
"""
import gc
import json
import logging
import os
import pathlib
import uuid
from datetime import datetime, timedelta
from typing import Union

import geoapis.lidar
import geopandas as gpd
import pandas as pd
from sqlalchemy.engine import Engine

from src import utils
from src.tables import TILE, LIDAR, SDC, CATCHMENT, create_table, deduplicate_table, get_data_by_id

logger = logging.getLogger(__name__)


def get_roi_from_file(file_path: Union[str, pathlib.Path], crs: str = 'epsg:2193') -> gpd.GeoDataFrame:
    with open(file_path, "r") as f:
        roi_json = json.load(f)
    gdf = gpd.GeoDataFrame.from_features(roi_json["features"])
    gdf.set_crs(crs=crs, inplace=True)
    return gdf


def get_roi_from_id(index: Union[int, str, list],
                    file_path: Union[str, pathlib.Path],
                    crs: str = 'epsg:2193') -> gpd.GeoDataFrame:
    """ convert json region of interest to geodataframe data type. Sample geojson content shows below:
        {
          "type": "FeatureCollection",
          "name": "selected_polygon",
          "crs": {
            "type": "name",
            "properties": {
              "name": "urn:ogc:def:crs:EPSG::2193"
            }
          },
          "features": [
            {
              "type": "Feature",
              "properties": {},
              "geometry": {
                "type": "Polygon",
                "coordinates": [
                  [
                    [
                      1569282.846145513700321,
                      5198773.764163039624691
                    ],
                    [
                      1569308.765869934810326,
                      5193071.206304613500834
                    ],
                    [
                      1576887.433781576342881,
                      5193101.414627440273762
                    ],
                    [
                      1576867.914614727720618,
                      5198803.969348043203354
                    ],
                    [
                      1569282.846145513700321,
                      5198773.764163039624691
                    ]
                  ]
                ]
              }
            }
          ]
        }
    :param index: index of the region of interest, currently only support 'sea_drain_catchment' table id
    :param file_path: path to the region of interest geojson file.
    :param crs: coordinate reference system, default is 2193.
    """
    index = [index] if not isinstance(index, list) else index
    gdf_read = gpd.GeoDataFrame(geometry=gpd.GeoSeries())
    engine = utils.get_database()
    for i in index:
        file = str(pathlib.Path(file_path) /
                   pathlib.Path(f'{i}') /
                   pathlib.Path(f'{i}.geojson'))
        if not os.path.exists(file):
            catchment_boundary = get_data_by_id(engine, CATCHMENT, i)
            if catchment_boundary.empty:
                catchment_boundary = get_data_by_id(engine, SDC, i)
                if catchment_boundary.empty:
                    logger.warning(f'Catchment {i} is not in database, ignore it.')
                    continue
            utils.gen_boundary_file(file_path, catchment_boundary, i)
        gdf = get_roi_from_file(file, crs=crs)
        gdf_read = pd.concat([gdf_read, gdf], ignore_index=True)
    engine.dispose()
    # union all the polygons to one "region of interest"
    union_poly = gdf_read.geometry.unary_union
    gdf_roi = gpd.GeoDataFrame(geometry=[union_poly])
    gdf_roi.set_crs(crs=crs, inplace=True)
    return gdf_roi


def get_lidar_data(data_path: Union[str, pathlib.Path],
                   dataset: Union[str, list] = None,
                   gdf: gpd.GeoDataFrame = None) -> None:
    """
    Download the LiDAR data within the catchment area from opentopography using geoapis.
    https://github.com/niwa/geoapis
    """
    if gdf is not None and not gdf.empty:
        lidar_fetcher = geoapis.lidar.OpenTopography(
            cache_path=data_path,
            # note that the search_polygon added buffer by default in geoapis response,
            # no need to add buffer here.
            search_polygon=gdf,
            download_limit_gbytes=800,
            verbose=True
        )
        try:
            lidar_fetcher.run()
        except Exception as e:
            logger.error(f"Fetch lidar data by API error: {e}")
    elif dataset is not None:  # to process OpenTopography private datasets, if we use polygon search, geoapi will exit.
        dataset = [dataset] if not isinstance(dataset, list) else dataset
        logging.info(f"Start downloading dataset:\n{dataset}")
        for dataset_name in dataset:
            lidar_fetcher = geoapis.lidar.OpenTopography(
                cache_path=data_path,
                # note that the search_polygon added buffer by default in geoapis response,
                # no need to add buffer here.
                # search_polygon=gdf,
                download_limit_gbytes=800,
                verbose=True
            )
            try:
                lidar_fetcher.run(dataset_name=dataset_name)
            except Exception as e:
                logger.error(f"Fetch lidar data by API error: {e}")
                continue
    else:
        raise ValueError(f"Input parameters are not correct, please check them.")


def gen_tile_data(gdf_in: gpd.GeoDataFrame, dataset: str) -> gpd.GeoDataFrame:
    """
    Generate tile table content based on the input dataframe.
    Note that the 2d and 3d tile file has different column name in the .shp tile file.
    Need to convert column name, and need to add uuid and dataset columns.
    gdf_in: input dataframe from reading the tile index file in the zip file.
    dataset: dataset name from the directory name.
    """
    gdf = gpd.GeoDataFrame(geometry=gpd.GeoSeries())
    # there are two format of contents in .shp file:
    # columns_3d = ['file_name', 'version', 'num_points', 'point_type', 'point_size',
    #               'min_x', 'max_x', 'min_y', 'max_y', 'min_z', 'max_z', 'URL', 'geometry']
    # columns_2d = ['Filename', 'MinX', 'MinY', 'MaxX', 'MaxY', 'URL', 'geometry']
    if gdf_in['geometry'][0].has_z:
        gdf['file_name'] = gdf_in['file_name'].copy()
        gdf['source'] = gdf_in['URL'].copy()
        # gdf['min_x'] = gdf_in['min_x'].copy()
        # gdf['min_y'] = gdf_in['min_y'].copy()
        # gdf['max_x'] = gdf_in['max_x'].copy()
        # gdf['max_y'] = gdf_in['max_y'].copy()
        gdf['geometry'] = utils.drop_z(gdf_in['geometry'].copy())
    else:
        gdf['file_name'] = gdf_in['Filename'].copy()
        gdf['source'] = gdf_in['URL'].copy()
        # gdf['min_x'] = gdf_in['MinX'].copy()
        # gdf['min_y'] = gdf_in['MinY'].copy()
        # gdf['max_x'] = gdf_in['MaxX'].copy()
        # gdf['max_y'] = gdf_in['MaxY'].copy()
        gdf['geometry'] = gdf_in['geometry'].copy()
    crs = gdf_in.crs
    gdf = gdf.set_crs(crs)
    if '2193' not in str(crs):
        logger.warning(f"Tile index file {gdf['file_name'].values[0]} has crs {str(crs)}, converting to epsg:2193.")
        gdf = gdf.to_crs(epsg=2193)
    gdf['uuid'] = gdf.apply(lambda x: uuid.uuid4(), axis=1)
    gdf['dataset'] = gdf.apply(lambda x: dataset, axis=1)
    duplicate_rows = gdf[gdf.duplicated(subset=['dataset', 'file_name'], keep=False)]
    if len(duplicate_rows) > 0:
        logger.warning(
            f"{int(len(duplicate_rows) / 2)} files have the same file name in a dataset.\n"
            f"{duplicate_rows['dataset'].tolist()},\n"
            f"{duplicate_rows['file_name'].tolist()}, \n"
            f"{duplicate_rows['source'].tolist()}."
        )
    return gdf


def gen_lidar_data(gdf_in: gpd.GeoDataFrame, file_path: str) -> pd.DataFrame:
    """
    Get uuid and geometry from tile dataframe for lidar table.
    if geometry in tile dataframe is polygon Z (3d: x, y, z), convert it to polygon (2d: x, y).
    gdf_in: dataframe from tile index .shp file.
    """
    df = pd.DataFrame()
    df['uuid'] = gdf_in['uuid'].copy()
    df['file_name'] = gdf_in['file_name'].copy()
    # use file source because there are lidar files with same file names and different source path in the same dataset
    df['file_path'] = gdf_in['source'].copy()
    if not (df['file_path'] == '').any():  # data from opentopography
        # convert file path from s3 to local file path.
        df['file_path'] = df['file_path'].apply(
            lambda x: x.replace('https://opentopography.s3.sdsc.edu/pc-bulk/',
                                str(pathlib.Path(file_path).parent.as_posix() + '/'))
        )
    # search all downloaded .laz files in the file_path directory.
    file_path_list = utils.get_files('.laz', file_path)
    file_name_list = [os.path.basename(p) for p in file_path_list]
    df_downloaded = pd.DataFrame({'file_name': file_name_list,
                                  'file_path': file_path_list})
    # check file name, remove not exist .laz row in the tile dataframe and get the same uuid with tile table.
    if not (df['file_path'] == '').any():  # opentopograhy datasets
        df = df.merge(df_downloaded, on=['file_path', 'file_name'], how='right')
    else:  # waikato datasets
        df = df[['uuid', 'file_name']].merge(df_downloaded, on=['file_name'], how='right')
    # check tile info and lidar file is matched,
    # since waidato lidar datasets have some missing tiles.
    nan_rows = df[df['uuid'].isnull()]
    if len(nan_rows) > 0:
        logger.warning(f'Warning:: {len(nan_rows)} .laz files are not found in the tile index file.\n'
                       f'{nan_rows["file_name"].tolist()}')
        # the lidar files without tile info (geometry) are useless, here we save it to database as records.
        df.loc[df['uuid'].isnull(), 'uuid'] = df['uuid'].apply(lambda l: uuid.uuid4())
    duplicate_rows = df[df.duplicated(subset=['uuid'], keep=False)]
    assert len(duplicate_rows) == 0, (
        f"{int(len(duplicate_rows) / 2)} .las files duplicated, please check dataset file.\n"
        f"{duplicate_rows['uuid'].tolist()},\n"
        f"{duplicate_rows['file_name'].tolist()}."
    )
    return df


def store_tile_to_db(engine: Engine, file_path: str) -> gpd.GeoDataFrame:
    """
    Store tile information to tile table.
    Load the zip file where tile info are stored as shape file,
    then tile info are stored in the database, as metadata to lidar table.
    """
    zip_file = utils.get_files('.zip', file_path, expect=1)
    zip_path = os.path.dirname(zip_file)
    dataset = os.path.basename(zip_path)
    logger.info(f"*** Processing {dataset} dataset ***")
    gdf_from_zip = gpd.GeoDataFrame.from_file('zip://' + zip_file)
    gdf_to_db = gen_tile_data(gdf_from_zip, dataset)
    gdf_to_db['created_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %X')
    # gdf_to_db = gdf_to_db[['uuid', 'dataset', 'file_name', 'source',
    #                        'min_x', 'min_y', 'max_x', 'max_y', 'geometry',
    #                        'created_at']]
    gdf_to_db = gdf_to_db[['uuid', 'dataset', 'file_name', 'source', 'geometry', 'created_at']]
    create_table(engine, TILE)
    gdf_to_db.to_postgis('tile', engine, index=False, index_label='uuid', if_exists="append")
    deduplicate_table(engine, TILE, 'source')
    return gdf_to_db


def store_lidar_to_db(engine: Engine, file_path: str, gdf: gpd.GeoDataFrame) -> int:
    """
    To store the path of downloaded point cloud files with geometry annotation.
    gdf: dataframe from tile table.
    """
    df_to_db = gen_lidar_data(gdf, file_path)
    df_to_db['created_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %X')
    df_to_db = df_to_db[['uuid', 'file_path', 'created_at']]
    create_table(engine, LIDAR)
    df_to_db.to_sql('lidar', engine, index=False, index_label='uuid', if_exists="append")
    deduplicate_table(engine, LIDAR, 'file_path')
    return df_to_db['file_path'].nunique()


def check_file_number(data_path: Union[str, pathlib.Path], expect: int) -> None:
    """ To check .laz file number, which should match the .laz number in lidar table. """
    file_path_list = utils.get_files('.laz', data_path)
    if len(file_path_list) != expect:
        logger.warning(
            f'Download .laz file number {len(file_path_list)} is different with "lidar" table {expect}.'
        )
    else:
        logger.info(
            f'Find total {len(file_path_list)} .laz files in {data_path}, equal to "lidar" table counting.'
        )


def store_data_to_db(engine: Engine, data_path: Union[str, pathlib.Path]) -> None:
    """ store tile and lidar data info into database. """
    count = 0
    runtime = []
    # load and save data based on each dataset directory
    for directory in os.listdir(data_path):
        start = datetime.now()
        file_path = os.path.join(data_path, directory)
        if os.path.isdir(file_path) and len(utils.get_files('.laz', file_path)) > 0:
            gdf = store_tile_to_db(engine, file_path)
            count += store_lidar_to_db(engine, file_path, gdf)
        else:
            logger.debug(f'No cloud point file in {file_path}, ignore it.')
        end = datetime.now()
        runtime.append(end - start)
    logger.debug(f"Total runtime: {sum(runtime, timedelta(0, 0))}\n"
                 f"Runtime for each dataset:{json.dumps(runtime, indent=2, default=str)}")
    # check_table_duplication(engine, TILE, 'dataset', 'file_name')
    # check_table_duplication(engine, LIDAR, 'file_path')
    check_file_number(data_path, count)


def run(roi_id: Union[int, str, list] = None,
        roi_file: Union[str, pathlib.Path] = r'configs/demo.geojson',
        roi_gdf: gpd.GeoDataFrame = None,
        name_base: bool = False,
        buffer: Union[int, float] = 20) -> None:
    """
    Main function for download lidar data from OpenTopography.

    :param roi_id: catchment id for 'sea_drain_catchment' or 'catchment' table.
    :param roi_gdf: region of interest boundary, a geodataframe with geometry column.
    :param roi_file: region of interest boundary file path, support one file only.
    :param name_base: if True, use dataset name retrieved form input to download lidar data.
    :param buffer: buffer distance for roi_gdf.
    """
    engine = utils.get_database()
    data_dir = pathlib.Path(utils.get_env_variable("DATA_DIR"))
    lidar_dir = pathlib.Path(utils.get_env_variable("LIDAR_DIR"))
    data_path = data_dir / lidar_dir
    if isinstance(roi_id, (int, str, list)):  # catch_id get higher priority then roi_gdf and roi_file
        dem_dir = pathlib.Path(utils.get_env_variable("DEM_DIR"))
        catch_path = data_dir / dem_dir
        roi_gdf = get_roi_from_id(roi_id, catch_path)
    if name_base:
        if roi_gdf is not None and not roi_gdf.empty:
            _, _, _, dataset = utils.retrieve_dataset(engine, boundary_df=roi_gdf, buffer=buffer)
        elif pathlib.Path(roi_file).exists():
            _, _, _, dataset = utils.retrieve_dataset(engine, boundary_file=roi_file, buffer=buffer)
        else:
            raise ValueError(f"Input parameters are not correct.")
        # filter out Waikato dataset
        dataset = [name for name in dataset if 'LiDAR_' not in name]
        if len(dataset) < 1:
            logger.warning(f"No dataset found in the region of interest, please check the input parameters.")
            engine.dispose()
            gc.collect()
            return
        get_lidar_data(data_path, dataset=dataset)
    else:
        if roi_id is None and pathlib.Path(roi_file).exists():
            roi_gdf = get_roi_from_file(roi_file)
        if roi_gdf is not None and not roi_gdf.empty:
            get_lidar_data(data_path, gdf=roi_gdf)
        else:
            raise ValueError(f"Input parameters are not correct.")
    store_data_to_db(engine, data_path)
    engine.dispose()
    gc.collect()


if __name__ == '__main__':
    # run(roi_file='./configs/nz_mainland.geojson')
    # run(roi_id=[1548, 1588])
    run(roi_id=[1548, 1394])
