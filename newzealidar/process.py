# -*- coding: utf-8 -*-
"""
This module is used to build process pipeline that generate hydrological conditioned DEMs from LiDAR for specific
catchment geometry.

Prerequisites:
* catchment list is ready: run catchments module to download catchments data and save to local database.
* dataset table is ready: run datasets module to download dataset metadata and extent files to local storage.
* lidar and tile files and tables is ready: run lidar module to download lidar data and save to local database.
"""
import gc
import json
import logging
import os
import pathlib
import sys
from datetime import datetime, timedelta
from typing import Union, Type
import geopandas as gpd
import pandas as pd
from sqlalchemy.engine import Engine

from newzealidar import utils
from newzealidar.tables import (Ttable, SDC, CATCHMENT, DEM, DEMATTR, DATASET, create_table,
                                get_data_by_id, get_split_catchment_by_id, get_id_under_area, check_table_duplication)

# if we use Fork GeoFabrics to fix GeoFabrics' issues:
# sys.path.insert(0, str(pathlib.Path(r'../ForkGeoFabrics/src/geofabrics')))
# otherwise:
from geofabrics import processor

logger = logging.getLogger(__name__)


# # for dask, set check time to 3 hours (or other appropriate value) to eliminate overwhelming warning like:
# #     distributed.core - INFO - Event loop was unresponsive in Worker for 3.90s.
# #     This is often caused by long-running GIL-holding functions or moving large chunks of data.
# #     This can cause timeouts and instability.
# os.environ["DASK_DISTRIBUTED__ADMIN__TICK__LIMIT"] = "3h"
# print("DASK_DISTRIBUTED__ADMIN__TICK__LIMIT = ", os.environ["DASK_DISTRIBUTED__ADMIN__TICK__LIMIT"])


def save_instructions(instructions: dict, instructions_path: str) -> None:
    """save instructions to json file."""

    def _recursive_str(d):
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, dict):
                    _recursive_str(v)
                else:
                    d[k] = str(v)
        else:
            d = str(d)
        return d

    with open(instructions_path, 'w') as f:
        json.dump(instructions, f, default=_recursive_str, indent=2)


def gen_instructions(engine: Engine,
                     instructions: dict,
                     index: int,
                     mode: str = 'api',
                     buffer: Union[int, float] = 0) -> dict:
    """Read basic instruction file and adds keys and uses geojson as catchment_boundary"""
    if instructions["instructions"]["processing"].get("number_of_cores") is None:
        instructions["instructions"]["processing"]["number_of_cores"] = os.cpu_count()
    data_dir = pathlib.PurePosixPath(utils.get_env_variable("DATA_DIR"))
    dem_dir = pathlib.PurePosixPath(utils.get_env_variable("DEM_DIR"))
    index_dir = pathlib.PurePosixPath(str(index))
    subfolder = pathlib.PurePosixPath(dem_dir / index_dir)
    if instructions["instructions"].get("data_paths") is None:
        instructions["instructions"]["data_paths"] = {}
    instructions["instructions"]["data_paths"]["local_cache"] = str(data_dir)
    instructions["instructions"]["data_paths"]["subfolder"] = str(subfolder)
    instructions["instructions"]["data_paths"]["downloads"] = str(data_dir)
    instructions["instructions"]["data_paths"]["result_dem"] = f'{index}.nc'
    instructions["instructions"]["data_paths"]["raw_dem"] = f'{index}_raw_dem.nc'
    instructions["instructions"]["data_paths"]["raw_dem_extents"] = f'{index}_raw_extents.geojson'
    instructions["instructions"]["data_paths"]["catchment_boundary"] = f'{index}.geojson'
    if utils.get_env_variable("LAND_FILE", allow_empty=True) is not '':
        # cwd is in dem_dir: datastorage/hydro_dem/index
        instructions["instructions"]["data_paths"]["land"] = str(
            f"../../{str(pathlib.PurePosixPath(utils.get_env_variable('LAND_FILE')))}"
        )
    catchment_boundary_file = str(pathlib.PurePosixPath(data_dir / subfolder / pathlib.Path(f'{index}.geojson')))

    if instructions["instructions"].get("datasets") is None:
        instructions["instructions"]["datasets"] = {"lidar": {}}
    if mode == 'api':
        if instructions["instructions"]["data_paths"].get("land") is None:
            if instructions["instructions"]["datasets"].get("vector") is None:
                instructions["instructions"]["datasets"]["vector"] = {"linz": {}}
            instructions["instructions"]["datasets"]["vector"]["linz"]["key"] = utils.get_env_variable("LINZ_API_KEY")
            instructions["instructions"]["datasets"]["vector"]["linz"]["land"] = {"layers": [51153]}
        instructions["instructions"]["datasets"]["lidar"]["open_topography"] = (
            utils.retrieve_dataset(engine,
                                   catchment_boundary_file,
                                   'survey_end_date',
                                   buffer=buffer)[0])
        instructions["instructions"]["datasets"]["lidar"]["local"] = {}
    if mode == 'local':
        instructions["instructions"]["datasets"]["lidar"]["local"] = (
            utils.retrieve_lidar(engine,
                                 catchment_boundary_file,
                                 'survey_end_date',
                                 buffer=buffer))
        instructions["instructions"]["datasets"]["lidar"]["open_topography"] = {}
    # for debug
    instructions_path = str(pathlib.PurePosixPath(data_dir /
                                                  subfolder /
                                                  pathlib.Path('instructions.json')))
    save_instructions(instructions, instructions_path)
    return instructions


def gen_dem(instructions) -> None:
    """Use geofabrics to generate the hydrologically conditioned DEM."""
    runner = processor.RawLidarDemGenerator(instructions["instructions"], debug=False)
    runner.run()
    runner = processor.HydrologicDemGenerator(instructions["instructions"], debug=False)
    runner.run()


def single_process(engine: Engine,
                   instructions: dict,
                   index: int,
                   mode: str = 'api',
                   buffer: Union[int, float] = 0) -> Union[dict, None]:
    """the gen_dem process in a single row of geodataframe"""
    logger.info(f'\n\n******* Processing {index} in {mode} mode with geometry buffer {buffer} *******')
    single_instructions = gen_instructions(engine, instructions, index, mode=mode, buffer=buffer)
    result_path = (pathlib.Path(single_instructions["instructions"]["data_paths"]["local_cache"]) /
                   pathlib.Path(single_instructions['instructions']['data_paths']['subfolder']))
    pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)  # to label the catchment are processed even failed
    if mode == 'api':
        if not single_instructions["instructions"]["datasets"]["lidar"]["open_topography"]:
            logger.info(f'The {index} catchment has no lidar data exist.')
            return None
    elif mode == 'local':
        if not single_instructions["instructions"]["datasets"]["lidar"]["local"]:
            logger.info(f'The {index} catchment has no lidar data exist.')
            return None
    else:
        raise ValueError(f'Invalid mode: {mode}')
    if not single_instructions["instructions"]["dataset_mapping"]:
        logger.error(f'The {index} catchment input instructions without dataset mapping, please check!')
        return None
    gen_dem(single_instructions)
    gc.collect()
    return single_instructions


def store_hydro_to_db(engine: Engine, instructions: dict) -> None:
    """save hydrological conditioned dem to database in hydro table."""
    assert len(instructions) > 0, 'instructions is empty dictionary.'
    index = os.path.basename(instructions["instructions"]["data_paths"]["subfolder"])
    dir_path = (pathlib.Path(instructions["instructions"]["data_paths"]["local_cache"]) /
                pathlib.Path(instructions["instructions"]["data_paths"]["subfolder"]))
    # {index}_raw_dem.nc
    raw_dem_path = str(dir_path /
                       pathlib.Path(instructions["instructions"]["data_paths"]["raw_dem"]))
    # {index}.nc
    result_dem_path = str(dir_path /
                          pathlib.Path(instructions["instructions"]["data_paths"]["result_dem"]))
    # {index}_raw_dem_extent.geojson
    raw_extent_path = str(dir_path /
                          pathlib.Path(instructions["instructions"]["data_paths"]["raw_dem_extents"]))
    instruction_extent_path = raw_extent_path.replace('_raw_extents', '')
    assert os.path.exists(raw_dem_path), f'File {raw_dem_path} not exist.'
    assert os.path.exists(result_dem_path), f'File {result_dem_path} not exist.'
    assert os.path.exists(raw_extent_path), f'File {raw_extent_path} not exist.'
    assert os.path.exists(instruction_extent_path), f'File {instruction_extent_path} not exist.'
    timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %X')
    # hydrologically conditioned DEM table
    create_table(engine, DEM)
    query = f"SELECT * FROM {DEM.__tablename__} WHERE catch_id = '{index}' ;"
    df_from_db = pd.read_sql(query, engine)
    if not df_from_db.empty:
        query = f"""UPDATE {DEM.__tablename__}
                    SET raw_dem_path = '{raw_dem_path}',
                        hydro_dem_path = '{result_dem_path}',
                        extent_path = '{raw_extent_path}',
                        updated_at = '{timestamp}'
                    WHERE catch_id = '{index}' ;"""
        engine.execute(query)
        logger.info(f'Updated {index} in {DEM.__tablename__} at {timestamp}.')
    else:
        query = f"""INSERT INTO {DEM.__tablename__} (
                    catch_id,
                    raw_dem_path,
                    hydro_dem_path,
                    extent_path,
                    created_at,
                    updated_at
                    ) VALUES (
                    {index},
                    '{raw_dem_path}',
                    '{result_dem_path}',
                    '{raw_extent_path}',
                    '{timestamp}',
                    '{timestamp}'
                    ) ;"""
        engine.execute(query)

        # hydrologically conditioned DEM geometry table, to faster query
        create_table(engine, DEMATTR)
        resolution = instructions["instructions"]["output"]["grid_params"]["resolution"]
        raw_geometry = gpd.read_file(instruction_extent_path, Driver='GeoJSON').geometry[0]
        geometry = gpd.read_file(raw_extent_path, Driver='GeoJSON').geometry[0]
        query = f"SELECT catch_id FROM {DEMATTR.__tablename__} WHERE catch_id = '{index}' ;"
        df_from_db = pd.read_sql(query, engine)
        if not df_from_db.empty:
            query = f"""UPDATE {DEMATTR.__tablename__}
                        SET raw_geometry = '{raw_geometry}'
                            resolution = '{resolution}',
                            geometry = '{geometry}',
                            updated_at = '{timestamp}'
                        WHERE catch_id = '{index}' ;"""
            engine.execute(query)
            logger.info(f'Updated {index} in {DEMATTR.__tablename__} at {timestamp}.')
        else:
            query = f"""INSERT INTO {DEMATTR.__tablename__} (
                        catch_id,
                        raw_geometry,
                        resolution,
                        geometry,
                        created_at,
                        updated_at
                        ) VALUES (
                        {index},
                        '{raw_geometry}',
                        '{resolution}',
                        '{geometry}',
                        '{timestamp}',
                        '{timestamp}'
                        ) ;"""
            engine.execute(query)
        logger.info(f'Add new {index} in {DEMATTR.__tablename__} at {timestamp}.')
    # check_table_duplication(engine, table, 'catch_id')


# for Digital-Twins
def main(catchment_boundary: Union[gpd.GeoDataFrame, str],
         log_level = 'INFO',
         index: Union[int, str, None] = None,
         buffer: Union[int, float] = 14
         ) -> None:
    """
    Run the hydrological conditioned dem generation process for a single catchment in API mode.

    Parameters
    ----------
    index : Union[int, str]
        The index of the catchment. Users should make sure it is not conflict with
        the existing catchment index in database.
    catchment_boundary : Union[gpd.GeoDataFrame, str]
        The catchment boundary. If it is a string, it should be the string of geojson data (not file path).
    buffer : Union[int, float], optional
        The buffer distance of the catchment boundary, by default 0.
    """
    logger.setLevel(log_level)

    if index is None:
        index = f'{datetime.now():%Y%m%d%H%M%S}'[-10:]  # integer range up to 2,147,483,647
        logger.info(f'Input index is None, use {index} as catchment index.')
    if isinstance(index, str):
        assert index.isdigit(), f'Catchment index {index} is not digit.'
    logger.info(f'Start Catchment {index} processing...')
    engine = utils.get_database()
    data_dir = pathlib.Path(utils.get_env_variable("DATA_DIR"))
    dem_dir = pathlib.Path(utils.get_env_variable("DEM_DIR"))
    result_dir = data_dir / dem_dir
    if isinstance(catchment_boundary, str):
        catchment_boundary = gpd.read_file(catchment_boundary, driver='GeoJSON')

    lidar_extent_file = (
            pathlib.Path(utils.get_env_variable('DATA_DIR')) /
            pathlib.Path('gpkg') /
            pathlib.Path('lidar_extent.gpkg'))
    if lidar_extent_file.exists():
        lidar_extent = gpd.read_file(lidar_extent_file, driver='GPKG')
    else:
        # generate lidar extent of all lidar datasets, to filter out catchments without lidar data
        lidar_extent = utils.gen_table_extent(engine, DATASET)
        # save lidar extent to check on QGIS
        utils.save_gpkg(lidar_extent, 'lidar_extent')
    if lidar_extent.buffer(buffer).intersects(catchment_boundary).any():
        json_file = pathlib.Path(result_dir) / pathlib.Path(f'{index}') / pathlib.Path(f'{index}.geojson')
        json_file.parent.mkdir(parents=True, exist_ok=True)
        if not pathlib.Path(json_file).exists():
            utils.gen_boundary_file(result_dir, catchment_boundary, index, buffer)
        instructions_file = pathlib.Path(utils.get_env_variable("INSTRUCTIONS_FILE"))
        with open(instructions_file, 'r') as f:
            instructions = json.loads(f.read())
        try:
            single_instructions = single_process(engine, instructions, index, mode='api', buffer=buffer)
            if single_instructions:
                store_hydro_to_db(engine, single_instructions)
                logger.info(f'Catchment {index} finished.')
            else:
                logger.warning(f'Catchment {index} failed. No instructions generated. Please check.')
        except Exception as e:
            logger.exception(f'Catchment {index} failed. Error message:\n{e}')
            logger.error(f'Catchment {index} failed. Running instructions:'
                         f'\n{json.dumps(instructions, indent=2, default=str)}')
            pass
        engine.dispose()
        gc.collect()


def run(catch_id: Union[int, str, list] = None,
        area: Union[int, float] = None,
        mode: str = 'api',
        buffer: float = 10,
        start: Union[int, str] = None,
        update: bool = False,
        gpkg: bool = True) -> None:
    """
    Main function for generate hydrological conditioned dem of catchments.
    :param catch_id: the id of target catchments.
    :param area: the upper limit area of target catchments.
        if both catch_id and area are none, get all catchments in the catchment table
    :param mode: 'api' or 'local', default is 'api'.
        If mode is 'api', the lidar data will be downloaded from open topography.
        If mode is 'local', the lidar data will be downloaded from local directory.
    :param buffer: the catchment boundary buffer for safeguard catchment boundary,
        default value is 10 meters.
    :param start: the start index of catchment in catchment table, for regression use.
    :param update: if True, run and update the existing dem in `hydro_dem` table, else pass if dem exist.
    :param gpkg: if True, save the hydrological conditioned dem as geopackage.
    """
    engine = utils.get_database()
    data_dir = pathlib.Path(utils.get_env_variable("DATA_DIR"))
    dem_dir = pathlib.Path(utils.get_env_variable("DEM_DIR"))
    catch_path = data_dir / dem_dir
    instructions_file = pathlib.Path(utils.get_env_variable("INSTRUCTIONS_FILE"))
    with open(instructions_file, 'r') as f:
        instructions = json.loads(f.read())

    if catch_id is not None:
        if isinstance(catch_id, str):
            assert catch_id.isdigit(), 'Input catch_id must be integer string.'
        catch_id = catch_id if isinstance(catch_id, list) else [catch_id]
        catch_id = [int(i) for i in catch_id]
        _gdf = pd.read_sql(f"SELECT catch_id FROM {SDC.__tablename__} ;", engine)
        sdc_id = sorted(_gdf['catch_id'].to_list())
        _gdf = pd.read_sql(f"SELECT catch_id FROM {CATCHMENT.__tablename__} ;", engine)
        catchment_id = sorted(_gdf['catch_id'].to_list())
        new_id = []
        for i in catch_id:
            if i in catchment_id:  # small catchment
                new_id.append(i)
            elif i in sdc_id and i not in catchment_id:  # large catchment, search subordinates
                _list = get_split_catchment_by_id(engine, i, sub=True)
                if len(_list) > 0:
                    new_id.extend(_list)
                    logger.debug(f'Catchment {i} split to {len(_list)} subordinates {_list}.')
                else:
                    logger.warning(f'Catchment {i} is not in `catchment` table, '
                                   f'please check if it is duplicated or overlap with other catchments.')
            else:
                logger.warning(f'Catchment {i} is not in catchment table, ignore it.')
        catch_id = new_id
        logger.debug(f'check catch_id: pass.')
    elif area is not None:
        catch_id = get_id_under_area(engine, SDC, area)
        logger.info(f'There are {len(catch_id)} Catchments that area is under {area} m2')
    else:
        _gdf = pd.read_sql(f"SELECT catch_id FROM {CATCHMENT.__tablename__} ;", engine)
        catch_id = sorted(_gdf['catch_id'].to_list())
        logger.info(f'******* FULL CATCHMENTS MODE ********* {len(catch_id)} Catchments DEM in total.')

    # generate lidar extent of all lidar datasets, to filter out catchments without lidar data
    lidar_extent = utils.gen_table_extent(engine, DATASET)
    # save lidar extent to check on QGIS
    utils.save_gpkg(lidar_extent, 'lidar_extent')

    if start is not None:
        if int(start) in catch_id:
            start_index = catch_id.index(int(start))
            catch_id = catch_id[start_index:]
        else:
            logger.info(f'Input start index {start} is not in catch_id list.')
            catch_id = sorted([x for x in catch_id if x > int(start)])

    runtime = []
    failed = []

    logger.info(f'******* Start process from catch_id {catch_id[0]} to {catch_id[-1]} *********')
    for i in catch_id:
        # to check if catchment boundary of RoI within lidar extent
        catchment_boundary = get_data_by_id(engine, CATCHMENT, i)
        # to check if already exist in hydro_dem table, if exist_ok, run and update, else pass
        exist_ok = (get_data_by_id(engine, DEM, i, geom_col='')).empty or update

        if lidar_extent.buffer(buffer).intersects(catchment_boundary).any() and exist_ok:
            # generate catchment boundary file for each catchment
            utils.gen_boundary_file(catch_path, catchment_boundary, i)
            # generate hydrological conditioned dem for each catchment
            t_start = datetime.now()
            try:
                single_instructions = single_process(engine, instructions, i, mode=mode, buffer=buffer)
            except Exception as e:
                logger.exception(f'Catchment {i} failed. Error message:\n{e}')
                logger.error(f'Catchment {i} failed. Running instructions:'
                             f'\n{json.dumps(instructions, indent=2, default=str)}')
                failed.append(i)
                continue
            t_end = datetime.now()
            if single_instructions:
                store_hydro_to_db(engine, single_instructions)
            else:
                logger.error(f'Catchment {i} failed. No instructions generated. Please check.')
                failed.append(i)
                continue
            logger.info(f'Catchment {i} finished. Runtime: {t_end - t_start}')
            runtime.append(t_end - t_start)

            # save lidar extent to check on QGIS
            if gpkg:
                dem_extent = utils.gen_table_extent(engine, DEM)
                utils.save_gpkg(dem_extent, 'dem_extent')
        else:
            if exist_ok:
                logger.info(f'Catchment {i} is not within lidar extent, ignor it.')
            else:
                logger.info(f'Catchment {i} already exist in hydro_dem table, ignor it.')
        logger.info('\n\n')
    if len(failed):
        logger.info(f'Failed {len(failed)} catchments: \n{failed}')

    logger.info(f"Total runtime: {sum(runtime, timedelta(0, 0))}\n"
                f"Runtime for each catch_id:{json.dumps(runtime, indent=2, default=str)}")
    engine.dispose()
    gc.collect()


if __name__ == '__main__':
    # catch_list = [1588, 1596]
    # catch_list = [1548, 1394]
    # catch_list = 1394
    # run(catch_id=catch_list, mode='local')
    # run(catch_id=catch_list, mode='api')
    # run(catch_id=1, mode='api', buffer=12)
    # run(area=1000000, mode='api', buffer=12)
    # run(area=1000000, mode='local', buffer=12)
    gdf = gpd.read_file(r'configs/demo.geojson')
    main(1234567, gdf)
