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
from pathlib import Path, PurePosixPath
from datetime import datetime, timedelta
from typing import Union
import shutil

import geopandas as gpd
import pandas as pd
from geofabrics.runner import from_instructions_dict
from sqlalchemy.engine import Engine

from newzealidar import utils
from newzealidar.tables import (
    SDC,
    CATCHMENT,
    DEM,
    DEMATTR,
    USERDEM,
    DATASET,
    GRID,
    GRIDDEM,
    GRIDDEMATTR,
    create_table,
    get_data_by_id,
    get_split_catchment_by_id,
    get_id_under_area,
    get_catchment_by_geometry,
)

logger = logging.getLogger(__name__)


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

    Path(instructions_path).parent.mkdir(parents=True, exist_ok=True)
    with open(instructions_path, "w") as f:
        json.dump(instructions, f, default=_recursive_str, indent=2)


def gen_instructions(
    engine: Engine,
    instructions: dict,
    index: int,
    mode: str = "api",
    grid: bool = False,
    buffer: Union[int, float] = 0,
) -> dict:
    """Read basic instruction file and adds keys and uses geojson as catchment_boundary"""
    if instructions["default"]["processing"].get("number_of_cores") is None:
        instructions["default"]["processing"]["number_of_cores"] = os.cpu_count()
    data_dir = PurePosixPath(utils.get_env_variable("DATA_DIR"))
    if grid:
        dem_dir = PurePosixPath(utils.get_env_variable("GRID_DIR"))
    else:
        dem_dir = PurePosixPath(utils.get_env_variable("DEM_DIR"))
    index_dir = PurePosixPath(str(index))
    subfolder = PurePosixPath(dem_dir / index_dir)
    if instructions.get("dem") is None or instructions["dem"].get("data_paths") is None:
        instructions["dem"]["data_paths"] = {}
    instructions["dem"]["data_paths"]["local_cache"] = str(data_dir)
    instructions["dem"]["data_paths"]["subfolder"] = str(subfolder)
    instructions["dem"]["data_paths"]["downloads"] = str(data_dir)
    instructions["dem"]["data_paths"]["result_dem"] = f"{index}.nc"
    if grid:
        with open(Path(data_dir) / Path(subfolder) / Path(f"{index}.nc"), "w+") as f:
            f.write("empty file to prevent generate hydro dem in grid dem directory.")
    instructions["dem"]["data_paths"]["raw_dem"] = f"{index}_raw_dem.nc"
    instructions["dem"]["data_paths"]["extents"] = f"{index}.geojson"
    if utils.get_env_variable("LAND_FILE", allow_empty=True) != "":
        # cwd is in dem_dir: datastorage/hydro_dem/index
        instructions["dem"]["data_paths"][
            "land"
        ] = f"../../{Path(utils.get_env_variable('LAND_FILE')).as_posix()}"
    catchment_boundary_file = str(
        PurePosixPath(data_dir / subfolder / Path(f"{index}.geojson"))
    )
    if instructions["dem"].get("datasets") is None:
        instructions["dem"]["datasets"] = {}
    if mode == "api":
        if instructions["dem"]["data_paths"].get("land") is None:
            instructions["dem"]["datasets"]["vector"]["linz"][
                "key"
            ] = utils.get_env_variable("LINZ_API_KEY")
            instructions["dem"]["datasets"]["vector"]["linz"]["land"] = {
                "layers": [51153]
            }
        instructions["dem"]["datasets"]["lidar"] = {
            "open_topography": utils.retrieve_dataset(
                engine, catchment_boundary_file, "survey_end_date", buffer=buffer
            )[0]
        }
        instructions["dem"]["datasets"]["lidar"]["local"] = {}
    if mode == "local":
        instructions["dem"]["datasets"]["lidar"] = {
            "local": utils.retrieve_lidar(
                engine, catchment_boundary_file, "survey_end_date", buffer=buffer
            )
        }
        instructions["dem"]["datasets"]["lidar"]["open_topography"] = {}
    # for debug
    instructions_path = str(
        PurePosixPath(data_dir / subfolder / Path("instructions.json"))
    )
    save_instructions(instructions, instructions_path)
    return instructions


def single_process(
    engine: Engine,
    instructions: dict,
    index: int,
    mode: str = "api",
    grid: bool = False,
    buffer: Union[int, float] = 0,
) -> Union[dict, None]:
    """the gen_dem process in a single row of geodataframe"""
    logger.info(
        f"\n\n******* Processing {index} in {mode} mode with geometry buffer {buffer} *******"
    )
    single_instructions = gen_instructions(
        engine, instructions, index, mode=mode, grid=grid, buffer=buffer
    )
    result_path = Path(single_instructions["dem"]["data_paths"]["local_cache"]) / Path(
        single_instructions["dem"]["data_paths"]["subfolder"]
    )
    Path(result_path).mkdir(
        parents=True, exist_ok=True
    )  # to label the catchment are processed even failed
    if mode == "api":
        if not single_instructions["dem"]["datasets"]["lidar"]["open_topography"]:
            logger.info(f"The {index} catchment has no lidar data exist.")
            return None
    elif mode == "local":
        if not single_instructions["dem"]["datasets"]["lidar"]["local"]:
            logger.info(f"The {index} catchment has no lidar data exist.")
            return None
    else:
        raise ValueError(f"Invalid mode: {mode}")
    if not single_instructions["dem"]["dataset_mapping"]:
        logger.error(
            f"The {index} catchment input instructions without dataset mapping, please check!"
        )
        return None

    from_instructions_dict(instructions)

    gc.collect()
    return single_instructions


def store_hydro_to_db(
    engine: Engine, instructions: dict, user_dem: bool = False
) -> bool:
    """save hydrological conditioned dem to database in hydro table."""
    assert len(instructions) > 0, "instructions is empty dictionary."
    index = os.path.basename(instructions["dem"]["data_paths"]["subfolder"])
    dir_path = Path(instructions["dem"]["data_paths"]["local_cache"]) / Path(
        instructions["dem"]["data_paths"]["subfolder"]
    )
    # {index}_raw_dem.nc
    raw_dem_path = (
        dir_path / Path(instructions["dem"]["data_paths"]["raw_dem"])
    ).as_posix()
    # {index}.nc
    result_dem_path = (
        dir_path / Path(instructions["dem"]["data_paths"]["result_dem"])
    ).as_posix()
    # {index}.geojson, ROI boundary
    raw_extent_path = (dir_path / Path(f"{index}.geojson")).as_posix()
    # {index}_raw_extent.geojson, DEM boundary
    dem_extent_path = (dir_path / Path(f"{index}_extents.geojson")).as_posix()

    # generate dem extent file from raw dem netcdf, due to geofabric v1.0.0 change.
    try:
        utils.get_extent_from_dem(result_dem_path, dem_extent_path)
    except Exception as e:
        logger.exception(f"Generate {index} extent failed. Error message:\n{e}")
        return False

    # utils.get_boundary_from_dem(raw_dem_path, dem_extent_path)

    if not all(
        (
            os.path.exists(raw_dem_path),
            os.path.exists(result_dem_path),
            os.path.exists(raw_extent_path),
            os.path.exists(dem_extent_path),
        )
    ):
        logger.warning(
            f"File {raw_dem_path} or {result_dem_path} or {raw_extent_path} or {dem_extent_path} not exist."
            f"Do not store hydrological conditioned dem to database."
        )
        return False
    timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %X")

    # save to hydrologically conditioned DEM table
    if user_dem:  # for user define catchment
        create_table(engine, USERDEM)
        resolution = instructions["default"]["output"]["grid_params"]["resolution"]
        raw_geometry = (gpd.read_file(raw_extent_path, Driver="GeoJSON")).unary_union
        dem_geometry = (gpd.read_file(dem_extent_path, Driver="GeoJSON")).unary_union
        query = f"""INSERT INTO {USERDEM.__tablename__} (
                    catch_id,
                    resolution,
                    raw_dem_path,
                    hydro_dem_path,
                    extent_path,
                    raw_geometry,
                    geometry,
                    created_at
                    ) VALUES (
                    {index},
                    '{resolution}',
                    '{raw_dem_path}',
                    '{result_dem_path}',
                    '{dem_extent_path}',
                    '{raw_geometry}',
                    '{dem_geometry}',
                    '{timestamp}'
                    ) ;"""
        engine.execute(query)
        logger.info(f"Add new {index} in {USERDEM.__tablename__} at {timestamp}.")
    else:  # for catchment table
        query = f"SELECT * FROM {DEM.__tablename__} WHERE catch_id = '{index}' ;"
        df_from_db = pd.read_sql(query, engine)
        if not df_from_db.empty:
            query = f"""UPDATE {DEM.__tablename__}
                        SET raw_dem_path = '{raw_dem_path}',
                            hydro_dem_path = '{result_dem_path}',
                            extent_path = '{dem_extent_path}',
                            updated_at = '{timestamp}'
                        WHERE catch_id = '{index}' ;"""
            engine.execute(query)
            logger.info(f"Updated {index} in {DEM.__tablename__} at {timestamp}.")
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
                        '{dem_extent_path}',
                        '{timestamp}',
                        '{timestamp}'
                        ) ;"""
            engine.execute(query)

        # hydrologically conditioned DEM geometry table, to faster query
        create_table(engine, DEMATTR)
        resolution = instructions["default"]["output"]["grid_params"]["resolution"]
        raw_geometry = (gpd.read_file(raw_extent_path, Driver="GeoJSON")).unary_union
        dem_geometry = (gpd.read_file(dem_extent_path, Driver="GeoJSON")).unary_union
        query = (
            f"SELECT catch_id FROM {DEMATTR.__tablename__} WHERE catch_id = '{index}' ;"
        )
        df_from_db = pd.read_sql(query, engine)
        if not df_from_db.empty:
            query = f"""UPDATE {DEMATTR.__tablename__}
                        SET raw_geometry = '{raw_geometry}',
                            resolution = '{resolution}',
                            geometry = '{dem_geometry}',
                            updated_at = '{timestamp}'
                        WHERE catch_id = '{index}' ;"""
            engine.execute(query)
            logger.info(f"Updated {index} in {DEMATTR.__tablename__} at {timestamp}.")
        else:
            query = f"""INSERT INTO {DEMATTR.__tablename__} (
                        catch_id,
                        resolution,
                        raw_geometry,
                        geometry,
                        created_at,
                        updated_at
                        ) VALUES (
                        {index},
                        '{resolution}',
                        '{raw_geometry}',
                        '{dem_geometry}',
                        '{timestamp}',
                        '{timestamp}'
                        ) ;"""
            engine.execute(query)
        logger.info(f"Add new {index} in {DEMATTR.__tablename__} at {timestamp}.")
    # check_table_duplication(engine, table, 'catch_id')
    return True


def store_grid_to_db(engine: Engine, instructions: dict) -> None:
    """save hydrological conditioned dem to database in hydro table."""
    assert len(instructions) > 0, "instructions is empty dictionary."
    index = os.path.basename(instructions["dem"]["data_paths"]["subfolder"])
    dir_path = Path(instructions["dem"]["data_paths"]["local_cache"]) / Path(
        instructions["dem"]["data_paths"]["subfolder"]
    )
    # {index}_raw_dem.nc
    raw_dem_path = str(dir_path / Path(instructions["dem"]["data_paths"]["raw_dem"]))
    # {index}.geojson, ROI boundary.
    raw_extent_path = (dir_path / Path(f"{index}.geojson")).as_posix()
    # {index}_dem_extent.geojson, DEM boundary.
    dem_extent_path = (dir_path / Path(f"{index}_extents.geojson")).as_posix()
    # generate dem extent file from raw dem netcdf
    # utils.get_extent_from_dem(raw_dem_path, dem_extent_path)
    utils.get_boundary_from_dem(raw_dem_path, dem_extent_path)

    assert os.path.exists(raw_dem_path), f"File {raw_dem_path} not exist."
    assert os.path.exists(raw_extent_path), f"File {raw_extent_path} not exist."
    assert os.path.exists(dem_extent_path), f"File {dem_extent_path} not exist."

    timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %X")

    create_table(engine, GRIDDEM)
    query = f"SELECT * FROM {GRIDDEM.__tablename__} WHERE grid_id = '{index}' ;"
    df_from_db = pd.read_sql(query, engine)
    if not df_from_db.empty:
        query = f"""UPDATE {GRIDDEM.__tablename__}
                    SET raw_dem_path = '{raw_dem_path}',
                        extent_path = '{dem_extent_path}',
                        updated_at = '{timestamp}'
                    WHERE grid_id = '{index}' ;"""
        engine.execute(query)
        logger.info(f"Updated {index} in {GRIDDEM.__tablename__} at {timestamp}.")
    else:
        query = f"""INSERT INTO {GRIDDEM.__tablename__} (
                    grid_id,
                    raw_dem_path,
                    extent_path,
                    created_at,
                    updated_at
                    ) VALUES (
                    {index},
                    '{raw_dem_path}',
                    '{dem_extent_path}',
                    '{timestamp}',
                    '{timestamp}'
                    ) ;"""
        engine.execute(query)

    # Grid DEM geometry table, to faster query
    create_table(engine, GRIDDEMATTR)
    resolution = instructions["default"]["output"]["grid_params"]["resolution"]
    raw_geometry = gpd.read_file(raw_extent_path, Driver="GeoJSON").geometry[0]
    dem_geometry = gpd.read_file(dem_extent_path, Driver="GeoJSON").geometry[0]
    query = (
        f"SELECT grid_id FROM {GRIDDEMATTR.__tablename__} WHERE grid_id = '{index}' ;"
    )
    df_from_db = pd.read_sql(query, engine)
    if not df_from_db.empty:
        query = f"""UPDATE {GRIDDEMATTR.__tablename__}
                    SET raw_geometry = '{raw_geometry}',
                        resolution = '{resolution}',
                        geometry = '{dem_geometry}',
                        updated_at = '{timestamp}'
                    WHERE grid_id = '{index}' ;"""
        engine.execute(query)
        logger.info(f"Updated {index} in {GRIDDEMATTR.__tablename__} at {timestamp}.")
    else:
        query = f"""INSERT INTO {GRIDDEMATTR.__tablename__} (
                    grid_id,
                    resolution,
                    raw_geometry,
                    geometry,
                    created_at,
                    updated_at
                    ) VALUES (
                    {index},
                    '{resolution}',
                    '{raw_geometry}',
                    '{dem_geometry}',
                    '{timestamp}',
                    '{timestamp}'
                    ) ;"""
        engine.execute(query)
    logger.info(f"Add new {index} in {GRIDDEMATTR.__tablename__} at {timestamp}.")
    # check_table_duplication(engine, table, 'catch_id')


# for Digital-Twins
def main(
    catchment_boundary: Union[gpd.GeoDataFrame, str],
    log_level="INFO",
    index: Union[int, str, None] = None,
    check_dem_exist: bool = True,
    exit_if_error: bool = True,
    buffer: Union[int, float] = 10,
) -> None:
    """
    Run the hydrological conditioned dem generation process for a single catchment in API mode.

    Parameters
    ----------
    catchment_boundary : Union[gpd.GeoDataFrame, str]
        The catchment boundary. If it is a string, it should be the string of geojson data (not file path).
    log_level : str, optional
    index : Union[int, str]
        The index of the catchment. Users should make sure it is not conflict with
        the existing catchment index in database.
    check_dem_exist : Default is True. If DEM is already exist in database, pass.
    exit_if_error: exit the process if there is an error.
    buffer : Union[int, float], optional
        The buffer distance of the catchment boundary, by default 10 metres.
    """
    logger.setLevel(log_level)

    if index is None:
        index = f"{datetime.now():%Y%m%d%H%M%S}"[
            -10:
        ]  # integer range up to 2,147,483,647
        logger.info(f"Use {index} as user define catchment index.")
    if isinstance(index, str):
        assert index.isdigit(), f"User define catchment index {index} is not digit."
    logger.info(f"Start Catchment {index} processing...")
    engine = utils.get_database()
    data_dir = Path(utils.get_env_variable("DATA_DIR"))
    dem_dir = Path(utils.get_env_variable("DEM_DIR"))
    result_dir = data_dir / dem_dir
    if isinstance(catchment_boundary, str):
        # read geojson string, not a file
        catchment_boundary = gpd.read_file(catchment_boundary, driver="GeoJSON")
        if "2193" not in str(catchment_boundary.crs):
            catchment_boundary = catchment_boundary.to_crs(2193)

    # check if catchment already exist in hydro_dem table, pass
    if check_dem_exist:
        _, table_name = utils.check_roi_dem_exist(engine, catchment_boundary)
        if table_name:
            logger.info(
                f"The ROI already exist in database, please fetch DEM from database using utils/clip_dem function."
            )
            return

    lidar_extent_file = (
        Path(utils.get_env_variable("DATA_DIR"))
        / Path("gpkg")
        / Path("lidar_extent.gpkg")
    )
    if lidar_extent_file.exists():
        lidar_extent = gpd.read_file(lidar_extent_file, driver="GPKG")
    else:
        # generate lidar extent of all lidar datasets, to filter out catchments without lidar data
        lidar_extent = utils.gen_table_extent(engine, DATASET)
        # save lidar extent to check on QGIS
        utils.save_gpkg(lidar_extent, "lidar_extent")
    if lidar_extent.buffer(buffer).intersects(catchment_boundary).any():
        geojson_file = Path(result_dir) / Path(f"{index}") / Path(f"{index}.geojson")
        geojson_file.parent.mkdir(parents=True, exist_ok=True)
        if not Path(geojson_file).exists():
            utils.gen_boundary_file(result_dir, catchment_boundary, index)
        instructions_file = Path(utils.get_env_variable("INSTRUCTIONS_FILE"))
        with open(instructions_file, "r") as f:
            instructions = json.loads(f.read())
        try:
            single_instructions = single_process(
                engine, instructions, index, mode="api", buffer=buffer
            )
            if single_instructions:
                store_hydro_to_db(engine, single_instructions, user_dem=True)
                logger.info(f"Catchment {index} finished.")
            else:
                logger.warning(
                    f"Catchment {index} failed. No instructions generated. Please check."
                )
        except Exception as e:
            logger.exception(f"Catchment {index} failed. Error message:\n{e}")
            logger.error(
                f"Catchment {index} failed. Running instructions:"
                f"\n{json.dumps(instructions, indent=2, default=str)}"
            )
            if exit_if_error:
                raise e  # DigitalTwins want to exit process if there is an error during the process
            else:
                pass
        engine.dispose()
        gc.collect()


def run(
    catch_id: Union[int, str, list] = None,
    area: Union[int, float] = None,
    mode: str = "api",
    buffer: float = 10,
    start: Union[int, str] = None,
    update: bool = False,
    gpkg: bool = True,
) -> None:
    """
    Main function for generate hydrological conditioned dem of catchments.
    :param catch_id: the id of target catchments.
    :param area: the upper limit area of target catchments.
        if both catch_id and area are none, get all catchments in the catchment table
    :param mode: 'api' or 'local', default is 'api'.
        If mode is 'api', the lidar data will be downloaded from open topography.
        If mode is 'local', the lidar data will be downloaded from local directory.
    :param buffer: the catchment boundary buffer for safeguard catchment boundary,
        default value is 10 metres.
    :param start: the start index of catchment in catchment table, for regression use.
    :param update: if True, run and update the existing dem in `hydro_dem` table, else pass if dem exist.
    :param gpkg: if True, save the hydrological conditioned dem as geopackage.
    """
    engine = utils.get_database()
    data_dir = Path(utils.get_env_variable("DATA_DIR"))
    dem_dir = Path(utils.get_env_variable("DEM_DIR"))
    catch_path = data_dir / dem_dir
    instructions_file = Path(utils.get_env_variable("INSTRUCTIONS_FILE"))
    with open(instructions_file, "r") as f:
        instructions = json.loads(f.read())

    if catch_id is not None:
        if isinstance(catch_id, str):
            assert catch_id.isdigit(), "Input catch_id must be integer string."
        catch_id = catch_id if isinstance(catch_id, list) else [catch_id]
        catch_id = [int(i) for i in catch_id]
        _gdf = pd.read_sql(f"SELECT catch_id FROM {SDC.__tablename__} ;", engine)
        sdc_id = sorted(_gdf["catch_id"].to_list())
        _gdf = pd.read_sql(f"SELECT catch_id FROM {CATCHMENT.__tablename__} ;", engine)
        catchment_id = sorted(_gdf["catch_id"].to_list())
        new_id = []
        for i in catch_id:
            if i in catchment_id:  # small catchment
                new_id.append(i)
            elif (
                i in sdc_id and i not in catchment_id
            ):  # large catchment, search subordinates
                _list = get_split_catchment_by_id(engine, i, sub=True)
                if len(_list) > 0:
                    new_id.extend(_list)
                    logger.debug(
                        f"Catchment {i} split to {len(_list)} subordinates {_list}."
                    )
                else:
                    logger.warning(
                        f"Catchment {i} is not in `catchment` table, "
                        f"please check if it is duplicated or overlap with other catchments."
                    )
            else:
                logger.warning(f"Catchment {i} is not in catchment table, ignore it.")
        catch_id = new_id
        logger.debug(f"check catch_id: pass.")
    elif area is not None:
        catch_id = get_id_under_area(engine, SDC, area)
        logger.info(
            f"There are {len(catch_id)} Catchments that area is under {area} m2"
        )
    else:
        _gdf = pd.read_sql(f"SELECT catch_id FROM {CATCHMENT.__tablename__} ;", engine)
        catch_id = sorted(_gdf["catch_id"].to_list())
        logger.info(
            f"******* FULL CATCHMENTS MODE ********* {len(catch_id)} Catchments DEM in total."
        )

    # generate lidar extent of all lidar datasets, to filter out catchments without lidar data
    lidar_extent = utils.gen_table_extent(engine, DATASET)
    # save lidar extent to check on QGIS
    utils.save_gpkg(lidar_extent, "lidar_extent")

    if start is not None:
        if int(start) in catch_id:
            start_index = catch_id.index(int(start))
            catch_id = catch_id[start_index:]
        else:
            logger.info(f"Input start index {start} is not in catch_id list.")
            catch_id = sorted([x for x in catch_id if x > int(start)])

    runtime = []
    failed = []
    create_table(engine, DEM)

    logger.info(
        f"******* Start process from catch_id {sorted(catch_id)[0]} to {sorted(catch_id)[-1]} *********"
    )
    for i in catch_id:
        if update:
            logger.warning(
                f"Update mode is on, will delete exist {i} directory and update existing info in database."
            )
            shutil.rmtree(catch_path / Path(str(i)), ignore_errors=True)

        # to check if catchment boundary of RoI within lidar extent
        catchment_boundary = get_data_by_id(engine, CATCHMENT, i)
        # to check if already exist in hydro_dem table, if exist_ok, run and update, else pass
        exist_ok = (get_data_by_id(engine, DEM, i, geom_col="")).empty or update

        if (
            lidar_extent.buffer(buffer).intersects(catchment_boundary).any()
            and exist_ok
        ):
            # generate catchment boundary file for each catchment
            utils.gen_boundary_file(catch_path, catchment_boundary, i, buffer=buffer)
            # generate hydrological conditioned dem for each catchment
            t_start = datetime.now()
            try:
                single_instructions = single_process(
                    engine, instructions, i, mode=mode, buffer=buffer
                )
            except Exception as e:
                logger.exception(f"Catchment {i} failed. Error message:\n{e}")
                logger.error(
                    f"Catchment {i} failed. Running instructions:"
                    f"\n{json.dumps(instructions, indent=2, default=str)}"
                )
                failed.append(i)
                continue
            t_end = datetime.now()
            if single_instructions:
                if not store_hydro_to_db(engine, single_instructions):
                    logger.warning(
                        f"Catchment {i} failed. Jump to generate next catchment."
                    )
                    continue
            else:
                logger.error(
                    f"Catchment {i} failed. No instructions generated. Please check."
                )
                failed.append(i)
                continue
            logger.info(f"Catchment {i} finished. Runtime: {t_end - t_start}")
            runtime.append(t_end - t_start)

            # save lidar extent to check on QGIS
            if gpkg:
                gpkg_file = (
                    Path(utils.get_env_variable("DATA_DIR"))
                    / Path("gpkg")
                    / Path("dem_extent.gpkg")
                )
                if gpkg_file.exists():
                    exist_extent = gpd.read_file(gpkg_file, driver="GPKG")
                    current_extent = gpd.read_file(
                        Path(single_instructions["dem"]["data_paths"]["local_cache"])
                        / Path(single_instructions["dem"]["data_paths"]["subfolder"])
                        / Path(f"{i}_extents.geojson"),
                        driver="GeoJSON",
                    )
                    dem_extent = pd.concat(
                        [exist_extent, current_extent], ignore_index=True
                    )
                    dem_geom = utils.filter_geometry(dem_extent.unary_union)
                    dem_extent = gpd.GeoDataFrame(
                        index=[0], geometry=[dem_geom], crs=2193
                    )
                else:
                    dem_extent = utils.gen_table_extent(engine, DEM)
                utils.save_gpkg(dem_extent, "dem_extent")
        else:
            if exist_ok:
                logger.info(f"Catchment {i} is not within lidar extent, ignor it.")
            else:
                logger.info(
                    f"Catchment {i} already exist in hydro_dem table, ignor it."
                )

    if len(failed):
        logger.info(f"Failed {len(failed)} catchments: \n{failed}")

    logger.info(
        f"Total runtime: {sum(runtime, timedelta(0, 0))}\n"
        f"Runtime for each catch_id:{json.dumps(runtime, indent=2, default=str)}"
    )
    engine.dispose()
    gc.collect()


def run_grid(
    boundary_path: Union[str, Path] = None,
    grid_id: Union[int, str, list] = None,
    mode: str = "api",
    buffer: float = 10,
    start: Union[int, str] = None,
    update: bool = False,
    gpkg: bool = True,
) -> None:
    """
    Function for generate raw dem by grids.

    :param boundary_path: the path of ROI boundary file.
    :param grid_id: the id of target grid.
    :param mode: 'api' or 'local', default is 'api'.
        If mode is 'api', the lidar data will be downloaded from open topography.
        If mode is 'local', the lidar data will be downloaded from local directory.
    :param buffer: the catchment boundary buffer for safeguard catchment boundary,
        default value is 10 metres.
    :param start: the start index of catchment in catchment table, for regression use.
    :param update: if True, run and update the existing dem in `hydro_dem` table, else pass if dem exist.
    :param gpkg: if True, save the raw dem extent as geopackage.
    """
    engine = utils.get_database()
    data_dir = Path(utils.get_env_variable("DATA_DIR"))
    dem_dir = Path(utils.get_env_variable("GRID_DIR"))
    grid_path = data_dir / dem_dir
    instructions_file = Path(utils.get_env_variable("INSTRUCTIONS_FILE"))
    with open(instructions_file, "r") as f:
        instructions = json.loads(f.read())

    if boundary_path is not None:
        gdf_boundary = gpd.read_file(boundary_path, driver="GeoJSON")
        gdf_boundary.to_crs("epsg:2193", inplace=True)
        gdf_grid = get_catchment_by_geometry(
            engine, GRID, gdf_boundary, relation="ST_Intersects", buffer=buffer
        )
        grid_id = sorted(gdf_grid["grid_id"].to_list())
        logger.info(
            f"There are {len(grid_id)} Grids that intersect with ROI boundary.\n{grid_id}"
        )
    elif grid_id is not None:
        if isinstance(grid_id, str):
            assert grid_id.isdigit(), "Input grid_id must be integer string."
        grid_id = grid_id if isinstance(grid_id, list) else [grid_id]
        grid_id = [int(i) for i in grid_id]
    else:
        _gdf = pd.read_sql(f"SELECT grid_id FROM {GRID.__tablename__} ;", engine)
        grid_id = sorted(_gdf["grid_id"].to_list())
        logger.info(f"******* FULL GRID MODE ********* {len(grid_id)} GRID in total.")

    lidar_extent_path = (
        Path(utils.get_env_variable("DATA_DIR"))
        / Path("gpkg")
        / Path("lidar_extent.gpkg")
    )
    if lidar_extent_path.is_file():
        lidar_extent = gpd.read_file(lidar_extent_path)
    else:
        # generate lidar extent of all lidar datasets, to filter out catchments without lidar data
        lidar_extent = utils.gen_table_extent(engine, DATASET)
        # save lidar extent to check on QGIS
        utils.save_gpkg(lidar_extent, "lidar_extent")

    if start is not None:
        if int(start) in grid_id:
            start_index = grid_id.index(int(start))
            grid_id = grid_id[start_index:]
        else:
            logger.info(f"Input start index {start} is not in grid_id list.")
            grid_id = sorted([x for x in grid_id if x > int(start)])

    runtime = []
    failed = []
    create_table(engine, GRIDDEM)

    # to check if catchment boundary of RoI within land extent
    gdf_land = gpd.read_file(
        (Path(data_dir) / Path(utils.get_env_variable("LAND_FILE"))), driver="GeoJSON"
    )
    if gdf_land.crs.to_epsg() != 2193:
        gdf_land.to_crs(2193, inplace=True)
    gdf_land = gpd.GeoDataFrame(index=[0], geometry=[gdf_land.unary_union], crs=2193)

    logger.info(
        f"******* Start process from grid_id {sorted(grid_id)[0]} to {sorted(grid_id)[-1]} *********"
    )
    for i in grid_id:
        if update:
            logger.warning(
                f"Update mode is on, will delete exist {i} directory and update existing info in database."
            )
            shutil.rmtree(Path(data_dir) / Path(str(i)), ignore_errors=True)
        # to check if catchment boundary of RoI within lidar extent
        grid_boundary = get_data_by_id(engine, GRID, i, index_column="grid_id")
        # to check if already exist in grid table and within land extent, if exist_ok is ture, run and update, else pass
        intersect_ok = (
            lidar_extent.buffer(buffer).intersects(grid_boundary).any()
            and gdf_land.buffer(buffer).intersects(grid_boundary).any()
        )
        exist_ok = (
            update
            or (
                get_data_by_id(engine, GRIDDEM, i, geom_col="", index_column="grid_id")
            ).empty
        )

        if intersect_ok and exist_ok:
            # generate grid boundary file for each grid
            utils.gen_boundary_file(grid_path, grid_boundary, i, buffer=buffer)
            # generate raw dem for each grid
            t_start = datetime.now()
            try:
                single_instructions = single_process(
                    engine, instructions, i, mode=mode, grid=True, buffer=buffer
                )
            except Exception as e:
                logger.exception(f"Grid {i} failed. Error message:\n{e}")
                logger.error(
                    f"Grid {i} failed. Running instructions:"
                    f"\n{json.dumps(instructions, indent=2, default=str)}"
                )
                failed.append(i)
                continue
            t_end = datetime.now()
            if single_instructions:
                store_grid_to_db(engine, single_instructions)
            else:
                logger.error(
                    f"Grid {i} failed. No instructions generated. Please check."
                )
                failed.append(i)
                continue
            logger.info(f"Grid {i} finished. Runtime: {t_end - t_start}")
            runtime.append(t_end - t_start)

            # save lidar extent to check on QGIS
            if gpkg:
                gpkg_file = (
                    Path(utils.get_env_variable("DATA_DIR"))
                    / Path("gpkg")
                    / Path("grid_extent.gpkg")
                )
                if gpkg_file.exists():
                    exist_extent = gpd.read_file(gpkg_file, driver="GPKG")
                    current_extent = gpd.read_file(
                        Path(single_instructions["dem"]["data_paths"]["local_cache"])
                        / Path(single_instructions["dem"]["data_paths"]["subfolder"])
                        / Path(f"{i}_extents.geojson"),
                        driver="GeoJSON",
                    )
                    dem_extent = pd.concat(
                        [exist_extent, current_extent], ignore_index=True
                    )
                    dem_geom = utils.filter_geometry(dem_extent.unary_union)
                    dem_extent = gpd.GeoDataFrame(
                        index=[0], geometry=[dem_geom], crs=2193
                    )
                else:
                    dem_extent = utils.gen_table_extent(engine, GRIDDEM)
                utils.save_gpkg(dem_extent, "grid_extent")
        else:
            if exist_ok:
                logger.info(f"Grid {i} is not within lidar extent, ignor it.")
            else:
                logger.info(f"Grid {i} already exist in grid_dem table, ignor it.")

    if len(failed):
        logger.info(f"Failed {len(failed)} grads: \n{failed}")

    logger.info(
        f"Total runtime: {sum(runtime, timedelta(0, 0))}\n"
        f"Runtime for each grid_id:{json.dumps(runtime, indent=2, default=str)}"
    )
    engine.dispose()
    gc.collect()
