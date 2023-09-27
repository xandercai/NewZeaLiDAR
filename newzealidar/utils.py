# -*- coding: utf-8 -*-
"""
This module contains utility functions for the package.
"""
import json
import logging
import os
import pathlib
import shutil
from collections import OrderedDict
from datetime import datetime
from fnmatch import fnmatch
from typing import Type, TypeVar, Union

import geojson
import geopandas as gpd
import pandas as pd
import rioxarray as rxr
from rioxarray import merge

# import xarray as xr
from dotenv import load_dotenv
import shapely
from shapely import unary_union, to_geojson
from shapely.geometry import MultiPolygon, Polygon, GeometryCollection, box
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import NullPool

from newzealidar import tables

logger = logging.getLogger(__name__)

load_dotenv()

Base = declarative_base()

# Generic type, used for static type checking
Ttable = TypeVar("Ttable", bound=Base)
T = TypeVar("T", str, bool, int, float)

CATCHMENT_RESOLUTION = 30  # the resolution of the catchment in meters
EPS = 0.1  # epsilon for float comparison


def get_env_variable(
    var_name: str, default: T = None, allow_empty: bool = False, cast_to: T = str
) -> T:
    """
    Reads an environment variable, with settings to allow defaults, empty values, and type casting
    To read a boolean EXAMPLE_ENV_VAR=False use get_env_variable("EXAMPLE_ENV_VAR", cast_to=bool)

    :param var_name: The name of the environment variable to retrieve.
    :param default: Default return value if the environment variable does not exist. Doesn't override empty string vars.
    :param allow_empty: If False then a KeyError will be raised if the environment variable is empty.
    :param cast_to: The type to cast to eg. str, int, or bool
    :return: The environment variable, or default if it does not exist, as type T.
    :raises: KeyError if allow_empty is False and the environment variable is empty string or None
    :raises: ValueError if cast_to is not compatible with the value stored.
    """
    env_var = os.getenv(var_name, default)
    if not allow_empty and env_var in (None, ""):
        raise KeyError(
            f"Environment variable {var_name} not set, and allow_empty is False"
        )
    return _cast_str(env_var, cast_to)


def _cast_str(str_to_cast: str, cast_to: T) -> T:
    """
    Takes a string and casts it to necessary primitive builtin types. Tested with int, float, and bool.
    For bools, this detects if the value is in the case-insensitive sets {"True", "T", "1"} or {"False", "F", "0"}
    and raises a ValueError if not. For example _cast_str("False", bool) -> False

    :param str_to_cast: The string that is going to be casted to the type
    :param cast_to: The type to cast to e.g. bool
    :return: The string casted to type T defined by cast_to.
    :raises: ValueError if [cast_to] is not compatible with the value stored.
    """
    # Special cases i.e. casts that aren't of the form int("7") -> 7
    if cast_to == bool:
        # For bool we have the problem where bool("False") == True but we want this function to return False
        truth_values = {"true", "t", "1"}
        false_values = {"false", "f", "0"}
        if str_to_cast.lower() in truth_values:
            return True
        elif str_to_cast.lower() in false_values:
            return False
        raise ValueError(
            f"{str_to_cast} being casted to bool but is not in {truth_values} or {false_values}"
        )
    # General case
    return cast_to(str_to_cast)


def get_database(
    null_pool: bool = False, pool_pre_ping: bool = False
) -> Type[create_engine]:
    """
    Exit the program if connection fails.

    :param null_pool: If True, use NullPool to avoid connection pool limitation in multiprocessing. Default is False.
    :param pool_pre_ping: If True, enable pool_pre_ping to check database connection before using. Default is False.
    """
    try:
        engine = get_connection_from_profile(
            null_pool=null_pool, pool_pre_ping=pool_pre_ping
        )
        return engine
    except ConnectionAbortedError:
        raise ConnectionAbortedError("Connection to database failed. Check .env file.")


def get_connection_from_profile(
    null_pool: bool = False, pool_pre_ping: bool = False
) -> Type[create_engine]:
    """Sets up database connection from .env file."""
    connection_keys = [
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]
    host, port, db, username, password = (
        get_env_variable(key) for key in connection_keys
    )
    assert (
        any(
            connection_cred is None
            for connection_cred in [host, port, db, username, password]
        )
        is False
    ), "Error:: One or more of the connection credentials is missing."
    return get_engine(
        db,
        username,
        host,
        port,
        password,
        null_pool=null_pool,
        pool_pre_ping=pool_pre_ping,
    )


def get_engine(
    db: str,
    user: str,
    host: str,
    port: str,
    password: str,
    null_pool: bool = False,
    pool_pre_ping: bool = False,
) -> Type[create_engine]:
    """
    Get SQLalchemy engine using credentials.
    Add connect_args to keep connection alive incase of long running process.

    :param db: database name
    :param user: Username
    :param host: Hostname of the database server
    :param port: Port number
    :param password: Password for the database
    :param null_pool: If True, use NullPool to avoid connection pool limitation in multiprocessing. Default is False.
    :param pool_pre_ping: If True, enable pool_pre_ping to check database connection before using. Default is False.
    :return: SQLalchemy engine
    """
    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    poolclass = NullPool if null_pool else None
    engine = create_engine(
        url,
        poolclass=poolclass,
        pool_pre_ping=pool_pre_ping,
        connect_args={
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
    )
    Base.metadata.create_all(engine)
    return engine


def timeit(f):
    """timer decorator"""

    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = f(*args, **kwargs)
        span = datetime.now() - start
        logging.info(
            f"\n*** TIME IT ***\n{f.__name__} runtime: {span}\n***************"
        )
        return result

    return wrapper


def cast_geodataframe(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    cast data type of geodataframe to correct type to avoid error when saving to database.
    the input columns must contain ['catch_id', 'area', 'geometry']
    """
    gdf["catch_id"] = gdf["catch_id"].astype(int)
    gdf["area"] = gdf["area"].astype(float)
    gdf = gdf.set_geometry("geometry")
    gdf = gdf.set_crs(epsg=2193)
    return gdf


def case_insensitive_rglob(directory: Union[str, pathlib.Path], pattern: str) -> list:
    """case-insensitive rglob function."""
    path = pathlib.Path(directory)
    assert path.is_dir(), f"{path} is not a directory."
    return [
        str(file.as_posix())
        for file in path.rglob(pattern)
        if fnmatch(file.name, pattern)
    ]


def get_files(
    suffix: Union[str, list], file_path: Union[str, pathlib.Path], expect: int = -1
) -> Union[list, str]:
    """To get the path of all the files with filetype extension in the input file path."""
    list_file_path = []
    list_suffix = suffix if isinstance(suffix, list) else [suffix]
    for _suffix in list_suffix:
        list_file_path.extend(case_insensitive_rglob(file_path, f"*{_suffix}"))
    if expect < 0 or 1 < expect == len(list_file_path):
        if len(list_file_path) == 0:
            logger.debug(f"No {suffix} file found in {file_path}.")
        return list_file_path
    elif expect == 1 and len(list_file_path) == 1:
        return list_file_path[0]
    else:
        logger.error(
            f"Find {len(list_file_path)} {suffix} files in {file_path}, where expect {expect}."
        )


def drop_z(ds: gpd.GeoSeries) -> gpd.GeoSeries:
    """
    Drop Z coordinates from GeoSeries, returns GeoSeries
    Requires pygeos to be installed, otherwise it get error without warning.
    source: https://gist.github.com/rmania/8c88377a5c902dfbc134795a7af538d8
    """
    return gpd.GeoSeries.from_wkb(ds.to_wkb(output_dimension=2))


def gen_boundary_file(
    data_path: Union[str, pathlib.Path],
    gdf_boundary: gpd.GeoDataFrame,
    index: Union[int, str],
    buffer: Union[int, float] = 0,
    crs: str = "2193",
    save_file: Union[str, pathlib.Path] = None,
) -> None:
    """
    generate boundary file based on the input geodataframe.
    Save to DATA_DIR/data_path/index/index.geojson.
    """
    if buffer > 0:  # not recommended, cause the original input boundary will be lost.
        gdf_boundary["geometry"] = gdf_boundary["geometry"].buffer(
            buffer, join_style="mitre"
        )
    feature_crs = {
        "type": "name",
        "properties": {"name": f"urn:ogc:def:crs:EPSG::{crs}"},
    }
    feature = geojson.Feature(geometry=gdf_boundary["geometry"][0], properties={})
    feature_collection = geojson.FeatureCollection(
        [feature], name="selected_polygon", crs=feature_crs
    )
    if save_file is None:
        file_path = (
            pathlib.Path(data_path)
            / pathlib.Path(f"{index}")
            / pathlib.Path(f"{index}.geojson")
        )
    else:
        file_path = pathlib.Path(save_file)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w") as f:
        geojson.dump(feature_collection, f, indent=2)
    logging.info(f"Generate region of interest geojson file at {file_path}.")


def map_dataset_name(
    engine: Engine, instructions_file: Union[str, pathlib.Path]
) -> None:
    """Mapping dataset name with its ordered id by publication date (and so on), and save in a json file."""
    logger.info(
        "Mapping dataset name with its ordered id by publication date (and so on)."
    )
    query = f"SELECT name, survey_end_date, publication_date FROM dataset ;"
    df = pd.read_sql(query, engine)
    # latest dataset first, if same then by name
    df = df.sort_values(
        by=["publication_date", "survey_end_date", "name"], ascending=False
    ).reset_index(drop=True)
    with open(instructions_file, "r") as f:
        instructions = json.load(f)
        if not instructions["instructions"].get("dataset_mapping"):
            instructions["instructions"]["dataset_mapping"] = {"lidar": {}}
        instructions["instructions"]["dataset_mapping"]["lidar"] = dict(
            zip(df["name"], df.index + 1)
        )
        instructions["instructions"]["dataset_mapping"]["lidar"]["Unknown"] = 0
    with open(instructions_file, "w") as f:
        json.dump(instructions, f, indent=2)


def get_geometry_from_file(
    boundary_file: Union[str, pathlib.Path], buffer: Union[int, float] = 0
) -> shapely.geometry:
    """
    Read boundary geometry boundary_file, and return the buffered geometry.
    """
    gdf = gpd.read_file(boundary_file, driver="GeoJSON")
    if "2193" not in str(gdf.crs):
        gdf = gdf.to_crs(epsg=2193)
    gdf["buffered"] = gdf["geometry"].buffer(buffer, join_style="mitre")
    return (
        gdf["geometry"].buffer(buffer, join_style="mitre").values[0]
        if buffer != 0
        else gdf["geometry"].values[0]
    )


def get_geometry_from_db(
    engine: Engine,
    table: Union[str, Type[Ttable]],
    column: Union[str, int],
    value: Union[str, int],
    buffer: Union[int, float] = 0,
) -> shapely.geometry:
    """
    return the buffered geometry by column value of table (catch_id, name, etc.).
    """
    if not isinstance(table, str):
        table = table.__tablename__
    query = f"SELECT {column}, geometry FROM {table} WHERE {column} = '{value}';"
    gdf = gpd.read_postgis(query, engine, crs="epsg:2193", geom_col="geometry")
    if gdf.empty:
        logger.error(f"Cannot find {column} = {value} in {table}.")
        return Polygon()
    geom = gdf["geometry"].unary_union
    return geom.buffer(buffer, join_style="mitre") if buffer != 0 else geom


def retrieve_dataset(
    engine: Engine,
    boundary_file: Union[str, pathlib.Path] = None,
    sort_by: str = "survey_end_date",
    buffer: Union[int, float] = 0,
    boundary_df: gpd.GeoDataFrame = None,
) -> tuple:
    """
    Read boundary geometry boundary_file,
    Query dataset to get dataset name which covers the geometry based on the boundary geometry,
    Sort the dataset name by 'sort_by', and return a dictionary of dataset name and crs.
    To safeguard the data/tile integrity, the geometry is buffered by 'buffer' distance, no buffer by default.

    :param engine: sqlalchemy engine
    :param boundary_df: boundary geodataframe, higher priority than boundary_file.
    :param boundary_file: boundary file path, geojson format. see demo examples in 'configs' directory.
    :param sort_by: sort dataset name by this column, default is 'survey_end_date'.
    :param buffer: buffer for the boundary geometry, default is 0.
    """
    if boundary_df is not None:
        geometry = (
            boundary_df["geometry"].values[0].buffer(buffer, join_style="mitre")
            if buffer != 0
            else boundary_df["geometry"].values[0]
        )
    elif boundary_file is not None:
        geometry = get_geometry_from_file(boundary_file, buffer=buffer)
    else:
        raise ValueError("Either boundary_df or boundary_file must be provided.")
    query = f"""SELECT name, {sort_by}, tile_path, geometry FROM dataset
                WHERE ST_Intersects(geometry, ST_SetSRID('{geometry}'::geometry, 2193)) ;"""
    gdf = gpd.read_postgis(query, engine, geom_col="geometry")
    gdf = gdf.sort_values(sort_by, ascending=False)  # latest/largest first
    dataset_name_list = gdf["name"].to_list()
    tile_path_list = gdf["tile_path"].to_list()
    dataset_list = [
        (n, {"crs": {"horizontal": 2193, "vertical": 7839}}) for n in dataset_name_list
    ]
    return OrderedDict(dataset_list), geometry, tile_path_list, dataset_name_list


def retrieve_lidar(
    engine: Engine,
    boundary_file: Union[str, pathlib.Path],
    sort_by: str = "survey_end_date",
    buffer: Union[int, float] = 0,
) -> OrderedDict:
    """
    Read catchment geometry from boundary_file,
    query dataset to get dataset name which intersect with the input geometry,
    sort the dataset name by 'sort_by',
    then retrieve the .laz file path, tile index file from tile table and lidar table,
    in the end return a dictionary of dataset name, crs, .laz file path and tile index file.
    """
    datasets_dict, geometry, tile_path_list, _ = retrieve_dataset(
        engine, boundary_file, sort_by, buffer=buffer
    )
    list_pop_dataset = []
    for dataset_name in datasets_dict.keys():
        query = f"""SELECT uuid, geometry FROM tile
                    WHERE ST_Intersects(geometry, ST_SetSRID('{geometry}'::geometry, 2193))
                    AND dataset = '{dataset_name}' ;"""
        gdf = gpd.read_postgis(query, engine, geom_col="geometry")
        if gdf.empty:
            logger.warning(
                f"{dataset_name} does not have any tile in the ROI geometry, will pop the dataset. "
                f"The reason may be the dataset extent in .kml file is larger than "
                f"the tile extent in tile.zip file."
            )
            list_pop_dataset.append(dataset_name)
            continue
        uuid = (
            tuple(gdf["uuid"].to_list())
            if len(gdf) > 1
            else str(f"""('{gdf["uuid"].values[0]}')""")
        )
        query = f"SELECT file_path FROM lidar WHERE uuid IN {uuid} ;"
        df = pd.read_sql(query, engine)
        if df.empty:
            logger.warning(
                f"{dataset_name} does not have any .laz file in the ROI geometry, will pop the dataset. "
                f"The reason may be the dataset lidar files are not downloaded completely."
            )
            list_pop_dataset.append(dataset_name)
            continue
        datasets_dict[dataset_name]["file_paths"] = [
            pathlib.PurePosixPath(p) for p in sorted(df["file_path"].to_list())
        ]
        if "LiDAR_" in dataset_name:  # to handle waikato datasets
            _dataset_name = "_".join(dataset_name.split("_")[:2])
            datasets_dict[dataset_name]["tile_index_file"] = [
                pathlib.PurePosixPath(p) for p in tile_path_list if _dataset_name in p
            ][0]
        else:
            datasets_dict[dataset_name]["tile_index_file"] = [
                pathlib.PurePosixPath(p) for p in tile_path_list if dataset_name in p
            ][0]
        logging.debug(
            f"Dataset {dataset_name} has "
            f'{len(datasets_dict[dataset_name]["file_paths"])} lidar files in '
            f"ROI with buffer distance {buffer} mitre."
        )
    if list_pop_dataset:
        for dataset_name in list_pop_dataset:
            datasets_dict.pop(dataset_name)
    return datasets_dict


def retrieve_catchment(
    engine: Engine,
    boundary_file: Union[str, pathlib.Path],
    buffer: Union[int, float] = 0,
) -> list:
    """
    read boundary geometry boundary_file,
    query dataset to get catch_id which covers the geometry based on the boundary geometry,
    to safeguard the data/tile integrity, the geometry is buffered by resolution * buffer_factor, no buffer by default.

    :param engine: sqlalchemy engine
    :param boundary_file: boundary file path, geojson format. see demo example in 'configs' directory.
    :param buffer: buffer factor for the boundary geometry, default is 0.
    """
    geometry = get_geometry_from_file(boundary_file, buffer=buffer)
    query = f"""SELECT catch_id, geometry FROM catchment
                WHERE ST_Intersects(geometry, ST_SetSRID('{geometry}'::geometry, 2193)) ;"""
    gdf = gpd.read_postgis(query, engine, geom_col="geometry")
    catch_list = sorted(gdf["catch_id"].to_list())
    logger.info(
        f"Retrieved {len(catch_list)} catchments from catchment table:\n{catch_list}"
    )
    return catch_list


def retrieve_dem(
    engine: Engine,
    boundary_file: Union[str, pathlib.Path],
    buffer: Union[int, float] = 0,
) -> pd.DataFrame:
    """
    Read boundary geometry boundary_file,
    Query dataset to get file path which covers the geometry based on the boundary geometry,
    To safeguard the data/tile integrity, geometry is buffered by 'buffer' distance, no buffer by default.

    :param engine: sqlalchemy engine
    :param boundary_file: boundary file path, geojson format. see demo example in 'configs' directory.
    :param buffer: buffer factor for the boundary geometry, default is 0.
    """
    catch_list = retrieve_catchment(engine, boundary_file, buffer)
    df = tables.get_data_by_id(engine, tables.DEM, catch_list, geom_col="")
    return df


def remove_holes(
    polygon: Union[Polygon, MultiPolygon],
    keep_threshold: Union[int, float] = 10_000 * 10_000,
) -> Union[Polygon, MultiPolygon]:
    """
    Convert multipolygon to polygon.
    Keep holes that area are greater than area, 100 km2 by default.

    :param polygon: shapely Polygon or MultiPolygon
    :param keep_threshold: area threshold for keeping the holes, 100 km2 by default
    """
    if isinstance(polygon, Polygon):
        multipolygon = MultiPolygon([polygon])
    else:
        multipolygon = polygon

    list_parts = []
    for geom in multipolygon.geoms:
        list_interiors = []

        for interior in geom.interiors:
            p = Polygon(interior)
            if p.area > keep_threshold:
                list_interiors.append(interior)

        temp_polygon = Polygon(geom.exterior.coords, holes=list_interiors).buffer(
            0, join_style="mitre"
        )
        # check validity
        # temp_polygon = make_valid(temp_polygon)
        list_parts.append(temp_polygon)

    return unary_union(list_parts)
    # return shapely.MultiPolygon(list_parts)


def filter_geometry(
    geometry: Union[
        shapely.Geometry, Polygon, MultiPolygon, GeometryCollection, gpd.GeoSeries
    ],
    resolution: Union[int, float] = CATCHMENT_RESOLUTION,
    polygon_threshold: Union[int, float] = 100 * 100,
    hole_threshold: Union[int, float] = 1_000 * 1_000,
) -> Union[Polygon, MultiPolygon]:
    """
    filter geometry, remove gaps, holes, tiny polygons and thin rectangle that no needed.

    :param geometry: input geometry
    :param resolution: resolution of the in put geometry in meters
    :param polygon_threshold: lower area threshold to filter out small polygons
    :param hole_threshold: lower area threshold to filter out small holes
    :return: filtered geometry
    """
    # combine geometry if possible
    if isinstance(geometry, (gpd.GeoSeries, pd.Series)):
        geometry = geometry.unary_union  # geopandas.GeoSeries.unary_union
    elif isinstance(geometry, (MultiPolygon, Polygon, GeometryCollection)):
        geometry = unary_union(geometry)  # shapely.unary_union
    else:
        raise ValueError("geometry is not a valid type", type(geometry))
    # remove polygons the under threshold
    if isinstance(geometry, Polygon):
        assert (
            geometry.area >= polygon_threshold
        ), "Input geometry is smaller than threshold."
    else:
        geometry = MultiPolygon(
            [p for p in geometry.geoms if p.area > polygon_threshold]
        )
    # remove spikes
    geometry = (
        geometry.buffer(-EPS, join_style="mitre")
        .buffer(EPS * 2, join_style="mitre")
        .buffer(-EPS, join_style="mitre")
    )
    # clean geometry boundary
    eps = resolution / 2 - EPS
    geometry = (
        geometry.buffer(eps, join_style="mitre")
        .buffer(-eps * 2, join_style="mitre")
        .buffer(eps, join_style="mitre")
    )
    # remove holes
    geometry = remove_holes(geometry, keep_threshold=hole_threshold)

    return geometry


# @timeit
def fishnet(geometry: shapely.geometry, threshold: Union[int, float]) -> list:
    """
    create fishnet grid based on the geometry and threshold
    """
    logging.info(f"Create fishnet grid with threshold {threshold}...")
    bounds = geometry.bounds
    xmin = int(bounds[0] // threshold)
    xmax = int(bounds[2] // threshold)
    ymin = int(bounds[1] // threshold)
    ymax = int(bounds[3] // threshold)
    # ncols = int(xmax - xmin + 1)
    # nrows = int(ymax - ymin + 1)
    result = []
    for i in range(xmin, xmax + 1):
        for j in range(ymin, ymax + 1):
            b = box(
                i * threshold, j * threshold, (i + 1) * threshold, (j + 1) * threshold
            )
            g = geometry.intersection(b)
            if g.is_empty:
                continue
            result.append(g)
    return result


# @timeit
def katana(
    geometry: shapely.geometry, threshold: Union[int, float], count: int = 0
) -> list:
    """Split a Polygon into two parts across its shortest dimension if area is greater than threshold."""
    bounds = geometry.bounds
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]
    if geometry.area <= threshold or count == 250:
        # either the polygon is smaller than the threshold, or the maximum
        # number of recursions has been reached
        return [geometry]
    if height >= width:
        # split left to right
        a = box(bounds[0], bounds[1], bounds[2], bounds[1] + height / 2)
        b = box(bounds[0], bounds[1] + height / 2, bounds[2], bounds[3])
    else:
        # split top to bottom
        a = box(bounds[0], bounds[1], bounds[0] + width / 2, bounds[3])
        b = box(bounds[0] + width / 2, bounds[1], bounds[2], bounds[3])
    result = []
    for d in (
        a,
        b,
    ):
        c = geometry.intersection(d)
        if not isinstance(c, GeometryCollection):
            c = [c]
        for e in c:
            if isinstance(e, (Polygon, MultiPolygon)):
                result.extend(katana(e, threshold, count + 1))
    if count > 0:
        return result
    # convert multipart into single part
    final_result = []
    for g in result:
        if isinstance(g, MultiPolygon):
            final_result.extend(g.geoms)
        else:
            final_result.append(g)
    return final_result


def gen_table_extent(
    engine: Engine, table: Union[str, Type[Ttable]], filter_it: bool = True
) -> gpd.GeoDataFrame:
    """
    Generate catchment extent from catchment table or DEM table.
    """
    if not isinstance(table, str):
        table = table.__tablename__
    if table == "hydro_dem" or table == "grid_dem":
        df = pd.read_sql(f"SELECT * FROM {table} ;", engine)
        df["geometry"] = df["extent_path"].apply(lambda x: gpd.read_file(x).geometry[0])
        if table == "grid_dem":
            gdf = gpd.GeoDataFrame(
                df[["grid_id", "geometry"]], crs="epsg:2193", geometry="geometry"
            )
        else:
            gdf = gpd.GeoDataFrame(
                df[["catch_id", "geometry"]], crs="epsg:2193", geometry="geometry"
            )
    else:
        gdf = gpd.read_postgis(
            f"SELECT * FROM {table}", engine, crs=2193, geom_col="geometry"
        )
    if filter_it:
        geom = filter_geometry(gdf["geometry"])
        gdf = gpd.GeoDataFrame(index=[0], crs=gdf.crs, geometry=[geom])
    return gdf


def check_roi_dem_exist(engine: Engine, geometry: shapely.geometry) -> tuple:
    """
    check if the ROI DEM is in the database.
    """
    # ensure USERDEM table exists
    tables.create_table(engine, tables.USERDEM)
    # check user defined DEM table first
    gdf = tables.get_catchment_by_geometry(
        engine,
        tables.USERDEM,
        geometry,
        geom_col="raw_geometry",
        relation="ST_Contains",
        buffer=-0.1,
    )
    if not gdf.empty:
        logger.info(
            f"Found DEM by geometry in table USERDEM, catch_id = {gdf['catch_id'].values[0]}."
        )
        return gdf, tables.USERDEM.__tablename__

    # ensure DEMATTR table exists
    tables.create_table(engine, tables.DEMATTR)
    # check pre-defined catchment DEM table then
    gdf = tables.get_catchment_by_geometry(
        engine,
        tables.DEMATTR,
        geometry,
        geom_col="raw_geometry",
        relation="ST_Intersects",
        buffer=10,
    )
    if not gdf.empty:
        logger.info(
            f"Found one or multiple DEMs by geometry in table DEM, "
            f"catch_id = {gdf['catch_id'].to_list()}."
        )
        return gdf, tables.DEMATTR.__tablename__

    return gpd.GeoDataFrame(), None


def get_dem_by_id(engine: Engine, index: Union[int, str, list]) -> pd.DataFrame:
    """
    get DEM file path by catch_id
    """
    if not isinstance(index, list):
        index = [index]
    index = tuple(index) if len(index) > 1 else str(f"({index[0]})")
    tables.create_table(engine, tables.DEM)
    query = f"SELECT * FROM hydro_dem WHERE catch_id IN {index} ;"
    df = pd.read_sql(query, engine)
    if df.empty:
        logger.info(f"Unable to find DEM by catch_id {index} in the database.")
    else:
        hydro_dem_path = df["hydro_dem_path"].to_list()
        raw_dem_path = df["raw_dem_path"].to_list()
        extent_path = df["extent_path"].to_list()
        for _hydro_dem_path, _raw_dem_path, _extent_path in zip(
            hydro_dem_path, raw_dem_path, extent_path
        ):
            if not pathlib.Path(_hydro_dem_path).exists():
                logging.warning(
                    f"Expected Hydro DEM File {_hydro_dem_path} does not exist."
                )
            if not pathlib.Path(_raw_dem_path).exists():
                logging.warning(
                    f"Expected Raw DEM File {_raw_dem_path} does not exist."
                )
            if not pathlib.Path(_extent_path).exists():
                logging.warning(f"Expected Extent File {_extent_path} does not exist.")
    return df


def get_dem_by_geometry(
    engine: Engine,
    geometry: Union[shapely.Geometry, gpd.GeoDataFrame, gpd.GeoSeries, pd.Series],
    index: Union[int, str] = None,
) -> tuple:
    """
    get DEM file path by geometry

    parameters
    ----------
    engine: sqlalchemy engine
    geometry: ROI geometry (polygon of selected region of interest)
    index: ROI id, default is None
    """
    hydro_dem_path = ""
    raw_dem_path = ""
    extent_path = ""
    resolution = -1

    if isinstance(geometry, (gpd.GeoSeries, pd.Series)):
        geometry = geometry.to_frame().T
    if isinstance(geometry, (gpd.GeoDataFrame, pd.DataFrame)):
        assert (
            len(geometry) == 1
        ), f"Only one geometry is allowed, {geometry.to_string()}."
        geometry = geometry["geometry"].values[0]
    gdf, table_name = check_roi_dem_exist(engine, geometry)

    # exact match in USERDEM table
    if table_name == tables.USERDEM.__tablename__:
        raw_dem_path = gdf["raw_dem_path"].values[0]
        hydro_dem_path = gdf["hydro_dem_path"].values[0]
        extent_path = gdf["extent_path"].values[0]
        resolution = gdf["resolution"].values[0]
    # contains by catchment DEMs, need clip
    elif table_name == tables.DEMATTR.__tablename__:
        clipped_gdf = clip_dem(engine, gdf, geometry, index=index)
        raw_dem_path = clipped_gdf["raw_dem_path"].values[0]
        hydro_dem_path = clipped_gdf["hydro_dem_path"].values[0]
        extent_path = clipped_gdf["extent_path"].values[0]
        resolution = clipped_gdf["resolution"].values[0]
    else:
        # should not happened in normal process pipeline
        logger.warning("Unable to find DEM by geometry in the database.")

    return hydro_dem_path, raw_dem_path, extent_path, resolution


def get_dem_band_and_resolution_by_geometry(
    engine: Engine,
    geometry: Union[shapely.Geometry, gpd.GeoDataFrame, gpd.GeoSeries],
    band: int = 1,
) -> tuple:
    """
    get DEM raster data band and resolution by geometry

    parameters
    ----------
    engine: sqlalchemy engine
    geometry: ROI geometry (polygon of selected region of interest)
    band: dataset band index, default is 1
    """
    dem_path, _, _, _ = get_dem_by_geometry(engine, geometry)

    # Open the Hydro DEM using rioxarray
    with rxr.open_rasterio(pathlib.Path(dem_path)) as f:
        # Select the first band of the Hydro DEM
        hydro_dem = f.sel(band=band)
        # Get the unique resolution from the Hydro DEM
        unique_resolution = list(set(abs(res) for res in hydro_dem.rio.resolution()))
        # Check if there is only one unique resolution
        res_no = unique_resolution[0] if len(unique_resolution) == 1 else None
        # Get the resolution from the Hydro DEM description
        res_description = int(hydro_dem.description.split()[-1])
        # Check if the resolution from the metadata matches the actual resolution
        if res_no != res_description:
            raise ValueError(
                "Inconsistent resolution between metadata and actual resolution of the Hydro DEM."
            )
        else:
            return hydro_dem, res_no


def clip_netcdf(
    file_list: list, save_file: pathlib.Path, geometry: shapely.geometry
) -> None:
    """ "
    Clip netcdf file by geometry
    """
    list_dem = []
    for file in file_list:
        if pathlib.Path(file).exists():
            # ValueError: Resulting object does not have monotonic global indexes along dimension y
            # list_xds.append(xr.open_dataset(file))
            with rxr.open_rasterio(pathlib.Path(file)) as f:
                list_dem.append(f.sel(band=1))
        else:
            logger.warning(f"Expected DEM File {file} does not exist.")
    # ValueError: Resulting object does not have monotonic global indexes along dimension y
    # xds = xr.combine_by_coords(list_xds, combine_attrs='drop', compat='no_conflicts')
    xds = rxr.merge.merge_datasets(list_dem)
    xds = xds.rio.write_crs(2193)
    xds_clipped = xds.rio.clip([geometry])
    save_file.unlink() if save_file.exists() else None
    xds_clipped.to_netcdf(save_file, mode="w")
    logger.debug(f"Save clipped NetCDF to {save_file}.")


def gen_clipped_data(
    index: int,
    df: pd.DataFrame,
    gdf: gpd.GeoDataFrame,
    save_dir: pathlib.Path,
    geometry: shapely.geometry,
):
    """
    Generate clipped netcdf file

    :param index: dem index
    :param df: dataframe from DEM table
    :param gdf: dataframe from DEMATTR table
    :param save_dir: save to this directory
    :param geometry: clip by this geometry
    """
    # set path for new files
    hydro_dem_file_list = df["hydro_dem_path"].tolist()
    raw_dem_file_list = df["raw_dem_path"].tolist()
    hydro_save_path = save_dir / pathlib.Path(f"{index}.nc")
    raw_save_path = save_dir / pathlib.Path(f"{index}_raw.nc")
    extent_save_path = save_dir / pathlib.Path(f"{index}_raw_extent.geojson")
    raw_extent_save_path = save_dir / pathlib.Path(f"{index}.geojson")

    # clip and save
    # hydro_dem
    clip_netcdf(hydro_dem_file_list, hydro_save_path, geometry)
    # raw_dem
    clip_netcdf(raw_dem_file_list, raw_save_path, geometry)
    # extent
    clipped_extent = gdf.unary_union.intersection(geometry)
    gdf_to_geojson = gpd.GeoDataFrame(
        {"geometry": [clipped_extent]}, index=[0], crs="epsg:2193", geometry="geometry"
    )
    gen_boundary_file("", gdf_to_geojson, "", save_file=extent_save_path)
    # original extent
    gdf_to_geojson = gpd.GeoDataFrame(
        {"geometry": [geometry]}, index=[0], crs="epsg:2193", geometry="geometry"
    )
    gen_boundary_file("", gdf_to_geojson, "", save_file=raw_extent_save_path)
    return clipped_extent


def clip_dem(
    engine: Engine,
    gdf: gpd.GeoDataFrame,
    geometry: shapely.geometry,
    index: Union[int, str] = None,
) -> pd.DataFrame:
    """
    Generate clipped DEM from DEM table.
    """
    if index is None:
        index = f"{datetime.now():%Y%m%d%H%M%S}"[
            -10:
        ]  # integer range up to 2,147,483,647
        logger.info(f"Input index is None, use {index} as catchment index.")
    if isinstance(index, str):
        assert index.isdigit(), f"Catchment index {index} is not digit."
    # get DEM file path
    clipped_dem_path = (
        pathlib.Path(get_env_variable("DATA_DIR"))
        / pathlib.Path(get_env_variable("DEM_DIR"))
        / pathlib.Path(f"{index}")
    )
    clipped_dem_path.mkdir(parents=True, exist_ok=True)
    catch_id = gdf["catch_id"].to_list()
    logger.info(f"Clipping catchment DEM {catch_id} to generate user DEM {index}.")
    df = get_dem_by_id(engine, catch_id)
    assert len(df) == len(
        gdf
    ), f"Retrieve {len(df)} in DEM table, while retrieve {len(gdf)} in DEMATTR table."
    clipped_dem_geometry = gen_clipped_data(index, df, gdf, clipped_dem_path, geometry)
    gdf_to_db = gpd.GeoDataFrame(
        {
            "catch_id": int(index),
            "resolution": gdf["resolution"].values[0],
            "raw_dem_path": str(clipped_dem_path / pathlib.Path(f"{index}_raw_dem.nc")),
            "hydro_dem_path": str(clipped_dem_path / pathlib.Path(f"{index}.nc")),
            "extent_path": str(
                clipped_dem_path / pathlib.Path(f"{index}_extent.geojson")
            ),
            "raw_geometry": geometry,
            "geometry": clipped_dem_geometry,
            "created_at": datetime.now(),
        },
        index=[0],
        crs="epsg:2193",
    )
    tables.create_table(engine, tables.USERDEM)
    gdf_to_db.to_postgis(
        tables.USERDEM.__tablename__, engine, if_exists="append", index=False
    )
    return gdf_to_db


def save_gpkg(gdf: gpd.GeoDataFrame, file: Union[Type[Ttable], str]):
    """
    Save source catchments to GPKG
    """
    gpkg_path = pathlib.Path(get_env_variable("DATA_DIR")) / pathlib.Path("gpkg")
    if isinstance(file, str):
        file_name = f"{file}.gpkg"
    else:
        file_name = f"{file.__tablename__}.gpkg"
    pathlib.Path(gpkg_path).mkdir(parents=True, exist_ok=True)
    gdf.set_crs(epsg=2193, inplace=True)
    gdf.to_file(str(gpkg_path / pathlib.Path(file_name)), driver="GPKG")
    logging.info(f"Save source catchments to {gpkg_path / pathlib.Path(file_name)}.")


def make_valid(geometry: shapely.geometry) -> shapely.geometry:
    """
    Returns a valid representation of the object.
    """
    if geometry.is_valid:
        return geometry
    return shapely.make_valid(geometry)


def get_min_width(geometry: shapely.geometry) -> float:
    """
    Get minimum width of the geometry
    """
    bounds = geometry.bounds
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]
    return min(width, height)


def delete_dir(directory: Union[str, pathlib.Path]) -> None:
    """
    Delete directory
    """
    if isinstance(directory, str):
        directory = pathlib.Path(directory)
    if directory.exists():
        shutil.rmtree(directory)
        logger.info(f"Delete directory {directory}.")
