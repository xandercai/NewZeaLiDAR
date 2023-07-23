# -*- coding: utf-8 -*-
"""
This module is used to define the database tables and utility functions for the tables.
"""
import logging
from typing import Type, TypeVar, Union

import geopandas as gpd
import pandas as pd
import shapely
from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, Float, String, Date, DateTime, inspect
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

from src.utils import timeit, get_database, cast_geodataframe

logger = logging.getLogger(__name__)

Base = declarative_base()
Ttable = TypeVar("Ttable", bound=Base)

EPS = 0.1  # epsilons for small float number convenience.
CATCHMENT_RESOLUTION = 30  # resolution of catchment geometry


# define dataset table
class DATASET(Base):
    __tablename__: str = "dataset"
    id = Column(Integer, comment='index in the table')
    name = Column(String, primary_key=True, comment='dataset name')
    describe = Column(String, comment='dataset short description')
    # source = Column(String, comment='dataset download source')
    # source_id = Column(String, comment='dataset id in the source')
    survey_start_date = Column(Date, comment='survey start date')
    survey_end_date = Column(Date, comment='survey end date')
    # points_per_square_meter = Column(Float, comment='point density')
    point_cloud_density = Column(Float,
                                 comment='indicator of the resolution of the data in points per square meter')
    original_datum = Column(String, comment='original vertical datum')
    # collector = Column(String, comment='data maker')
    meta_path = Column(String, comment='metadata file path')
    meta_source = Column(String, comment='metadata source address')
    extent_path = Column(String, comment='extent file path')
    # extent_source = Column(String, comment='extent source address')
    tile_path = Column(String, comment='tile file path')
    geometry = Column(Geometry("GEOMETRY", srid="2193"), comment='dataset extent')
    created_at = Column(DateTime, comment='table created time')
    updated_at = Column(DateTime, comment='table updated time')


# define lidar table
class LIDAR(Base):
    __tablename__: str = "lidar"
    uuid = Column(UUID(as_uuid=True), primary_key=True, comment='point cloud file uuid')
    file_path = Column(String, comment='point cloud file path')
    created_at = Column(DateTime, comment='table created time')


# define tile table
class TILE(Base):
    __tablename__: str = "tile"
    uuid = Column(UUID(as_uuid=True), primary_key=True, comment='point cloud file uuid')
    file_name = Column(String, comment='point cloud file name')
    dataset = Column(String, comment='dataset name of the point cloud file')
    source = Column(String, comment='point cloud file source link')
    # min_x = Column(Float, comment='minimum x coordinate of the point cloud file')
    # min_y = Column(Float, comment='minimum y coordinate of the point cloud file')
    # max_x = Column(Float, comment='maximum x coordinate of the point cloud file')
    # max_y = Column(Float, comment='maximum y coordinate of the point cloud file')
    geometry = Column(Geometry("POLYGON", srid="2193"), comment='point cloud file extent')
    created_at = Column(DateTime)


class CATCHMENT(Base):
    __tablename__: str = "catchment"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


class CATCHTEMP(Base):
    __tablename__: str = "catchment_temporary"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


class CATCHMENTG(Base):
    __tablename__: str = "catchment_gap"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


# define hydrologically conditioned DEM table
class DEM(Base):
    __tablename__: str = "hydro_dem"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    raw_dem_path = Column(String, comment='raw DEM file path')
    hydro_dem_path = Column(String, comment='hydrologically conditioned DEM file path')
    extent_path = Column(String, comment='DEM extent file path')
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


# define sea draining catchments table
class SDC(Base):
    __tablename__: str = "sea_draining_catchment"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


# define the relationship between sea draining catchments and its split catchments if any
class SDCS(Base):
    __tablename__: str = "sea_draining_catchment_split"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    super_id = Column(Integer, comment='superior catchment area index')
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


# define processed sea draining catchments table that processed (deduplicated)
class SDCP(Base):
    __tablename__: str = "sea_draining_catchment_processed"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


# define the gaps table of sea draining catchments
class SDCG(Base):
    __tablename__: str = "sea_draining_catchment_gap"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


# Order 5 catchments
class ORDER5(Base):
    __tablename__: str = "order5_catchment"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


# Order 5 catchments processed (deduplicated)
class ORDER5P(Base):
    __tablename__: str = "order5_catchment_processed"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


# Order 4 catchments
class ORDER4(Base):
    __tablename__: str = "order4_catchment"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


# Order 4 catchments processed (deduplicated)
class ORDER4P(Base):
    __tablename__: str = "order4_catchment_processed"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


# coast catchments
class COAST(Base):
    __tablename__: str = "coast_catchment"
    catch_id = Column(Integer, primary_key=True, comment='catchment area index')  # catchment region id
    area = Column(Float, comment='catchment area in square meters')
    geometry = Column(Geometry("Geometry", srid="2193"), comment='catchment area extent')
    created_at = Column(DateTime)


def create_table(engine, table: Type[Ttable]) -> None:
    """Create table if it doesn't exist."""
    table.__table__.create(bind=engine, checkfirst=True)


def delete_table(engine, table: Type[Ttable], column: str = None, key: str = None) -> None:
    """Delete table records by key or delete all records, but keep table schema."""
    if isinstance(column, str) and isinstance(key, str):
        query = f"DELETE FROM {table.__table__} WHERE {column} = '{key}' ;"
    else:
        query = f"DELETE FROM {table.__tablename__} ;"
    engine.execute(query)


def prepare_to_db(gdf_in: gpd.GeoDataFrame, check_empty: bool = True) -> gpd.GeoDataFrame:
    """
    Prepare geodataframe to be saved to database.
    the input dataframe must at least contains 'catch_id', 'area', and 'geometry' columns.

    :param gdf_in: input geodataframe
    :param check_empty: check empty catchments or not, default True
    """
    gdf = gdf_in.copy()
    assert gdf['catch_id'].is_unique, 'catch_id column must be unique'
    if check_empty:
        # filter out empty catchments
        list_empty = gdf[gdf['area'] <= EPS]['catch_id'].to_list()
        if list_empty:
            logger.warning(f"Filter out empty catchments: {list_empty}")
        gdf = gdf[gdf['area'] > EPS]
    gdf = cast_geodataframe(gdf)
    gdf['created_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %X')
    columns = gdf.columns.to_list()
    _ = [columns.remove(x) for x in ['catch_id', 'area', 'geometry', 'created_at']]
    columns = ['catch_id', 'area'] + columns + ['geometry', 'created_at']
    gdf = gdf[columns]
    gdf = gdf.sort_values('catch_id')
    gdf = gdf.set_index('catch_id')
    return gdf


def create_catchment_table(engine, table: Type[Ttable], gdf: gpd.GeoDataFrame, columns: list, ) -> None:
    """
    Create or replace catchments table.

    :param engine: database engine
    :param table: catchments table class
    :param gdf: input catchments dataframe
    :param columns: input catchments dataframe columns, note colunms[0] is catchment id, columns[1] is catchment area
    :return: None
    """
    gdf = gdf.rename(columns={columns[0]: 'catch_id', columns[1]: 'area'})
    gdf = prepare_to_db(gdf)
    create_table(engine, table)
    gdf.to_postgis(table.__tablename__, engine, index=True, if_exists="replace")


def check_columns_up_to_3(table: Type[Ttable], column_1: str, column_2: str, column_3: str) -> tuple:
    """check columns, support up to 3 columns."""
    columns = table.__table__.columns.keys()
    c1 = isinstance(column_1, str) and column_1 in columns
    c2 = isinstance(column_2, str) and column_2 in columns
    c3 = isinstance(column_3, str) and column_3 in columns
    return c1, c2, c3


# @timeit
def check_table_duplication(engine,
                            table: Type[Ttable],
                            column_1: str,
                            column_2: str = None,
                            column_3: str = None) -> bool:
    """
    check duplicate table based on column/columns.
    support up to 3 columns to compare.
    by default, check the first column only.
    """
    c1, c2, c3 = check_columns_up_to_3(table, column_1, column_2, column_3)
    table = table.__tablename__
    query = None
    # column_1
    if c1 and not c2 and not c3:
        query = f"SELECT COUNT(*) FROM {table} GROUP BY {column_1} HAVING COUNT(*) > 1 ; "
    # column_1 and column_2
    if c1 and c2 and not c3:
        query = f"SELECT COUNT(*) FROM {table} GROUP BY {column_1}, {column_2} HAVING COUNT(*) > 1 ;"
    # column_1, column_2 and column_3
    if c1 and c2 and c3:
        query = f"SELECT COUNT(*) FROM {table} GROUP BY {column_1}, {column_2}, {column_3} HAVING COUNT(*) > 1 ;"
    assert query is not None, (
        f"Input values error: table {table}, columns {column_1} {column_2} {column_3}."
    )
    result = engine.execute(query).fetchall()
    count_all = len(result)
    count_1 = result[0][0] if count_all > 0 else 0  # first duplication count
    if count_all > 0:
        logger.warning(f"Table {table} duplication check: columns: 1 {column_1}, 2 {column_2}, 3 {column_3}, "
                        f"find total {count_all} rows with {count_1} duplicated each.")
    return True if count_all > 0 else False


# @timeit
def deduplicate_table(engine,
                      table: Type[Ttable],
                      column_1: str,
                      column_2: str = None,
                      column_3: str = None,
                      index: str = 'uuid') -> None:
    """
    deduplicate table based on column/columns, keep the latest based on crated_at column value.
    support up to 3 columns to compare.
    """
    c1, c2, c3 = check_columns_up_to_3(table, column_1, column_2, column_3)
    table = table.__tablename__
    query = None
    # column_1
    if c1 and not c2 and not c3:
        query = (
            f"""
                DELETE FROM {table}
                WHERE {index} IN
                (
                    SELECT {index}
                    FROM (
                        SELECT
                            *,
                            row_number() OVER (PARTITION BY {column_1} ORDER BY created_at DESC)
                        FROM {table}
                    ) s
                    WHERE row_number > 1
                )
            """)

    # column_1 and column_2
    if c1 and c2 and not c3:
        query = (
            f"""
                DELETE FROM {table}
                WHERE {index} IN
                (
                    SELECT {index}
                    FROM (
                        SELECT
                            *,
                            row_number() OVER (PARTITION BY {column_1}, {column_2} ORDER BY created_at DESC)
                        FROM {table}
                    ) s
                    WHERE row_number > 1
                )
            """)

    # column_1, column_2 and column_3
    if c1 and c2 and c3:
        # slow approach:
        # query = (f"""DELETE FROM {table} WHERE created_at NOT IN
        #              (SELECT MAX(created_at) FROM {table} GROUP BY {column_1}, {column_2}, {column_3}) ;""")
        # faster approach:
        query = (
            f"""
                DELETE FROM {table}
                WHERE {index} IN
                (
                    SELECT {index}
                    FROM (
                        SELECT
                            *,
                            row_number() OVER (PARTITION BY {column_1}, {column_2}, {column_3}
                            ORDER BY created_at DESC)
                        FROM {table}
                    ) s
                    WHERE row_number > 1
                )
            """)
    assert query is not None, (
        f"Input values error: table {table}, columns {column_1} {column_2} {column_3}."
    )
    # try:
    #     result = engine.execute(query).fetchll()
    #     for line in result:
    #         logger.info(line[0])
    # except Exception as e:
    #     logger.error(f"Error: {e}")
    logger.debug(f"Table {table} deduplicating: columns: 1 {column_1}, 2 {column_2}, 3 {column_3} with index {index}.")
    engine.execute(query)


# not used due to create_table will check table first.
def is_table_exist(engine, table: str):
    """Check table if it exists."""
    return inspect(engine).has_table(table)


def read_postgis_table(engine,
                       table: Union[str, Type[Ttable]],
                       limit: int = 0,
                       sort_by: str = None,
                       desc: bool = True) -> gpd.GeoDataFrame:
    """Read table from postgis into GeoDataFrame."""
    if not isinstance(table, str):
        table = table.__tablename__

    if sort_by is None:
        if limit == 0:
            query = f"SELECT * FROM {table} ;"
        else:
            query = f"SELECT * FROM {table} LIMIT {limit} ;"
    else:
        desc = "DESC" if desc else "ASC"
        if limit == 0:
            query = f"SELECT * FROM {table} ORDER BY {sort_by} {desc} ;"
        else:
            query = f"SELECT * FROM {table} ORDER BY {sort_by} {desc} LIMIT {limit} ;"
    return gpd.read_postgis(query, engine, geom_col="geometry")


def get_data_by_id(engine,
                   table: Union[Type[Ttable], str],
                   index: Union[int, str, list],
                   geom_col: str = 'geometry') -> gpd.GeoDataFrame:
    """retrieve db by id to get catchment boundary geometry"""
    if not isinstance(index, list):
        index = [index]
    retrieve_index = tuple(index) if len(index) > 1 else str(f"({index[0]})")
    if not isinstance(table, str):
        table = table.__tablename__
    query = f"SELECT * FROM {table} WHERE catch_id IN {retrieve_index} ;"
    if len(geom_col):
        df = gpd.read_postgis(query, engine, geom_col=geom_col)
    else:
        df = pd.read_sql(query, engine)
    if len(df) != len(index):
        not_exist = [i for i in index if i not in df['catch_id'].to_list()]
        logger.warning(f"Try find {index}, but {not_exist} not exist in table {table}.")
    return df


def get_adjacent_catchment_by_id(engine,
                                 table: Type[Ttable],
                                 index: Union[int, str, list],
                                 buffer: Union[int, float] = CATCHMENT_RESOLUTION + EPS,
                                 order_by: str = 'area',
                                 desc: bool = True) -> list:
    """
    retrieve db by id to get adjacent catchment boundary geometry
    note the results contains the input index
    """
    if not isinstance(index, list):
        index = [index]

    gdf_concat = gpd.GeoDataFrame(geometry=gpd.GeoSeries())
    for i in index:
        query = f"""SELECT * FROM {table.__table__} WHERE catch_id = {i} ;"""
        gdf = gpd.read_postgis(query, engine, geom_col='geometry')
        geom = gdf['geometry'].values[0]
        if buffer > 0:
            geom = geom.buffer(buffer, join_style='mitre')
        query = f"""SELECT * FROM {table.__table__}
                    WHERE ST_Intersects(geometry, ST_SetSRID('{geom}'::geometry, 2193)) ;"""
        gdf = gpd.read_postgis(query, engine, geom_col='geometry')
        gdf_concat = pd.concat([gdf_concat, gdf], ignore_index=True)

    if gdf_concat.empty:
        logger.warning(f"Input index {index} has no adjacent catchment.")
        return []

    if order_by is not None:
        gdf_concat = gdf_concat.sort_values(by=order_by, ascending=(not desc)).reset_index(drop=True)

    return gdf_concat['catch_id'].unique().astype(dtype=int).tolist()


def get_within_catchment_by_geometry(engine,
                                     table: Union[str, Type[Ttable]],
                                     geom: Union[shapely.Geometry, gpd.GeoDataFrame, gpd.GeoSeries, pd.Series],
                                     buffer: Union[int, float] = -CATCHMENT_RESOLUTION*2-EPS,
                                     desc: bool = None) -> gpd.GeoDataFrame:
    """
    retrieve table by geometry to get catchment dataframe from the table which contains the input catchment geometry.

    :param engine: sqlalchemy engine
    :param table: table name or object to query
    :param geom: shapely geometry, dataframe or series to be checked
    :param buffer: buffer distance to be applied to the geometry in the table
    :param desc: None - both side, False - only check index larger than input index, True - only check smaller index
    :return: GeoDataFrame
    """
    if not isinstance(table, str):
        table = table.__tablename__
    index = None
    if isinstance(geom, (gpd.GeoSeries, pd.Series)):
        assert len(geom.to_frame().T) == 1, f"Only one geometry is allowed, {type(geom)} \n{geom.to_string()}."
        index = geom['catch_id']
        geom = geom['geometry']
    if isinstance(geom, gpd.GeoDataFrame):
        assert len(geom) == 1, f"Only one geometry is allowed, {type(geom)} \n{geom.to_string()}."
        index = geom['catch_id'].values[0]
        geom = geom['geometry'].values[0]
    geom = shapely.buffer(geom, buffer, join_style='mitre') if buffer != 0 else geom
    if desc is None or index is None:
        query = f"""SELECT * FROM {table}
                    WHERE ST_Contains(geometry, ST_SetSRID('{geom}'::geometry, 2193)) ;"""
    elif desc:
        query = f"""SELECT * FROM {table}
                    WHERE ST_Contains(geometry, ST_SetSRID('{geom}'::geometry, 2193)) AND catch_id <= {index} ;"""
    else:
        query = f"""SELECT * FROM {table}
                    WHERE ST_Contains(geometry, ST_SetSRID('{geom}'::geometry, 2193)) AND catch_id >= {index} ;"""
    return gpd.read_postgis(query, engine, geom_col='geometry')


def read_postgres_table(engine, table: str, limit: int = 0) -> pd.DataFrame:
    """Read table from postgres into DataFrame."""
    if limit == 0:
        query = f"SELECT * FROM {table} ;"
    else:
        query = f"SELECT * FROM {table} LIMIT {limit} ;"
    return pd.read_sql(query, engine)


def check_table_info(engine, table: str, schema: str = 'public'):
    """Get table information"""
    query = f"SELECT * FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}' ;"
    return pd.read_sql(query, engine)


def get_max_value(engine, table: Union[str, Type[Ttable]], column: str = 'id') -> Union[int, float]:
    """Get max value of a column from table"""
    if not isinstance(table, str):
        query = f"SELECT MAX({column}) FROM {table.__tablename__} ;"
    else:
        query = f"SELECT MAX({column}) FROM {table} ;"
    return engine.execute(query).fetchall()[0][0]


def get_min_value(engine, table: Union[str, Type[Ttable]], column: str = 'id') -> Union[int, float]:
    """Get min value of a column from table"""
    if not isinstance(table, str):
        query = f"SELECT MIN({column}) FROM {table.__tablename__} ;"
    else:
        query = f"SELECT MIN({column}) FROM {table} ;"
    return engine.execute(query).fetchall()[0][0]


def check_all_table_duplicate():
    """Check all table duplication"""
    logger.info(f"*** Checking all table duplication ***")
    engine = get_database()
    logger.debug(f"*** Checking sea_draining_catchments table duplication ***")
    check_table_duplication(engine, SDC, 'catch_id')
    logger.debug(f"*** Checking dataset table duplication ***")
    check_table_duplication(engine, DATASET, 'name')
    logger.debug(f"*** Checking tile table duplication ***")
    check_table_duplication(engine, TILE, 'dataset', 'file_name')
    logger.debug(f"*** Checking lidar table duplication ***")
    check_table_duplication(engine, LIDAR, 'file_path')
    engine.dispose()


def get_id_under_area(engine, table: Union[str, Type[Ttable]], area: Union[int, float]) -> list:
    """Get catchment id list by area limit."""
    if not isinstance(table, str):
        table = table.__tablename__
    query = f"""SELECT catch_id FROM {table} WHERE area < {area} ;"""
    df = pd.read_sql(query, engine)
    return df.iloc[:, 0].to_list()  # the first column is catch_id by default.


def get_catchment_by_area_range(engine,
                                table: Union[str, Type[Ttable]],
                                upper_area: Union[int, float, None],
                                lower_area: Union[int, float, None]) -> gpd.GeoDataFrame:
    """
    Get catchment id list by area limit.
    :param engine: database engine
    :param table: table name
    :param upper_area: upper area limit
    :param lower_area: lower area limit
    """
    if not isinstance(table, str):
        table = table.__tablename__
    if upper_area is not None and lower_area is None:
        query = f"""SELECT * FROM {table} WHERE area < {upper_area} ;"""
    elif upper_area is None and lower_area is not None:
        query = f"""SELECT * FROM {table} WHERE area >= {lower_area} ;"""
    elif upper_area is not None and lower_area is not None:
        query = f"""SELECT * FROM {table} WHERE area < {upper_area} AND area >= {lower_area} ;"""
    else:
        raise ValueError("Error: upper_area and lower_area cannot be None at the same time.")
    gdf = gpd.read_postgis(query, engine, geom_col="geometry")
    # assert gdf.isnull().values.any() == False, f"Error: {table} has null value."
    return gdf


def get_split_catchment_by_id(engine,
                              index: Union[str, int, list],
                              sub: bool = True) -> list:
    """
    Get split subordinate catchment id list by superior catchment id if sub is True.
    Get split superior catchment id by subordinate catchment id if sub is False.
    the table is fix to sea_draining_catchment_split

    :param engine: database engine
    :param index: catchment id to be searched
    :param sub: True for getting subordinate catchment ids, False for superior catchment id
    """
    table = SDCS.__tablename__
    if not isinstance(index, list):
        index = [index]
    index = tuple(index) if len(index) > 1 else str(f"({index[0]})")
    column = ['super_id', 'catch_id']
    if sub:
        query = f"""SELECT * FROM {table} WHERE {column[0]} in {index} ;"""
    else:
        query = f"""SELECT * FROM {table} WHERE {column[1]} in {index} ;"""
    gdf = gpd.read_postgis(query, engine, geom_col="geometry")
    result = sorted(gdf['catch_id'].to_list()) if sub else sorted(list(set(gdf['super_id'].to_list())))
    return result
