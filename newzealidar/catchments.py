# -*- coding: utf-8 -*-
"""
This module is used to fetch sea draining catchments data from data.mfe.govt.nz, generate coast catchments,
split large catchments to small catchments, and save to local database.
"""
import gc
import logging
import multiprocessing
import pathlib
from itertools import repeat
from multiprocessing.pool import ThreadPool as Pool
from typing import Type, Union

import geopandas as gpd
import pandas as pd
import shapely
from geoapis.vector import WfsQueryBase
from sqlalchemy.engine import Engine

from newzealidar import tables
from newzealidar import utils

logger = logging.getLogger(__name__)

EPS = 0.1  # epsilon for small float number convenience.
CATCHMENT_RESOLUTION = 30  # resolution of catchment geometry, unit is meter.
COAST_DISTANCE = 6000  # distance from coast, unit is meter.
COAST_GRID = 20_000  # grid size of coast, unit is meter.
# UPPER_AREA = 800_000_000  # if area is larger than this, it will be split.
# UPPER_AREA = 600_000_000  # if area is larger than this, it will be split.
UPPER_AREA = 100_000_000_000  # do not split.
LOWER_AREA = (
    CATCHMENT_RESOLUTION * CATCHMENT_RESOLUTION - EPS
)  # if area is smaller than this, it will be filtered out.
GRID_SIZE = 10_000  # grid size of split land to calculate raw dem, unit is meter.

""" Catch_id range of different datasets to avoid overlap in the same dataframe or table

Source data ------- Sea draining catchments ------------ 1 - 10_131
Source data ------- Order 4 catchments --------- 1_000_075 - 15_067_760
Source data ------- Order 5 catchments --------- 1_002_295 - 15_066_882
Generated data ---- Coast catchments -------------- 20_001 - 30_000
Generated data ---- Split catchments ---------- 20_000_001 - 30_000_000
Generated data ---- Gap in catchment table ---- 30_000_001 - 320_000_000
Generated data ---- Gap in gap table ---------- 32_000_001 - 40_000_000

To implement about catch_id range, set the following global constant:

e.g.:
    A large catchment, catch_id = 1_234, split into subordinate catchments (Order5 or/and Order4) and split catchments,
    the start catch_id of split catchments can be calculated as below:
        catch_id * SPLIT_SHIFT + SPLIT_OFFSET = 1_234 * 1_000 + 20_000_001 = 20_1234_001

Note that the upper limits of the above range are not checked since they will not be exceeded logically.
"""
COAST_OFFSET = 20_001
SPLIT_OFFSET = 20_000_001
SPLIT_SHIFT = 1_000
GAP_OFFSET = 32_000_001  # final gap catchment in the database will be `i + GAP_OFFSET - GAP_REVISE`
GAP_REVISE = 2_000_000

# mapping dictionary
GAP_TABLE_MAPPING = {
    tables.SDCP.__tablename__: tables.SDCG,
    tables.CATCHMENT.__tablename__: tables.CATCHMENTG,
}


class MFE(WfsQueryBase):
    """
    A class to manage fetching Vector data from data.mfe.govt.nz.

    General details at: https://data.mfe.govt.nz/
    API details at: https://help.koordinates.com

    Note that the 'GEOMETRY_NAMES' used when making a WFS 'cql_filter' queries varies
    between layers. the data.mfe server generally follows the LINZ LDS but uses 'Shape' in place of
    'shape'. It still uses 'GEOMETRY'.
    """

    NETLOC_API = "data.mfe.govt.nz"
    GEOMETRY_NAMES = ["GEOMETRY", "Shape"]


def fetch_data_from_mfe(
    layer: int,
    crs: int = 2193,
    bounding_polygon: gpd.GeoDataFrame = None,
    verbose: bool = True,
) -> gpd.GeoDataFrame:
    """
    fetching data from data.mfe.govt.nz

    :param layer: layer id of data.mfe.govt.nz
    :param crs: coordinate reference system, default is 2193
    :param bounding_polygon: bounding geometry to fetch, default is None
    :param verbose: print log or not, default is True
    :return: fetched data
    """
    key = utils.get_env_variable("MFE_API_KEY")
    vector_fetcher = MFE(
        key=key, crs=crs, bounding_polygon=bounding_polygon, verbose=verbose
    )
    return vector_fetcher.run(layer)


def gen_source_catchment_table(engine: Engine, gpkg: bool = False) -> None:
    """
    fetching catchment data from data.mfe.govt.nz and save to local database
    current version fetches sea draining catchments, order 5 catchments, order 4 catchments.

    :param engine: database engine
    :param gpkg: save catchments to geopackage or not, default is True
    """
    logger.info("Fetch catchments data from data.mfe.govt.nz ...")
    list_layers = [
        99776,
    ]  # sea draining catchments, order 5 catchments, order 4 catchments
    list_tables = [tables.SDC]
    column_sea = ["Catch_id", "Shape_Area", "geometry"]
    list_columns = [column_sea]
    for layer, table, column in zip(list_layers, list_tables, list_columns):
        gdf = fetch_data_from_mfe(layer)
        gdf = gdf[column].copy()
        tables.create_catchment_table(engine, table, gdf, column)
        logger.info(
            f"Finish fetching {table.__tablename__} data from data.mfe.govt.nz."
        )
        if gpkg:
            utils.save_gpkg(gdf, table)


def initiate_tables(gpkg: bool = False) -> None:
    """
    fetching catchment data from data.mfe.govt.nz and save to local database
    current version fetches sea draining catchments, order 5 catchments, order 4 catchments.

    :param gpkg: save catchments to geopackage or not, default is True
    """
    engine = utils.get_database()
    # get data from data.mfe.govt.nz
    gen_source_catchment_table(engine, gpkg)
    # add other table initialization here:
    # ...
    engine.dispose()
    gc.collect()


def check_duplicate_catchments(
    gds: Union[gpd.GeoSeries, pd.Series],
    table: Type[tables.Ttable],
    buffer: Union[int, float] = -10,
) -> list:
    """
    check if there are duplicate catchments in database.

    :param gds: input GeoSeries of catchment geometry
    :param table: table to check
    :param buffer: buffer distance, default is -10
    """
    engine = utils.get_database(null_pool=True)
    # it will check duplicate and overlap catchments, so cannot utilise `desc` parameter for saving time.
    gdf = tables.get_catchment_by_geometry(engine, table, gds, buffer=buffer)
    engine.dispose()
    gc.collect()
    assert (
        len(gdf) >= 1
    ), f"Unexpected check result, it must contains itself: {gds['catch_id']} {table.__tablename__}"
    list_result = []
    if len(gdf) > 1:
        gdf = gdf.sort_values(by=["area"], ascending=False)
        list_result = gdf["catch_id"].tolist()
        logger.debug(f"{table.__tablename__}, find duplicate catchments {list_result}")
    return list_result


# @utils.timeit
def deduplicate_single_table(
    source_table: Type[tables.Ttable],
    processed_table: Type[tables.Ttable],
    buffer: Union[int, float] = -10,
    parallel: bool = True,
    gpkg: bool = False,
) -> None:
    """
    deduplicate catchments by area and geometry.

    :param source_table: table to deduplicate
    :param processed_table: table to save deduplicated catchments
    :param buffer: buffer distance, default is -10
    :param parallel: parallel running or not, default is True
    :param gpkg: save catchments to geopackage or not, default is False
    """
    logger.info(f"Start deduplicating table {source_table.__tablename__} ...")
    engine = utils.get_database()
    gdf = tables.read_postgis_table(engine, source_table)
    engine.dispose()
    gc.collect()

    gdf = gdf.sort_values(by=["catch_id"]).reset_index(drop=True)

    if parallel:  # SDC 0:08:49.020825, ORDER4 0:00:03.879000, ORDER5 0:00:38.870021
        list_gds = [gds for _, gds in gdf.iterrows()]
        with Pool(processes=multiprocessing.cpu_count()) as pool:
            list_result = pool.starmap(
                check_duplicate_catchments,
                zip(list_gds, repeat(source_table), repeat(buffer)),
            )
            pool.close()
            pool.join()
    else:
        list_result = []
        for _, gds_row in gdf.iterrows():
            result = check_duplicate_catchments(gds_row, source_table, buffer)
            list_result.append(result)

    list_deduplicate = []
    for list_i in list_result:
        if list_i:
            # keep the lager catchment
            list_deduplicate.extend(list_i[1:])
    # remove redundant indexes
    list_deduplicate = sorted(list(set(list_deduplicate)))
    if list_deduplicate:
        gdf_to_db = gdf[~gdf["catch_id"].isin(list_deduplicate)]
        gdf_to_db = gdf_to_db.sort_values(by=["catch_id"]).reset_index(drop=True)
        logger.info(
            f"Found {len(list_deduplicate)} duplicate catchments in table {source_table.__tablename__}."
        )
        logger.info(
            f"Deduplicate data and save data in table {processed_table.__tablename__}, "
            f"duplicated catchments:\n{list_deduplicate}"
        )
    else:
        gdf_to_db = gdf.copy()
        logger.info(
            f"No duplicate catchments found in table {source_table.__tablename__}."
        )

    if gpkg:
        utils.save_gpkg(gdf_to_db, source_table.__tablename__ + "_deduplicated")

    gdf_to_db = tables.prepare_to_db(gdf_to_db)
    engine = utils.get_database()
    gdf_to_db.to_postgis(
        processed_table.__tablename__,
        engine,
        index=True,
        if_exists="replace",
        chunksize=4096,
    )
    engine.dispose()
    gc.collect()


# @utils.timeit  # 0:09:16.738249 for SDC, ORDER4, ORDER5 in total; 0:07:20.119389 for SDCP; 0:09:49.277458 for CATCHTMP
def deduplicate_table(
    source_table: Union[Type[tables.Ttable], list],
    processed_table: Union[Type[tables.Ttable], list],
    buffer: Union[int, float] = -10,
    parallel: bool = True,
    gpkg: bool = False,
) -> None:
    """
    deduplicate catchments in all tables.
    """
    if not isinstance(source_table, list):
        source_table = [source_table]
    if not isinstance(processed_table, list):
        processed_table = [processed_table]
    for s, p in zip(source_table, processed_table):
        deduplicate_single_table(s, p, buffer=buffer, parallel=parallel, gpkg=gpkg)


def extend_boundary(
    index: int, list_id: list, table: Type[tables.Ttable]
) -> gpd.GeoDataFrame:
    """
    extend boundary of a catchment to adjacent catchments, to remove holes, silvers, and spikes between polygons.
    """
    engine = utils.get_database(null_pool=True)
    # find adjacent catchments
    list_adj_id = tables.get_adjacent_catchment_by_id(engine, table, index)
    assert (
        index in list_id and index in list_adj_id
    ), f"Unexpected index: {index} not in {list_id} or {list_adj_id}"
    i = list_id.index(index)
    ignore = list_id[:i]
    list_adj_id = [i for i in list_adj_id if i not in ignore]
    if len(list_adj_id) < 2:
        if len(list_adj_id) == 0:
            logger.warning(
                f"Find empty geometry catchment, catch_id: {index}, adjacent_id: {list_adj_id}"
            )
        if len(list_adj_id) == 1:
            assert (
                list_adj_id[0] == index
            ), f"unexpected adjacent_id: {list_adj_id} of catch_id: {index}."
            logger.debug(
                f"{table.__tablename__}, "
                f"Ignoring {i + 1:>5}th catchment, catch_id: {index:>8}, adjacent catch_id: {list_adj_id}"
            )
        gdf = tables.get_data_by_id(engine, table, index)
        engine.dispose()
        gc.collect()
        return gdf
    logger.debug(
        f"{table.__tablename__}, "
        f"Refining {i + 1:>5}th catchment, catch_id: {index:>8}, adjacent catch_id: {list_adj_id}"
    )
    gdf = tables.get_data_by_id(engine, table, list_adj_id)
    engine.dispose()
    gc.collect()
    # rest catchments - exclude the current index catchment
    gdf_r = gdf[gdf["catch_id"] != index]
    assert (
        gdf_r.empty is False
    ), f"unexpected empty rest geometry, catch_id: {index}, adjacent_id: {list_adj_id}"
    # original geometry - all adjacent catchments including the current index catchment
    geom_o = utils.filter_geometry(
        gdf["geometry"], resolution=CATCHMENT_RESOLUTION, polygon_threshold=LOWER_AREA
    )
    # rest geometry - all adjacent catchments excluding the current index catchment
    geom_r = utils.filter_geometry(
        gdf_r["geometry"], resolution=CATCHMENT_RESOLUTION, polygon_threshold=LOWER_AREA
    )
    # difference geometry - it contains holes, silvers, and spikes between polygons of the current index catchment
    geom_d = geom_o.difference(geom_r)
    geom_d = utils.filter_geometry(
        geom_d, resolution=CATCHMENT_RESOLUTION, polygon_threshold=LOWER_AREA
    )
    assert geom_d.area > EPS, (
        f"unexpected empty difference geometry, catch_id: {index}, area: {geom_d.area}, "
        f"adjacent_id: {list_adj_id}"
    )
    gdf_result = gpd.GeoDataFrame(index=[0], crs="epsg:2193", geometry=[geom_d])
    gdf_result["catch_id"] = index
    gdf_result["area"] = geom_d.area
    return gdf_result


# @utils.timeit  # 0:21:48.086703 for SDCP; 0:39:41.483969 for CATCHTEMP
def extend_catchments(
    table: Type[tables.Ttable], parallel: bool = True, gpkg: bool = False
) -> None:
    """
    extend boundary of catchments to adjacent catchments, to remove holes, silvers, and spikes between polygons.
    """
    logger.info(f"Extending catchment geometry in table {table.__tablename__}...")
    engine = utils.get_database()
    gdf = tables.read_postgis_table(
        engine, table.__tablename__, sort_by="area", desc=True
    )
    assert gdf.empty is False, f"unexpected empty table {table.__tablename__}."
    list_area = gdf["area"].to_list()[
        :10
    ]  # check top 10 largest catchments to save time
    assert all(
        list_area[i] >= list_area[i + 1] for i in range(len(list_area) - 1)
    ), "area should be sorted."
    engine.dispose()
    gc.collect()

    list_id = gdf["catch_id"].to_list()

    if parallel:  # runtime: 0:24:10.686879.
        with Pool(processes=multiprocessing.cpu_count()) as pool:
            list_result = pool.starmap(
                extend_boundary, zip(list_id, repeat(list_id), repeat(table))
            )
            pool.close()
            pool.join()
        gdf_result = pd.concat(list_result, ignore_index=True)
    else:  # runtime: 1:45:46.976877.
        gdf_result = gpd.GeoDataFrame(geometry=gpd.GeoSeries())
        for i, catch_id in enumerate(list_id):
            gdf = extend_boundary(catch_id, list_id, table)
            gdf_result = pd.concat([gdf_result, gdf], ignore_index=True)

    if gpkg:
        utils.save_gpkg(gdf_result, table.__tablename__ + "_extended")

    gdf_to_db = tables.prepare_to_db(gdf_result)
    engine = utils.get_database()
    tables.create_table(engine, table)
    gdf_to_db.to_postgis(
        table.__tablename__, engine, index=True, if_exists="replace", chunksize=4096
    )
    engine.dispose()
    gc.collect()
    logger.info(f"Finish extend catchments, save to table {table.__tablename__}.")


# @utils.timeit  # 0:07:07.559980 for SDCP; 0:11:38.107690 for CATCHTEMP
def find_gaps(
    gdf_in: gpd.GeoDataFrame,
    polygon_threshold: Union[int, float] = LOWER_AREA,
    hole_threshold: Union[int, float] = 10_000 * 10_000,
    table: Type[tables.Ttable] = None,
    gpkg: bool = False,
) -> gpd.GeoDataFrame:
    """
    find gaps between catchments.

    :param gdf_in: input dataframe of catchments
    :param table: table of gap to be saved for debugging, default None
    :param polygon_threshold: remove the polygon if it is smaller than the threshold, default LOWER_AREA
    :param hole_threshold: take holes as gaps if a hole area is smaller than the threshold, default 100 KM2
    :param gpkg: whether to save gap to gpkg, default False
    """
    logger.info(f"Searching gaps between catchments...")
    gdf = gdf_in.copy()
    # original geometry
    geom_o = gdf["geometry"].unary_union
    # filtered geometry
    geom_f = utils.filter_geometry(
        gdf["geometry"],
        resolution=CATCHMENT_RESOLUTION,
        polygon_threshold=polygon_threshold,
        hole_threshold=hole_threshold,
    )
    # difference geometry
    geom_d = geom_f.difference(geom_o)
    if geom_d.is_empty:
        logger.info(f"No gap found between catchments, nothing to do.")
        return gdf_in
    gdf_result = gpd.GeoDataFrame(index=[0], crs="epsg:2193", geometry=[geom_d])
    gdf_result = gdf_result.explode(ignore_index=True)
    gdf_result["area"] = gdf_result["geometry"].area
    gdf_result = gdf_result.sort_values(by="area", ascending=False).reset_index(
        drop=True
    )
    gdf_result["catch_id"] = GAP_OFFSET + gdf_result.index.values
    gdf_result = gdf_result[["catch_id", "area", "geometry"]]

    if table is not None:
        logger.info(f"Save gaps to {table.__tablename__} table.")
        gdf_to_db = tables.prepare_to_db(gdf_result, check_empty=False)
        engine = utils.get_database()
        tables.create_table(engine, table)
        gdf_to_db.to_postgis(
            table.__tablename__, engine, index=True, if_exists="replace", chunksize=1000
        )
        engine.dispose()
        gc.collect()

    if gpkg:
        if table is None:
            table = "gaps"
        utils.save_gpkg(gdf_result, table)

    gdf_result = gdf_result[gdf_result["area"] > EPS]
    gdf_result = gdf_result.sort_values(by="area", ascending=False).reset_index(
        drop=True
    )
    logger.info(f"Found {len(gdf_result)} gaps between catchments.")
    gc.collect()
    return gdf_result


# runtime: 0:00:26.887988.
# @utils.timeit  # 0:01:05.014197 for SDCP;  0:05:56.538906 for CATCHMENT
def merge_catchments(
    gdf_in: gpd.GeoDataFrame,
    lower_area: Union[int, float] = EPS,
    upper_area: Union[int, float] = CATCHMENT_RESOLUTION * CATCHMENT_RESOLUTION * 4,
) -> gpd.GeoDataFrame:
    """
    Merge target gaps between polygons of catchments into the largest adjacent catchments.
    target gaps are those gaps with area between lower_area and upper_area.

    :param gdf_in: input GeoDataFrame of catchments.
    :param lower_area: gap area smaller than lower_area will be ignored.
    :param upper_area: gap area lager than upper_area will be saved as new catchments, default is 10 pixels.
    """
    logger.info(f"Merging small catchments to its largest adjacent catchment...")
    gdf = gdf_in.copy()
    assert gdf[
        "catch_id"
    ].is_unique, f"catch_id is not unique in gdf: {sorted(gdf[gdf['catch_id'].duplicated()]['catch_id'].to_list())}"
    count_merge = 0
    count_skip = 0
    count_standalone = 0
    gdf_keep = gdf[gdf["area"] > upper_area].copy()
    gdf_merge = gdf[gdf["area"] <= upper_area].copy()
    gdf_keep = gdf_keep.reset_index(drop=True)
    gdf_merge = gdf_merge.reset_index(drop=True)

    # abandon `iterrows` approach and use `at` approach for better performance and convenience.
    for i in range(len(gdf_merge)):
        if (
            lower_area < gdf_merge.at[i, "area"] < upper_area
        ):  # to update existing catchments
            # find adjacent catchments
            gdf_adj = gdf_keep[
                gdf_keep["geometry"].intersects(
                    gdf_merge.at[i, "geometry"].buffer(
                        CATCHMENT_RESOLUTION / 2 - EPS, join_style="mitre"
                    )
                )
            ].copy()
            if gdf_adj.empty:
                logger.warning(
                    f"Cannot find adjacent catchments for standalone catchment: "
                    f"{gdf_merge.at[i, 'catch_id']}, add it in catchment table."
                )
                gdf_row = gpd.GeoDataFrame(
                    index=[0], crs="epsg:2193", geometry=[gdf_merge.at[i, "geometry"]]
                )
                gdf_row["area"] = gdf_merge.at[i, "area"]
                gdf_row["catch_id"] = gdf_merge.at[i, "catch_id"]
                gdf_keep = pd.concat([gdf_keep, gdf_row], ignore_index=True)
                count_standalone += 1
                continue
            # merge into the largest adjacent catchment
            gdf_largest = gdf_adj[gdf_adj["area"] == gdf_adj["area"].max()]
            catch_id = gdf_largest["catch_id"].values[0]
            geometry = gdf_largest["geometry"].values[0]
            # area = gdf_largest['area'].values[0]  # not used because it may not correct after merge
            area = (
                gdf_largest["geometry"].values[0].area
            )  # use the calculated area instead
            # skip overlapped catchments
            if geometry.buffer(EPS, join_style="mitre").contains(
                gdf_merge.at[i, "geometry"]
            ):
                logger.debug(
                    f"Skip  catchment catch_id: {gdf_merge.at[i, 'catch_id']:>8}, "
                    f"since it is covered by catchment {catch_id}."
                )
                count_skip += 1
                continue
            index = gdf_keep[gdf_keep["catch_id"] == catch_id].index.values[
                0
            ]  # index in gdf_keep
            logger.debug(
                f"Merge catchment catch_id: {gdf_merge.at[i, 'catch_id']:>8}, "
                f"into catch_id {catch_id:>8}, adjacent catchments {gdf_adj['catch_id'].to_list()}"
            )
            geom_update_t = shapely.unary_union([geometry, gdf_merge.at[i, "geometry"]])
            # to remove internal linestring and tiny gaps,
            # this step may cause gap catchments to be merged are overlapped.
            geom_update = geom_update_t.buffer(EPS, join_style="mitre").buffer(
                -EPS, join_style="mitre"
            )
            assert geom_update.area > area + EPS, (
                f"Area should be larger after merge. "
                f"{area} | {gdf_merge.at[i, 'area']} | {geom_update.area}"
            )
            # update catchment immediately for next iteration
            gdf_keep.at[index, "geometry"] = geom_update
            gdf_keep.at[index, "area"] = geom_update.area
            assert (
                gdf_keep[gdf_keep["catch_id"] == catch_id]["geometry"].values[0]
                == geom_update
            ), f"Did not assign geometry value successfully. index: {index} catch_id: {catch_id}"
            assert (
                gdf_keep[gdf_keep["catch_id"] == catch_id]["area"].values[0]
                == geom_update.area
            ), f"Did not assign area value successfully. index: {index} catch_id: {catch_id}"
            count_merge += 1
        elif gdf_merge.at[i, "area"] <= lower_area:  # to abandon
            logger.debug(
                f"Skip tiny catch_id: {gdf_merge.at[i, 'catch_id']} with area: {gdf_merge.at[i, 'area']}."
            )
            count_skip += 1
        else:  # should not happen
            raise ValueError(f"Unexpected data: {gdf_merge.iloc[i].to_string()}.")

    logger.info(f"Merged {count_merge} gaps into its largest adjacent catchments.")
    if count_skip > 0:
        logger.info(f"Skip {count_skip} gaps.")
    if count_standalone > 0:
        logger.info(
            f"Added {count_standalone} standalone catchments into catchment table."
        )

    assert gdf_keep["catch_id"].is_unique, (
        f"catch_id is not unique in gdf_keep: "
        f"{sorted(gdf_keep[gdf_keep['catch_id'].duplicated()]['catch_id'].to_list())}"
    )

    # process gap catchment catch_id offset if any (when the upper area is small, there might be some gaps left)
    gdf_keep_gap = gdf_keep[gdf_keep["catch_id"] >= GAP_OFFSET].copy()
    if not gdf_keep_gap.empty:
        gdf_exist_gap = gdf_keep[
            (gdf_keep["catch_id"] >= GAP_OFFSET - GAP_REVISE)
            & (gdf_keep["catch_id"] < GAP_OFFSET)
        ].copy()
        if not gdf_exist_gap.empty:
            init_id = gdf_exist_gap["catch_id"].max() + 1
        else:
            init_id = GAP_OFFSET - GAP_REVISE
        gdf_keep_gap = gdf_keep_gap.sort_values(by="area", ascending=False).reset_index(
            drop=True
        )
        gdf_keep_gap["catch_id"] = gdf_keep_gap.index + init_id
        logger.debug(
            f"Processed {len(gdf_keep_gap)} gaps catch_id and save them as new catchments, "
            f"catch_id: {gdf_keep_gap['catch_id'].to_list()}."
        )
        gdf_result = pd.concat(
            [gdf_keep[gdf_keep["catch_id"] < GAP_OFFSET].copy(), gdf_keep_gap],
            ignore_index=True,
        )
        gdf_result = gdf_result.sort_values(by="catch_id", ascending=True).reset_index(
            drop=True
        )
    else:
        gdf_result = gdf_keep.sort_values(by="catch_id", ascending=True).reset_index(
            drop=True
        )
        logger.debug(f"No new catchments form gaps area are found.")
    assert not (
        gdf_result["catch_id"] >= GAP_OFFSET
    ).any(), f"catch_id should be smaller than {GAP_OFFSET}."
    gc.collect()
    return gdf_result


def trim_single_catchment(gds: Union[gpd.GeoSeries, pd.Series]) -> tuple:
    """
    align input geometry with corresponding superior sea draining catchments.
    """
    geom = gds["geometry"]

    # filter out sticks and spikes, only work for catchments with one large stick which is over 4 pixels.
    geom_fil = geom.buffer(
        -(CATCHMENT_RESOLUTION / 2 + EPS), join_style="mitre"
    ).buffer((CATCHMENT_RESOLUTION / 2 + EPS), join_style="mitre")
    geom_fil = utils.filter_geometry(
        geom_fil, resolution=CATCHMENT_RESOLUTION, polygon_threshold=LOWER_AREA
    )
    geom_mask = geom.difference(geom_fil)  # get sticks and spikes geometry
    if isinstance(geom_mask, shapely.geometry.MultiPolygon):
        geom_mask = max(geom_mask.geoms, key=lambda x: x.area)
    if geom_mask.area > CATCHMENT_RESOLUTION * CATCHMENT_RESOLUTION * 8:
        logger.debug(f"Find and cut down a stick, catch_id: {gds['catch_id']}")
        geom = geom.difference(geom_mask)
        geom = utils.filter_geometry(
            geom, resolution=CATCHMENT_RESOLUTION, polygon_threshold=LOWER_AREA
        )
        gdf_result = gpd.GeoDataFrame(index=[0], crs="epsg:2193", geometry=[geom])
        gdf_result["area"] = gdf_result["geometry"].area
        gdf_result["catch_id"] = gds["catch_id"]
        update = 1
    else:
        gdf_result = gpd.GeoDataFrame(
            index=[0], crs="epsg:2193", geometry=[gds["geometry"]]
        )
        gdf_result["area"] = gds["area"]
        gdf_result["catch_id"] = gds["catch_id"]
        update = 0
    return gdf_result, update


def align_single_catchment(index: int, table: Type[tables.Ttable]) -> gpd.GeoDataFrame:
    """
    Note: Deprecated

    align input geometry with corresponding superior sea draining catchments.
    """
    engine = utils.get_database(null_pool=True)
    gdf = tables.get_data_by_id(engine, table, index)
    geom = gdf["geometry"].values[0]

    # align with superior sea draining catchments
    buffer = (
        CATCHMENT_RESOLUTION * 3 + EPS
        if utils.get_min_width(geom) > CATCHMENT_RESOLUTION * 3
        else 10
    )
    gdf_sdc = tables.get_catchment_by_geometry(
        engine, tables.SDCP, geom, buffer=-buffer
    )
    assert len(gdf_sdc) <= 1, (
        f"Find multiple superior sea draining catchments {gdf_sdc['catch_id'].to_list()} "
        f"for {table.__tablename__} {index}."
    )
    no_superior = gdf_sdc.empty
    geom_sdc = gdf_sdc["geometry"].values[0] if not no_superior else None
    no_intersect = geom_sdc.contains(geom) if not no_superior else True
    if no_superior or no_intersect:
        logger.debug(
            f"{table.__tablename__}, {index:>8}, no superior catchment: {no_superior}, or "
            f"no intersect with superior catchment: {no_intersect}, nothing to do."
        )
        gdf_result = gpd.GeoDataFrame(index=[0], crs="epsg:2193", geometry=[geom])
        gdf_result["area"] = gdf_result["geometry"].area
        gdf_result["catch_id"] = index
        return gdf_result
    logger.debug(
        f"{table.__tablename__}, {index:>8}, "
        f"align with superior sea draining catchments {gdf_sdc['catch_id'].to_list()}."
    )
    geom_dif = geom_sdc.difference(geom)
    if isinstance(geom_dif, shapely.geometry.MultiPolygon):
        geom_dif = max(
            geom_dif.geoms, key=lambda x: x.area
        )  # select the biggest polygon
    geom_clip = geom_sdc.intersection(geom.buffer(20, join_style="mitre"))
    geom_align = geom_clip.difference(geom_dif)
    geom_align = utils.filter_geometry(geom_align)
    gdf_result = gpd.GeoDataFrame(index=[0], crs="epsg:2193", geometry=[geom_align])
    gdf_result["area"] = gdf_result["geometry"].area
    gdf_result["catch_id"] = index
    engine.dispose()
    gc.collect()
    return gdf_result


# @utils.timeit
def trim_catchments(gdf: gpd.GeoDataFrame, parallel: bool = True) -> tuple:
    """
    trim catchments in a GeoDataFrame.
    """
    gdf_concat = gpd.GeoDataFrame(geometry=gpd.GeoSeries())
    update_sum = 0
    if parallel:
        list_gds = [gds for _, gds in gdf.iterrows()]
        with Pool(processes=multiprocessing.cpu_count()) as pool:
            list_result = pool.map(trim_single_catchment, list_gds)
            pool.close()
            pool.join()
        for gdf_trim, update in list_result:
            gdf_concat = pd.concat([gdf_concat, gdf_trim], ignore_index=True)
            update_sum += update
    else:
        for _, gds in gdf.iterrows():
            gdf_trim, update = trim_single_catchment(gds)
            gdf_concat = pd.concat([gdf_concat, gdf_trim], ignore_index=True)
            update_sum += update

    return gdf_concat, update_sum


def align_catchments(
    list_id: list, table: Type[tables.Ttable], parallel: bool = True, gpkg: bool = False
) -> gpd.GeoDataFrame:
    """
    align catchments of catchment index list in a table.
    """
    if parallel:
        with Pool(processes=multiprocessing.cpu_count()) as pool:
            gdf_result = pool.starmap(
                align_single_catchment, zip(list_id, repeat(table))
            )
            pool.close()
            pool.join()
        gdf_concat = pd.concat(gdf_result, ignore_index=True)
    else:
        gdf_concat = gpd.GeoDataFrame(geometry=gpd.GeoSeries())
        for catch_id in list_id:
            gdf_result = align_single_catchment(catch_id, table)
            gdf_concat = pd.concat([gdf_concat, gdf_result], ignore_index=True)
    return gdf_concat


# @utils.timeit  # 0:08:34.051835 for SDCP; 0:18:32.292305 for CATCHMENT
def refine_catchments(
    table: Type[tables.Ttable],
    trim: bool = False,
    parallel: bool = True,
    gpkg: bool = False,
) -> None:
    """
    refine sea draining catchments data, remove holes, silvers, and spikes between polygons, and save to local database.
    """
    assert (
        table.__tablename__ in GAP_TABLE_MAPPING.keys()
    ), f"Only support table {list(GAP_TABLE_MAPPING.keys())}."

    logger.info(f"Refining table {table.__tablename__} catchments data...")
    engine = utils.get_database()
    gdf = tables.read_postgis_table(
        engine, table.__tablename__, sort_by="area", desc=True
    )
    merge_threshold = tables.get_min_value(engine, tables.SDC, "area")

    if trim:
        logger.info(f"Trimming {table.__tablename__} catchments ...")
        gdf, update_sum = trim_catchments(gdf, parallel=parallel)
        logger.info(
            f"Finish trimming {table.__tablename__} catchments, trimmed {update_sum} catchments."
        )

        if gpkg:
            utils.save_gpkg(gdf, table.__tablename__ + "_trimmed")

    # find gaps between catchments and merge them into its largest adjacent catchments
    gdf_gaps = find_gaps(gdf, table=GAP_TABLE_MAPPING[table.__tablename__], gpkg=gpkg)
    gdf = pd.concat([gdf, gdf_gaps], ignore_index=True)
    gdf = gdf.sort_values(by="area", ascending=False).reset_index(drop=True)
    gdf = merge_catchments(gdf, upper_area=merge_threshold)

    if gpkg:
        utils.save_gpkg(gdf, table.__tablename__ + "_refined")

    gdf_to_db = tables.prepare_to_db(gdf)
    tables.create_table(engine, table)
    gdf_to_db.to_postgis(
        table.__tablename__, engine, index=True, if_exists="replace", chunksize=4096
    )
    engine.dispose()
    gc.collect()
    logger.info(
        f"Finish refine catchments, get total {len(gdf_to_db)} catchments and "
        f"save to table {table.__tablename__}."
    )


# @utils.timeit  # 0:01:51.943599 for Order5P and Order4P, trimming only.
def refine_sub_catchments(
    trim: bool = True, align: bool = False, parallel: bool = True, gpkg: bool = False
) -> None:
    """
    refine subordinated catchments data, remove holes, silvers, and spikes between sea draining catchments,
    and save to local database.
    """
    logger.info(f"Refining subordinated catchments data...")
    list_table = [tables.ORDER5P, tables.ORDER4P]
    for table in list_table:
        logger.info(f"Refining {table.__tablename__} catchments...")
        engine = utils.get_database()
        gdf_sub = tables.read_postgis_table(engine, table.__tablename__)
        engine.dispose()
        gc.collect()

        if trim:
            logger.info(f"Trimming {table.__tablename__} catchments ...")
            gdf_sub, update_sum = trim_catchments(gdf_sub, parallel=parallel)
            logger.info(
                f"Finish trimming {table.__tablename__} catchments, trimmed {update_sum} catchments."
            )

            if gpkg:
                utils.save_gpkg(gdf_sub, table.__tablename__ + "_trimmed")

        if align:
            logger.info(f"Aligning {table.__tablename__} catchments ...")
            list_id = gdf_sub["catch_id"].tolist()
            gdf_sub = align_catchments(list_id, table, parallel=parallel, gpkg=gpkg)

            if gpkg:
                utils.save_gpkg(gdf_sub, table.__tablename__ + "_aligned")

        gdf_to_db = tables.prepare_to_db(gdf_sub)
        engine = utils.get_database()
        tables.create_table(engine, table)
        gdf_to_db.to_postgis(
            table.__tablename__, engine, index=True, if_exists="replace", chunksize=4096
        )
        engine.dispose()
        gc.collect()
        logger.info(
            f"Finish refining {table.__tablename__} catchments, get {len(gdf_to_db)} processed catchments."
        )
    logger.info(f"Finish refining all subordinated catchments data.")


# @utils.timeit  # 0:10:15.315367
def gen_coast_catchments(
    coast_distance: Union[int, float] = COAST_DISTANCE,
    lower_area: int = LOWER_AREA * 4,
    gpkg: bool = False,
) -> None:
    """
    generate cost catchments from sea draining catchments.

    :param coast_distance: distance from coast, defined by COAST_DISTANCE
    :param lower_area: lower limit area to filter out, defined by LOWER_AREA
    :param gpkg: save to gpkg or not
    :return: coast catchments
    """
    logger.info("Generating coast catchments table...")
    engine = utils.get_database()
    # get catchments geometry
    gdf_catchments = tables.read_postgis_table(engine, tables.CATCHMENT.__tablename__)
    geom_catchments = utils.filter_geometry(
        gdf_catchments["geometry"].copy(),
        resolution=CATCHMENT_RESOLUTION,
        polygon_threshold=lower_area,
    )
    data_dir = pathlib.Path(utils.get_env_variable("DATA_DIR"))
    land_path = pathlib.Path(utils.get_env_variable("LAND_FILE"))
    gdf_land = gpd.read_file(data_dir / land_path)
    gdf_land = gdf_land.to_crs(epsg=2193)
    geom_land_ex = gdf_land["geometry"].buffer(coast_distance).unary_union
    geom_coast = geom_land_ex.difference(geom_catchments)
    geom_coast = utils.filter_geometry(
        geom_coast,
        resolution=CATCHMENT_RESOLUTION,
        polygon_threshold=lower_area,
        hole_threshold=100 * 100,
    )
    # cut into grid catchments
    list_fishnet = utils.fishnet(geom_coast, threshold=COAST_GRID)
    gdf_grid = gpd.GeoDataFrame(
        index=range(COAST_OFFSET, COAST_OFFSET + len(list_fishnet)),
        crs="epsg:2193",
        geometry=list_fishnet,
    )
    gdf_grid = gdf_grid.reset_index().rename(columns={"index": "catch_id"})
    gdf_grid["area"] = gdf_grid["geometry"].area
    gdf_grid = gdf_grid[["catch_id", "area", "geometry"]]

    if gpkg:
        utils.save_gpkg(gdf_grid, tables.COAST)

    gdf_to_db = tables.prepare_to_db(gdf_grid)
    tables.create_table(engine, tables.COAST)
    gdf_to_db.to_postgis(
        tables.COAST.__tablename__, engine, index=True, if_exists="replace"
    )

    gdf = pd.concat([gdf_catchments, gdf_grid], ignore_index=True)
    gdf = gdf.sort_values(by=["catch_id"]).reset_index(drop=True)

    # always save to gpkg for checking
    utils.save_gpkg(gdf, tables.CATCHMENT)

    gdf_to_db = tables.prepare_to_db(gdf)
    tables.create_table(engine, tables.CATCHMENT)
    gdf_to_db.to_postgis(
        tables.CATCHMENT.__tablename__,
        engine,
        index=True,
        if_exists="replace",
        chunksize=4096,
    )
    engine.dispose()
    gc.collect()
    logger.info(
        f"Finish generating coast catchments table, "
        f"add {len(gdf_grid)} coast catchments in coast_catchment table, "
        f"catch_id range:\n{gdf_grid['catch_id'].min()} - {gdf_grid['catch_id'].max()}"
    )
    logger.info(
        f"Finish generating all catchments table, total {len(gdf)} catchments in catchment table, "
        f"catch_id range:\n{gdf['catch_id'].min()} - {gdf['catch_id'].max()}"
    )


def find_subordinate(
    engine: Engine,
    table: Union[Type[tables.Ttable], str],
    geometry: shapely.Geometry,
    buffer: Union[int, float] = CATCHMENT_RESOLUTION,
) -> gpd.GeoDataFrame:
    """
    Search for subordinate catchments in a table

    :param engine: database engine
    :param table: table name
    :param geometry: geometry to search
    :param buffer: buffer distance for search geometry to ensure the subordinate catchments are included
    :return: subordinate catchments
    """
    geometry = shapely.buffer(geometry, buffer, join_style="mitre")
    query = f"""SELECT * FROM {table} WHERE
                ST_Within(geometry, ST_SetSRID('{geometry}'::geometry, 2193)) ;"""
    gdf = gpd.read_postgis(query, engine, geom_col="geometry", crs="epsg:2193")
    if gdf.empty:
        logger.warning(f"No subordinate catchments found in Table {table}")
    return gdf


def split_catchment(gds: Union[pd.Series, gpd.GeoSeries]) -> tuple:
    """
    Split catchment into smaller catchments

    :param gds: catchment geometry to split
    :return: split catchments dataframe and the split mapping information
    """
    logger.info(f"Splitting {gds['catch_id']:>8}, starting...")
    buffer = CATCHMENT_RESOLUTION * 20  # adjustable if needed
    engine = utils.get_database(null_pool=True)
    gdf_order5 = find_subordinate(
        engine, tables.ORDER5P.__tablename__, gds["geometry"], buffer
    )
    gdf_order5 = (
        gdf_order5[gdf_order5["area"] < UPPER_AREA]
        if not gdf_order5.empty
        else gdf_order5
    )
    # return to collect the split catchments together
    gdf_concat = gpd.GeoDataFrame(geometry=gpd.GeoSeries())
    # recorde steps
    step = 1

    if gdf_order5.empty:
        logger.debug(
            f"Splitting {gds['catch_id']:>8}, step {step}: order5 fail,\torder 4 trying..."
        )
        geom_dif = utils.filter_geometry(
            gds["geometry"],
            resolution=CATCHMENT_RESOLUTION,
            polygon_threshold=LOWER_AREA,
        )
        rest_area = gds["area"]
    else:
        logger.debug(
            f"Splitting {gds['catch_id']:>8}, "
            f"step {step}: order5 success:\t{gdf_order5['catch_id'].to_list()}"
        )
        gdf_concat = pd.concat([gdf_concat, gdf_order5], ignore_index=True)
        geom = utils.filter_geometry(
            gdf_order5["geometry"].copy(),
            resolution=CATCHMENT_RESOLUTION,
            polygon_threshold=LOWER_AREA,
        )
        geom_dif = gds["geometry"].difference(geom)
        geom_dif = utils.filter_geometry(
            geom_dif, resolution=CATCHMENT_RESOLUTION, polygon_threshold=LOWER_AREA
        )
        rest_area = geom_dif.area

    step += 1
    if rest_area > UPPER_AREA:
        gdf_order4 = find_subordinate(
            engine, tables.ORDER4P.__tablename__, geom_dif, buffer
        )
        gdf_order4 = (
            gdf_order4[gdf_order4["area"] < UPPER_AREA]
            if not gdf_order4.empty
            else gdf_order4
        )
        if gdf_order4.empty:
            logger.warning(
                f"Splitting {gds['catch_id']:>8}, "
                f"step {step}: downgrade fail,\tstraight line cutting..."
            )
        else:
            logger.debug(
                f"Splitting {gds['catch_id']:>8}, "
                f"step {step}: order4 success:\t{gdf_order4['catch_id'].to_list()}"
            )
            gdf_concat = pd.concat([gdf_concat, gdf_order4], ignore_index=True)
            geom = utils.filter_geometry(
                gdf_order4["geometry"].copy(),
                resolution=CATCHMENT_RESOLUTION,
                polygon_threshold=LOWER_AREA,
            )
            geom_dif = geom_dif.difference(geom)
            geom_dif = utils.filter_geometry(
                geom_dif, resolution=CATCHMENT_RESOLUTION, polygon_threshold=LOWER_AREA
            )
            rest_area = geom_dif.area

        step += 1
        if rest_area > UPPER_AREA:
            list_katana = []
            for geom in geom_dif.geoms:
                list_result = utils.katana(geom, threshold=UPPER_AREA)
                list_katana.extend(list_result)
            gdf_rest = gpd.GeoDataFrame(
                index=range(len(list_katana)), crs="epsg:2193", geometry=list_katana
            )
            gdf_rest["area"] = gdf_rest["geometry"].area
            gdf_rest = gdf_rest[
                gdf_rest["area"] > LOWER_AREA * 20
            ]  # ignore tiny polygons
            gdf_rest = gdf_rest.sort_values(by="area", ascending=False).reset_index(
                drop=True
            )
            gdf_rest["catch_id"] = (
                gds["catch_id"] * SPLIT_SHIFT + SPLIT_OFFSET + gdf_rest.index.values
            )
            gdf_rest = gdf_rest[["catch_id", "area", "geometry"]]
            logger.debug(
                f"Splitting {gds['catch_id']:>8}, "
                f"step {step}: residue done:\t{gdf_rest['catch_id'].to_list()}"
            )
            gdf_concat = pd.concat([gdf_concat, gdf_rest], ignore_index=True)
            gdf_split = gdf_concat.copy()
            gdf_split["super_id"] = gds["catch_id"]
            engine.dispose()
            gc.collect()
            return gdf_concat, gdf_split

    gdf_rest = gpd.GeoDataFrame(index=[0], crs="epsg:2193", geometry=[geom_dif])
    gdf_rest = gdf_rest.explode(ignore_index=True)
    gdf_rest["area"] = gdf_rest["geometry"].area
    gdf_rest = gdf_rest[gdf_rest["area"] > LOWER_AREA * 20]  # ignore tiny polygons
    gdf_rest = gdf_rest.sort_values(by="area", ascending=False).reset_index(drop=True)
    gdf_rest["catch_id"] = (
        gds["catch_id"] * SPLIT_SHIFT + SPLIT_OFFSET + gdf_rest.index.values
    )
    gdf_rest = gdf_rest[["catch_id", "area", "geometry"]]
    gdf_concat = pd.concat([gdf_concat, gdf_rest], ignore_index=True)
    logger.debug(
        f"Splitting {gds['catch_id']:>8}, step {step}: residue done:\t{gdf_rest['catch_id'].to_list()}"
    )
    gdf_split = gdf_concat.copy()
    gdf_split["super_id"] = gds["catch_id"]
    engine.dispose()
    gc.collect()
    return gdf_concat, gdf_split


# @utils.timeit  # 0:04:32.662574
def gen_catchment_table(parallel: bool = True, gpkg: bool = False) -> None:
    """
    Generate catchment table from SDC table and its subordinate tables.

    :param parallel: whether to use parallel computing
    :param gpkg: whether to save the result to GPKG
    :return: None
    """
    logger.info("Generating catchment table content...")
    engine = utils.get_database()
    # area up limit that can be processed by server for the limit of memory
    # fetch smaller catchments to be saved to catchment table
    gdf_to_db = tables.get_catchment_by_area_range(
        engine, tables.SDCP, upper_area=UPPER_AREA, lower_area=None
    )
    # records split catchments information for debugging
    gdf_split = gpd.GeoDataFrame(geometry=gpd.GeoSeries())
    # fetch larger catchments to be split to subordinate catchments
    gdf_to_split = tables.get_catchment_by_area_range(
        engine, tables.SDCP, upper_area=None, lower_area=UPPER_AREA
    )
    assert gdf_to_db.empty is False, f"{tables.SDCP.__tablename__} table is empty!"
    assert gdf_to_split.empty is False, f"{tables.SDCP.__tablename__} table is empty!"
    engine.dispose()
    gc.collect()

    logger.info(
        f"Splitting catchments with area > {UPPER_AREA}, total {len(gdf_to_split)}"
    )
    logger.debug(
        f"Splitting catchments catch_id:\n{gdf_to_split['catch_id'].to_list()}"
    )
    if parallel:
        list_gds = [gds for _, gds in gdf_to_split.iterrows()]
        with Pool(processes=multiprocessing.cpu_count()) as pool:
            list_result = pool.map(split_catchment, list_gds)
        for gdf_to_db_row, gdf_split_row in list_result:
            gdf_to_db = pd.concat([gdf_to_db, gdf_to_db_row], ignore_index=True)
            gdf_split = pd.concat([gdf_split, gdf_split_row], ignore_index=True)
    else:
        for _, gds_row in gdf_to_split.iterrows():
            gdf_to_db_row, gdf_split_row = split_catchment(gds_row)
            gdf_to_db = pd.concat([gdf_to_db, gdf_to_db_row], ignore_index=True)
            gdf_split = pd.concat([gdf_split, gdf_split_row], ignore_index=True)

    if gpkg:  # for debug
        utils.save_gpkg(gdf_to_db, tables.CATCHTEMP.__tablename__ + "_split")
        utils.save_gpkg(gdf_split, tables.SDCS)

    engine = utils.get_database()
    gdf_to_db = tables.prepare_to_db(gdf_to_db)
    tables.create_table(engine, tables.CATCHTEMP)
    gdf_to_db.to_postgis(
        tables.CATCHTEMP.__tablename__,
        engine,
        index=True,
        if_exists="replace",
        chunksize=4096,
    )
    logger.info(f"Save split catchments to {tables.CATCHTEMP.__tablename__} table.")
    gdf_split = tables.prepare_to_db(gdf_split, check_empty=False)
    tables.create_table(engine, tables.SDCS)
    gdf_split.to_postgis(
        tables.SDCS.__tablename__, engine, index=True, if_exists="replace"
    )
    logger.info(
        f"Save split catchments superior-subordinate mapping to {tables.SDCS.__tablename__} table."
    )
    engine.dispose()
    gc.collect()


def gen_grid_table(
    coast_distance: Union[int, float] = COAST_DISTANCE, gpkg: bool = False
) -> None:
    """
    Generate grid table from land geometry.

    :param coast_distance: distance from coast, defined by COAST_DISTANCE
    :param gpkg: whether to save the result to GPKG
    :return: None
    """
    logger.info("Generating grid table content...")
    engine = utils.get_database()

    data_dir = pathlib.Path(utils.get_env_variable("DATA_DIR"))
    land_path = pathlib.Path(utils.get_env_variable("LAND_FILE"))
    gdf_land = gpd.read_file(data_dir / land_path)
    gdf_land = gdf_land.to_crs(epsg=2193)
    geom_land_ex = gdf_land["geometry"].buffer(coast_distance).unary_union
    # cut into grid
    list_fishnet = utils.fishnet(geom_land_ex, threshold=GRID_SIZE, lrbu=True)
    gdf_grid = gpd.GeoDataFrame(
        index=range(len(list_fishnet)),
        crs="epsg:2193",
        geometry=list_fishnet,
    )
    gdf_grid = gdf_grid.reset_index().rename(columns={"index": "catch_id"})
    gdf_grid["area"] = gdf_grid["geometry"].area
    gdf_grid = gdf_grid[["catch_id", "area", "geometry"]]
    if gpkg:
        utils.save_gpkg(gdf_grid, tables.GRID)
    gdf_to_db = tables.prepare_to_db(gdf_grid)
    gdf_to_db.index.rename("grid_id", inplace=True)
    tables.create_table(engine, tables.GRID)
    gdf_to_db.to_postgis(
        tables.GRID.__tablename__, engine, index=True, if_exists="replace"
    )
    logger.info(f"Save grid partition to {tables.GRID.__tablename__} table.")
    engine.dispose()
    gc.collect()


def drop_temp_table(table: Union[list, str, Type[tables.Ttable]]) -> None:
    """
    Drop temp table from database.

    :param table: table to drop
    :return: None
    """
    if isinstance(table, str):
        table = [table]
    if not isinstance(table, list):
        table = [table.__tablename__]
    engine = utils.get_database()
    for t in table:
        if tables.is_table_exist(engine, t):
            tables.delete_table(engine, t, keep_schema=False)
    engine.dispose()
    gc.collect()


@utils.timeit  # 2:12:52.391500
def run(grid_only: bool = False, drop_tmp: bool = True, gpkg: bool = False) -> None:
    """
    fetch sea draining catchment from data.mfe.govt.nz.
    process the data to generate catchment table by following steps:
    1. deduplicate sea draining catchment
    2. extend sea draining catchment table to eliminate gaps among catchments
    3. deduplicate processed sea draining catchment table again to eliminate overlaps among catchments
    4. refine sea draining catchment table to eliminate gaps among catchments
    10. generate coastal catchments and save to catchment table
    11. delete temp tables if needed
    the catchments in catchment table are aligned and gap free, ready to be used for further processing.

    :param grid_only: whether to generate grid table only
    :param drop_tmp: whether to drop temp tables
    :param gpkg: whether to save the result to GPKG
    """
    if not grid_only:
        initiate_tables(gpkg=gpkg)
        deduplicate_table(tables.SDC, tables.SDCP)
        extend_catchments(tables.SDCP, gpkg=gpkg)
        deduplicate_table(tables.SDCP, tables.CATCHMENT, buffer=-EPS, gpkg=gpkg)
        refine_catchments(tables.CATCHMENT, gpkg=gpkg)
        deduplicate_table(tables.CATCHMENT, tables.CATCHMENT, buffer=-EPS, gpkg=gpkg)
        gen_coast_catchments(gpkg=gpkg)
    if drop_tmp:
        drop_temp_table(
            [tables.SDC, tables.SDCP, tables.SDCS, tables.CATCHMENTG, tables.COAST]
        )
    gen_grid_table(gpkg=True)
    logger.info(f"\n-------------- Catchment Process Finished! ----------------")


if __name__ == "__main__":
    run(gpkg=True)
