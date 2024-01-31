import os
import gc
import math
import numpy as np
import pandas as pd
import dask.dataframe as dd
import geopandas as gpd
from shapely import box, unary_union, wkt
from shapely.ops import nearest_points, polygonize
from pathlib import Path
import osmnx as ox
import osmnx.settings
import osmnx.features
import leafmap
from sqlalchemy.engine import Engine

import multiprocessing
from itertools import repeat
from multiprocessing.pool import ThreadPool as Pool

from newzealidar import tables, utils

import logging

# for debug efficiency, if not debug, comment out the following ox settings
# set cache folder
ox.settings.cache_folder = os.path.expanduser("~/.cache/osmnx")
# Enable or disable the cache
ox.settings.use_cache = True  # osmnx default is True
# Enable print console log
ox.settings.log_console = True

logger = logging.getLogger(__name__)

MAX_ENPOINT_SEA_DIST = 1_100
MAX_DIST_SEA = 10_000
CATCHMENT_RESOLUTION = 30
RIVER_NETWORK_FILE = "river_network.geojson"


def plt_gdf(*gdfs):
    gdf_plt = []
    area = []
    for i, gdf in enumerate(gdfs):
        if gdf is None or gdf.empty:
            continue
        gdf_plt.append(gdf.copy().to_crs(4326))
        gdf.to_crs(2193, inplace=True)
        area.append(gdf.geometry.envelope.area.values[0])
    if not len(gdf_plt):
        logger.debug("No data to plot")
        return

    area_sorted = sorted(area, reverse=True)
    if area_sorted[0] > 2e11 and len(area) > 1:  # mainland
        area_max = area_sorted[1]
    else:
        area_max = area_sorted[0]
    index = area.index(area_max)

    coords = gdf_plt[index].total_bounds
    centre = (np.average(coords[0::2]), np.average(coords[1::2]))

    bounds = gdf_plt[index].bounds.iloc[0].to_list()
    bounds = [[bounds[1], bounds[0]], [bounds[3], bounds[2]]]

    # m = leafmap.Map(width=800, height=600)
    m = leafmap.Map()
    colors = ["blue", "green", "yellow", "orange", "red", "black"]
    for i, gdf in enumerate(gdf_plt):
        # convert first polygon to line to avoid too much blue fill
        if i == 0 and gdf.geometry.unary_union.geom_type in {"Polygon", "MultiPolygon"}:
            gdf = gpd.GeoDataFrame(index=[0], geometry=[gdf.geometry.unary_union.boundary], crs=4326)
        m.add_gdf(gdf, layer_name=f"layer_{i}", fill_colors=colors)
    m.set_center(centre[0], centre[1])
    m.fit_bounds(bounds)

    return m


# download data first - https://data.linz.govt.nz/layer/51153-nz-coastlines-and-islands-polygons-topo-150k/
def prep_coast_linz(file_path, buffer=200, save=False):
    save_path = Path(file_path).with_suffix(".gpkg")
    if not Path(file_path).is_file() and not Path(save_path).is_file():
        logger.error(f"File not exist {file_path}")
        return None

    if Path(save_path).is_file():
        logger.info(f"Load existing coastline {save_path}")
        return gpd.read_file(save_path, driver="GPKG")

    gdf_coast = gpd.read_file(file_path, driver="GeoJSON")
    gdf_coast = gdf_coast.intersection(box(165, -48, 180, -34))  # mainland
    # gdf_coast = gpd.GeoDataFrame(index=[0], geometry=[gdf_coast.unary_union.boundary], crs=4326).to_crs(2193)
    gdf_coast = gpd.GeoDataFrame(index=[0], geometry=[gdf_coast.unary_union], crs=4326).to_crs(2193)

    if buffer:
        geometry = gdf_coast.geometry.values[0]
        geometry = (
            geometry.buffer(buffer, join_style="mitre")
            .buffer(-buffer * 2, join_style="mitre")
            .buffer(buffer, join_style="mitre")
        )
        gdf_coast.assign(geometry=geometry)

    if save:
        gdf_coast.to_file(save_path.as_posix(), driver="GPKG")

    return gdf_coast


# download data first - https://osmdata.openstreetmap.de/data/coastlines.html
def prep_coast_osm(file_path, save=False):
    save_path = Path(file_path).with_suffix(".gpkg")
    if not Path(file_path).is_file() and not Path(save_path).is_file():
        logger.error(f"File not exist {file_path}")
        return None

    if Path(save_path).is_file():
        logger.info(f"Load existing coastline {save_path}")
        return gpd.read_file(save_path, driver="GPKG")

    gdf_coast = gpd.read_file(file_path, driver="ESRI Shapefile")
    gdf_coast = gdf_coast[gdf_coast.within(box(165, -48, 180, -34))]  # mainland
    geometry = gdf_coast.geometry.unary_union
    geometry = polygonize(geometry)
    geometry = unary_union(list(geometry))
    gdf_coast = gpd.GeoDataFrame(index=[0], geometry=[geometry], crs=4326).to_crs(2193)

    if save:
        gdf_coast.to_file(save_path.as_posix(), driver="GPKG")

    return gdf_coast


def prep_rec(file_path, save=False):
    save_path = Path(file_path).with_suffix(".csv")
    if not Path(file_path).is_file() and not Path(save_path).is_file():
        logger.error(f"File not exist {file_path}")
        return None

    if Path(save_path).is_file():
        logger.info(f"Load existing coastline {save_path}")
        dd_rec = dd.read_csv(save_path)
        df_rec = dd_rec.compute()
        df_rec["geometry"] = df_rec["geometry"].apply(wkt.loads)
        return gpd.GeoDataFrame(df_rec, crs="epsg:2193", geometry="geometry")

    gdf_rec = gpd.read_file(file_path)
    logger.debug(f"Original REC1 shape: {gdf_rec.shape}")
    gdf_rec.to_crs(2193, inplace=True)
    # reduce the size of the dataframe
    gdf_rec = gdf_rec[["NZREACH", "DISTSEA", "LENGTH", "CATCHAREA", "geometry"]]
    gdf_rec = gdf_rec[gdf_rec["DISTSEA"] < MAX_DIST_SEA]
    logger.debug(f"Converted REC1 shape: {gdf_rec.shape}")

    if save:
        logger.info(f"Save REC1 to {save_path}")
        with open(save_path, "w") as f:
            pd.DataFrame(gdf_rec.assign(geometry=gdf_rec["geometry"].apply(lambda p: p.wkt))).to_csv(f, index=False)

    return gdf_rec


def get_osmid(catch_id, gdf_sdc, gdf_coast, by="dist"):
    assert gdf_sdc.crs.to_epsg() == 2193, f"{catch_id} Input data CRS is not 2193 but {gdf_sdc.crs.to_epsg()}"
    assert gdf_coast.crs.to_epsg() == 2193, f"{catch_id} Input data CRS is not 2193 but {gdf_coast.crs.to_epsg()}"

    if not catch_id:
        return None, None, None, None, None

    endpoint = None
    gdf_catch = gdf_sdc[gdf_sdc.catch_id == catch_id].copy()
    assert not gdf_catch.empty, f"{catch_id} not exist in catchment dataframe "
    # clip the catchment to keep it inside the coastline
    gdf_catch = gdf_catch.clip(gdf_coast.unary_union)
    gdf_catch_4326 = gdf_catch.to_crs(4326)
    if not gdf_catch_4326.geometry.is_valid.all():
        gdf_catch_4326.geometry = gdf_catch_4326.geometry.buffer(0)
        logger.debug(f"Fixed invalid geometry {catch_id}")
    try:
        gdf_waterway = ox.features.features_from_polygon(gdf_catch_4326.unary_union, tags={"waterway": True})
    except Exception as e:
        logger.debug(f"catch_id {catch_id}\n{e}")
        return None, None, None, None, None
    if gdf_waterway.empty:
        logger.debug(f"{catch_id} No waterway in the catchment.")
        return None, None, None, gdf_catch, gdf_waterway
    gdf_waterway.to_crs(2193, inplace=True)
    _len0 = len(gdf_waterway)
    gdf_waterway = gdf_waterway[pd.notnull(gdf_waterway["waterway"])].copy()
    _len1 = len(gdf_waterway)
    logger.debug(f"Found {_len0 - _len1} null waterway") if _len0 > _len1 else None
    gdf_waterway = gdf_waterway.reset_index()
    gdf_waterway = gdf_waterway[gdf_waterway.element_type == "way"].copy()
    gdf_river = gdf_waterway[gdf_waterway.waterway == "river"].copy()
    if gdf_river.empty:
        logger.debug(f"{catch_id} No rivers in the catchment.")
        return None, None, None, gdf_catch, gdf_waterway
    # clip the river to keep it inside the coastline to calculate length and distance to the coast
    # buffer it to make the catchment edge smoother to avoid the unappropriated endpoint
    gdf_waterway_clipped = gdf_waterway.clip(gdf_catch.buffer(CATCHMENT_RESOLUTION / 2).unary_union)
    gdf_waterway["len"] = gdf_waterway_clipped.geometry.length
    gdf_waterway["dist"] = gdf_waterway_clipped.geometry.apply(lambda x: x.distance(gdf_coast.geometry[0].boundary))
    if by == "len":
        gdf_sorted = gdf_waterway.sort_values(by="len", ascending=False).reset_index(drop=True)
    elif by == "dist":
        # gdf_sorted = gdf_waterway.sort_values(by="dist", ascending=True).reset_index(drop=True)
        # distance get higher priority than length
        gdf_sorted = gdf_waterway.sort_values(by=["dist", "len"], ascending=[True, False]).reset_index(drop=True)
        # full geometry of the river section
        # river_geom = gdf_sorted[gdf_sorted.index == 0].geometry.values[0]
        # the part geometry of the river section inside the catchment
        river_osm_id = gdf_sorted[gdf_sorted.index == 0].osmid.values[0]
        river_geom_clipped = gdf_waterway_clipped[gdf_waterway_clipped.osmid == river_osm_id].geometry.values[0]
        coast_geom = gdf_coast.geometry.values[0].boundary
        catch_geom = gdf_catch.geometry.values[0]
        dist_catch_coast = catch_geom.distance(coast_geom)
        # print(dist_catch_coast)
        endpoint = nearest_points(river_geom_clipped, coast_geom)[0]
        dist_endpoint_coast = endpoint.distance(coast_geom)
        # print(dist_endpoint_coast)
        if abs(dist_endpoint_coast - dist_catch_coast) > MAX_ENPOINT_SEA_DIST:
            logger.debug(
                f"{catch_id} The equivalent distance between endpoint ({endpoint.x:.0f} {endpoint.y:.0f}) to coast is "
                f"{abs(dist_endpoint_coast - dist_catch_coast):.0f} meter which is "
                f"larger than {MAX_ENPOINT_SEA_DIST} limitation."
            )
            return None, None, None, gdf_catch, gdf_waterway
    else:
        raise ValueError("Not support mode")
    name = None
    if "name" in gdf_sorted.columns:
        name = gdf_sorted.name.values[0]
        assert isinstance(name, str) or math.isnan(name), name
        if isinstance(name, str):
            gdf_sorted = gdf_sorted[gdf_sorted.name == name].copy()
    osm_id = gdf_sorted.osmid.to_list()
    logger.debug(f"{catch_id} Retrieve river {name} OSM ID: {osm_id}")
    # gdf_sorted = gdf_sorted[["osmid", "name", "waterway", "nodes", "len", "dist", "geometry"]]
    gdf_sorted = gdf_sorted[["osmid", "waterway", "nodes", "geometry"]]

    return osm_id[0], endpoint, gdf_sorted, gdf_catch, gdf_waterway


def get_recid(catch_id, gdf_sdc, gdf_rec, geom_point=None, by="dist"):
    assert gdf_sdc.crs.to_epsg() == 2193, f"Input data CRS is not 2193 but {gdf_sdc.crs.to_epsg()}"
    assert gdf_rec.crs.to_epsg() == 2193, f"Input data CRS is not 2193 but {gdf_rec.crs.to_epsg()}"

    if catch_id is None or gdf_sdc is None or gdf_sdc.empty or gdf_rec is None or gdf_rec.empty:
        logger.info(f"{catch_id} At least one of the compulsory input is None or empty, please check.")
        return None, None, None, None

    gdf_catch = gdf_sdc[gdf_sdc.catch_id == catch_id].copy()
    assert not gdf_catch.empty, f"{catch_id} Catch_id not exist in catchment dataframe."
    if not gdf_catch.geometry.values[0].is_valid:
        gdf_catch = gdf_catch.buffer(0)
        logger.debug(f"{catch_id} Fixed invalid geometry.")
    gdf_rec_catch = gdf_rec[gdf_rec.intersects(gdf_catch.unary_union)].copy()
    if gdf_rec_catch.empty:
        logger.debug(f"{catch_id} No REC1 in the catchment")
        return None, None, gdf_catch, gdf_rec_catch
    if by == "area":
        gdf_sorted = gdf_rec_catch.sort_values(by="CATCHAREA", ascending=False).reset_index(drop=True)
    elif by == "dist":
        if geom_point:
            # print(geom_point)
            gdf_rec_catch["dist"] = gdf_rec_catch.geometry.apply(lambda x: x.distance(geom_point))
            gdf_sorted = gdf_rec_catch.sort_values(by="dist", ascending=True).reset_index(drop=True)
            # 4 closest NZREACH to osm river endpoint
            gdf_sorted = gdf_sorted[gdf_sorted.index < 5].copy()
            gdf_sorted = gdf_sorted.sort_values(by="DISTSEA", ascending=True).reset_index(drop=True)
            # 2 closest NZREACH to the coast
            gdf_sorted = gdf_sorted[gdf_sorted.index < 3].copy()
            # 1 closest largest NZREACH
            gdf_sorted = gdf_sorted.sort_values(by="CATCHAREA", ascending=False).reset_index(drop=True)
        else:
            logger.debug(f"{catch_id} Input geom_pont is invalid")
            return None, None, gdf_catch, gdf_rec_catch
    else:
        raise ValueError(f"{catch_id} Not support column")
    net_id = gdf_sorted.NZREACH.values[0]
    logger.debug(f"{catch_id} Retrieve NZREACH: {net_id}")
    gdf_sorted = gdf_sorted[gdf_sorted.index == 0]
    return net_id, gdf_sorted, gdf_catch, gdf_rec_catch


def gen_river_network(gdf_roi, gdf_rec, gdf_flow, path):
    if gdf_rec.crs.to_epsg() != 2193:
        gdf_rec.to_crs(2193, inplace=True)
    if gdf_roi.crs.to_epsg() != 2193:
        gdf_roi.to_crs(2193, inplace=True)

    geom = box(*gdf_roi.total_bounds)
    gdf_rec = gdf_rec.clip(geom)
    gdf_rec = gdf_rec[gdf_rec["NZREACH"].isin(gdf_flow["nzreach"])]
    gdf_rec = gdf_rec.drop(
        columns=[
            "LENGTH",
            "CLIMATE",
            "GEOLOGY",
            "DISTSEA",
            "SHAPE_LENG",
            "ORDER1",
            "PS_FLOW",
            "LC_NATIVE",
            "LC_EXOTIC",
            "LC_TUSSOCK",
            "LC_SCRUB",
            "LC_PASTURE",
            "LC_WATER",
            "LC_URBAN",
            "PAST_BEEF",
            "PAST_DAIRY",
            "PAST_DEER",
            "PAST_DRY",
            "PAST_GRAZE",
            "PAST_SHEEP",
            "PAST_MIXED",
            "APATITE",
            "LC_DRAIN1",
            "LC_DRAIN2",
            "LC_DRAIN3",
            "LC_DRAIN4",
            "LC_DRAIN5",
            "LC_DRAIN",
            "RAIN",
            "SLOPE",
            "flow_lps",
            "Flow_T",
        ]
    )
    gdf_rec = gdf_rec.astype({"NZREACH": "int64"})
    gdf_rec = gdf_rec.sort_values(by=["NZREACH"]).reset_index(drop=True)
    gdf_rec.rename(columns=str.lower, inplace=True)
    gdf_flow = gdf_flow.sort_values(by=["nzreach"]).reset_index(drop=True)

    gdf_rec_with_flow = gpd.GeoDataFrame(pd.merge(gdf_rec, gdf_flow, on="nzreach", how="left"))

    gdf_rec_with_flow.to_file((Path(path) / "river" / RIVER_NETWORK_FILE).as_posix())
    logger.info(f"Save river network to {path}")


def loop_proc(
    catch_id: int,
    table: tables.Ttable,
    gdf_sdc: gpd.GeoDataFrame,
    gdf_rec: gpd.GeoDataFrame,
    gdf_coast: gpd.GeoDataFrame,
    update: bool = False,
):
    logger.info(f"*** Processing catchment {catch_id} ***")
    engine = utils.get_database(null_pool=True)
    query = f"SELECT catch_id FROM {table.__tablename__} WHERE catch_id = '{catch_id}' ;"
    df_from_db = pd.read_sql(query, engine)
    engine.dispose()
    gc.collect()

    if df_from_db.empty or update:
        osm_id, endpoint, gdf_river, _, gdf_waterway = get_osmid(catch_id, gdf_sdc, gdf_coast, by="dist")
        if osm_id:
            nzreach, gdf_net, _, gdf_rec_catch = get_recid(catch_id, gdf_sdc, gdf_rec, endpoint, by="dist")
        else:
            logger.debug(f"No river found in catchment {catch_id}")
            return
    else:
        logger.debug(f"Skip {catch_id} due to existing record in {table.__tablename__} and update is {update}.")
        return

    if not df_from_db.empty and update:
        query = f"""UPDATE {table.__tablename__}
                    SET rec_id = {nzreach},
                        osm_id = {osm_id},
                        rec_geometry = '{gdf_net.geometry.values[0]}',
                        osm_geometry = '{gdf_river.geometry.values[0]}',
                        updated_at = '{pd.Timestamp.now()}'
                    WHERE catch_id = '{catch_id}' ;"""
        engine = utils.get_database(null_pool=True)
        engine.execute(query)
        engine.dispose()
        gc.collect()
        logger.info(f"Updated {catch_id} in {table.__tablename__} at {pd.Timestamp.now()}.")
    elif df_from_db.empty:
        query = f"""INSERT INTO {table.__tablename__} (
                    catch_id,
                    rec_id,
                    osm_id,
                    rec_geometry,
                    osm_geometry,
                    created_at,
                    updated_at
                    ) VALUES (
                    {catch_id},
                    {nzreach},
                    {osm_id},
                    '{gdf_net.geometry.values[0]}',
                    '{gdf_river.geometry.values[0]}',
                    '{pd.Timestamp.now()}',
                    '{pd.Timestamp.now()}'
                    ) ;"""
        engine = utils.get_database(null_pool=True)
        engine.execute(query)
        engine.dispose()
        gc.collect()
        logger.info(f"Add new {catch_id} in {table.__tablename__} at {pd.Timestamp.now()}.")
    else:
        logger.debug(f"Skip {catch_id} due to existing record in {table.__tablename__} and update is {update}.")


def run(engine: Engine, rec_path, coast_path, catch_table=None, update=False):
    """
    Generate river network_id and osm_id table
    """
    table = tables.SDCP if catch_table is None else catch_table

    gdf_sdc = tables.read_postgis_table(engine, table)

    if Path(rec_path).is_file():
        if Path(rec_path).suffix == ".shp" or Path(rec_path).suffix == ".csv":
            gdf_rec = prep_rec(rec_path, save=True)
        else:
            logger.error(f"File not supported {rec_path}")
            return
        if gdf_rec.crs.to_epsg() != 2193:
            gdf_rec.to_crs(2193, inplace=True)
    else:
        logger.error(f"File not exist {rec_path}")
        return

    if Path(coast_path).is_file():
        if Path(coast_path).suffix == ".shp" or Path(coast_path).suffix == ".gpkg":
            gdf_coast = prep_coast_osm(coast_path, save=True)
        elif Path(coast_path).suffix == ".geojson":
            gdf_coast = prep_coast_linz(coast_path, save=True)
        else:
            logger.error(f"File not supported {coast_path}")
            return
        if gdf_coast.crs.to_epsg() != 2193:
            gdf_coast.to_crs(2193, inplace=True)
    else:
        logger.error(f"File not exist {coast_path}")
        return

    catch_id = sorted(gdf_sdc.catch_id.to_list())
    logger.info(
        f"******* Start process from {table.__tablename__} "
        f"catch_id {sorted(catch_id)[0]} to {sorted(catch_id)[-1]} *********"
    )

    with Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.starmap(
            loop_proc,
            zip(catch_id, repeat(tables.RIVER), repeat(gdf_sdc), repeat(gdf_rec), repeat(gdf_coast), repeat(update)),
        )
        pool.close()
        pool.join()

    # for i in catch_id:
    #     logger.info(f"*** Processing catchment {i} ***")
    #     # make sure the table exist
    #     tables.create_table(engine, tables.RIVER)

    #     query = f"SELECT catch_id FROM {tables.RIVER.__tablename__} WHERE catch_id = '{i}' ;"
    #     df_from_db = pd.read_sql(query, engine)

    #     if df_from_db.empty or update:
    #         osm_id, endpoint, gdf_river, _, gdf_waterway = get_osmid(i, gdf_sdc, gdf_coast, by="dist")
    #         if osm_id:
    #             nzreach, gdf_net, _, gdf_rec_catch = get_recid(i, gdf_sdc, gdf_rec, endpoint, by="dist")
    #         else:
    #             logger.debug(f"No river found in catchment {i}")
    #             continue
    #     else:
    #         logger.info(f"Skip {i} due to existing record in {tables.RIVER.__tablename__} and update is {update}.")
    #         continue

    #     if not df_from_db.empty and update:
    #         query = f"""UPDATE {tables.RIVER.__tablename__}
    #                     SET rec_id = {nzreach},
    #                         osm_id = {osm_id},
    #                         rec_geometry = '{gdf_net.geometry.values[0]}',
    #                         osm_geometry = '{gdf_river.geometry.values[0]}',
    #                         updated_at = '{pd.Timestamp.now()}'
    #                     WHERE catch_id = '{i}' ;"""
    #         engine.execute(query)
    #         logger.info(f"Updated {i} in {tables.RIVER.__tablename__} at {pd.Timestamp.now()}.")
    #     elif df_from_db.empty:
    #         query = f"""INSERT INTO {tables.RIVER.__tablename__} (
    #                     catch_id,
    #                     rec_id,
    #                     osm_id,
    #                     rec_geometry,
    #                     osm_geometry,
    #                     created_at,
    #                     updated_at
    #                     ) VALUES (
    #                     {i},
    #                     {nzreach},
    #                     {osm_id},
    #                     '{gdf_net.geometry.values[0]}',
    #                     '{gdf_river.geometry.values[0]}',
    #                     '{pd.Timestamp.now()}',
    #                     '{pd.Timestamp.now()}'
    #                     ) ;"""
    #         engine.execute(query)
    #         logger.info(f"Add new {i} in {tables.RIVER.__tablename__} at {pd.Timestamp.now()}.")
    #     else:
    #         logger.info(f"Skip {i} due to existing record in {tables.RIVER.__tablename__} and update is {update}.")

    # logger.info(f"Finished process {tables.RIVER.__tablename__} at {pd.Timestamp.now()}.")


if __name__ == "__main__":
    from newzealidar import logs

    ox.settings.log_console = False

    logs.setup_logging()

    engine = utils.get_database()

    rec_path = r"../datastorage/REC1/rec1.shp"
    coast_path = r"../datastorage/coastlines_4326/lines.shp"
    run(engine, rec_path, coast_path, catch_table=tables.SDC, update=True)
