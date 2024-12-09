# -*- coding: utf-8 -*-
"""
This module is used to get LiDAR datasets information from opentopography.org and save to local database.
It leverages scrapy to crawl the website and download dataset metadata and extent files to local storage.
"""
import gc
import logging
import os
import pathlib
import re
import time
from datetime import datetime

import geopandas as gpd
import pandas as pd
import scrapy
import scrapy.item
from fiona.drvsupport import supported_drivers
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider
from scrapy.pipelines.files import FilesPipeline
from scrapy.spiders import CrawlSpider
from shapely.geometry import Polygon

from newzealidar import utils
from newzealidar.tables import DATASET, create_table, delete_table, get_max_value

logger = logging.getLogger(__name__)

supported_drivers["LIBKML"] = "rw"


def get_extent_geometry(item: scrapy.Item) -> gpd.GeoSeries.geometry:
    """Get extent geometry from geojson file."""
    file = pathlib.Path(item["extent_path"])
    file = file.parent / pathlib.Path("tmp_datasets__" + str(file.name))
    # new added: filter out the datasets with datum other than NZVD2016
    if item["datum"] != "NZVD2016":
        logger.warning(
            f"original datum is not NZVD2016, will use empty geometry for dataset {item['name']}."
        )
        gdf = gpd.GeoDataFrame(index=[0], crs="epsg:2193", geometry=[Polygon()])
    else:
        if os.path.exists(file):
            gdf = gpd.read_file(file)
            gdf = gdf.to_crs(2193)
        else:  # even file not exists, do not change item['extent_path'] to empty
            # if the dataset does not provide the extent file, use the tile index file to get the extent.
            # do not suggest to use this method, because read and transform tile index file to geometry is slow.
            # the tile index file will not exist if lidar.py does not download the tile index file.
            if os.path.exists(item["tile_path"]):
                logger.warning(
                    f"Extent file {file} is not exist, will use tile index geometry to generate dataset extent."
                )
                gdf = gpd.GeoDataFrame.from_file("zip://" + str(item["tile_path"]))
                assert not gdf.empty, f'Tile index file {item["tile_path"]} is empty.'
                assert "2193" in str(
                    gdf.crs
                ), f'Tile index file {item["tile_path"]} is not epsg:2193.'
                # get the extent of the dataset
                geom = gdf["geometry"].unary_union
                # remove gaps
                geom = geom.buffer(2, join_style="mitre").buffer(-2, join_style="mitre")
                gdf = gpd.GeoDataFrame(index=[0], crs="epsg:2193", geometry=[geom])
            else:  # even file not exists, do not change item['tile_path'] to empty
                logger.warning(
                    f'Extent file {file} and tile index file {item["tile_path"]} are not available, '
                    f"use empty geometry."
                )
                gdf = gpd.GeoDataFrame(index=[0], crs="epsg:2193", geometry=[Polygon()])
    return gdf["geometry"].values[0]


def search_string(pattern: str, string: str) -> str:
    """Search string by pattern using regex."""
    match = re.search(pattern, string)
    if not match:
        logger.warning(f'No target pattern: "{pattern}" found in string: "{string}".')
    return match.group(1)


# define item class to scrape
class DatasetItem(scrapy.Item):
    """A class to define item class to scrape."""

    dataset_url = scrapy.Field()
    file_urls = (
        scrapy.Field()
    )  # for download automatically by scrapy.pipelines.files.FilesPipeline
    name = scrapy.Field()
    # ot_id = scrapy.Field()
    describe = scrapy.Field()
    # collector = scrapy.Field()
    survey_start_date = scrapy.Field()
    survey_end_date = scrapy.Field()
    publication_date = scrapy.Field()
    point_cloud_density = scrapy.Field()
    meta_path = scrapy.Field()
    extent_path = scrapy.Field()
    tile_path = scrapy.Field()
    datum = scrapy.Field()
    private = scrapy.Field()


class ExtraFilesPipeline(FilesPipeline):
    """
    The class to define the process pipeline after crawling items.
    In detail, it renames download files and saves items to database.
    Check https://docs.scrapy.org/en/latest/topics/item-pipeline.html for more details.
    """

    def file_path(self, request, response=None, info=None, *, item=None):
        """Rename downloaded files."""
        if response:
            end_str = request.url[-3:]
            if end_str == "xml":
                directory = pathlib.Path(item["meta_path"]).parent
                name = "tmp_datasets__" + str(pathlib.Path(item["meta_path"]).name)
                file_path = str(pathlib.PurePosixPath(directory / pathlib.Path(name)))
            elif end_str == "son":  # geojson
                directory = pathlib.Path(item["extent_path"]).parent
                name = "tmp_datasets__" + str(pathlib.Path(item["extent_path"]).name)
                file_path = str(pathlib.PurePosixPath(directory / pathlib.Path(name)))
            else:
                logger.warning(f"input url {request.url} is not correct.")
                file_path = None
            return file_path

    def item_completed(self, results, item, info):
        """Save crawled data to database."""
        if item["private"]:
            logger.warning(f'Private or Embargo dataset: {item["name"]} is not saved to database.')
            return item
        engine = utils.get_database(null_pool=True)
        create_table(engine, DATASET)
        timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %X")
        data = {
            "id": "-1",
            "name": item["name"],
            # 'ot_id': item['ot_id'],
            "describe": item["describe"],
            # 'collector': item['collector'],
            "survey_start_date": item["survey_start_date"],
            "survey_end_date": item["survey_end_date"],
            "publication_date": item["publication_date"],
            "point_cloud_density": item["point_cloud_density"],
            "original_datum": item["datum"],
            "meta_path": item["meta_path"],
            "meta_source": item["file_urls"][1],
            "extent_path": item["extent_path"],
            # 'extent_source': item['file_urls'][1],
            "tile_path": item["tile_path"],
            "geometry": [get_extent_geometry(item)],
            "created_at": timestamp,
            "updated_at": timestamp,
        }
        gdf_to_db = gpd.GeoDataFrame(data, crs="epsg:2193", geometry="geometry")
        query = (
            f"""SELECT * FROM {DATASET.__tablename__} WHERE name = '{item["name"]}' ;"""
        )
        gdf_from_db = gpd.read_postgis(query, engine, geom_col="geometry")
        if gdf_from_db.empty:
            _id = get_max_value(engine, "dataset")
            gdf_to_db["id"] = _id + 1 if _id else 1
        else:
            delete_table(engine, DATASET, "name", item["name"])
            # keep the 'created_at', 'id' and update the rest columns.
            gdf_to_db["id"] = gdf_from_db["id"].copy()
            gdf_to_db["created_at"] = gdf_from_db["created_at"].copy()
        gdf_to_db = gdf_to_db[
            [
                "id",
                "name",
                # 'ot_id',
                "describe",
                "survey_start_date",
                "survey_end_date",
                "publication_date",
                "point_cloud_density",
                "original_datum",
                # 'collector',
                "meta_path",
                "meta_source",
                "extent_path",
                # 'extent_source',
                "tile_path",
                "geometry",
                "created_at",
                "updated_at",
            ]
        ]
        gdf_to_db.to_postgis("dataset", engine, index=False, if_exists="append")
        # check_table_duplication(engine, DATASET, 'name')
        engine.dispose()
        gc.collect()
        return item


class DatasetSpider(CrawlSpider):
    """
    The class to define spider class to scrape defined items.
    Check https://docs.scrapy.org/en/latest/topics/spiders.html for more details.
    """

    name = "dataset"

    # custom_settings = {
    #     'LOG_LEVEL': 'INFO',
    # }  # not working

    allowed_domains = ["portal.opentopography.org", "raw.githubusercontent.com"]

    def __init__(self, data_dir, *a, **kw):
        super(DatasetSpider, self).__init__(*a, **kw)
        self.data_dir = data_dir
        self.start_urls = ("https://portal.opentopography.org/", "https://raw.githubusercontent.com/")

    def start_requests(self):
        urls = [
            "https://portal.opentopography.org/ajaxOTDatasets?search=new%20zealand"
        ]  # only one url now, but keep it in a list for further reuse.
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_table)

    def parse_table(self, response):
        for url in response.xpath(
            '//a[text()="Full Dataset Metadata"]/@href'
        ).extract():
            url = response.urljoin(url)
            yield scrapy.Request(url, callback=self.parse_metadata)

    def parse_metadata(self, response):
        item = DatasetItem()
        _item_name = (
            response.xpath('//strong[text()="Short Name"]/following-sibling::text()[1]')
            .extract()[0]
            .split(":")[-1]
            .strip()
        )
        item["name"] = _item_name

        # item['ot_id'] = response.xpath(
        #     '//strong[text()="OT Collection ID"]/following-sibling::text()[1]'
        # ).extract()[0].split(':')[-1].strip()

        _item_describe = (
            response.xpath(
                '//strong[text()="OT Collection Name"]/following-sibling::text()[1]'
            )
            .extract()[0]
            .split(":")[-1]
            .strip()
        )
        item["describe"] = _item_describe

        date = 0
        survey_date = (
            response.xpath('//strong[text()="Survey Date"]/following-sibling::text()')
            .extract()[0]
            .strip()
        )
        survey_date = survey_date.split("-")
        if len(survey_date) == 1:  # no start date
            date = search_string(r"(\d{2}/\d{2}/\d{4})", survey_date[0])
            _date = datetime.strptime(
                date, "%m/%d/%Y"
            ).strftime("%Y-%m-%d")
            item["survey_start_date"] = item["survey_end_date"] = _date
        elif len(survey_date) == 2:
            date = search_string(r"(\d{2}/\d{2}/\d{4})", survey_date[0])
            _date_s = datetime.strptime(date, "%m/%d/%Y").strftime(
                "%Y-%m-%d"
            )
            item["survey_start_date"] = _date_s
            date = search_string(r"(\d{2}/\d{2}/\d{4})", survey_date[1])
            _date_e = datetime.strptime(date, "%m/%d/%Y").strftime(
                "%Y-%m-%d"
            )
            item["survey_end_date"] = _date_e
            date = (
            response.xpath(
                '//strong[text()="Publication Date"]/following-sibling::text()'
            )
            .extract()[0]
            .strip()
        )
        date = search_string(r"(\d{2}/\d{2}/\d{4})", date)
        _date_p = datetime.strptime(date, "%m/%d/%Y").strftime(
            "%Y-%m-%d"
        )
        item["publication_date"] = _date_p

        # item['collector'] = response.xpath(
        #     '//text()[contains(.,"Collector")]/following-sibling::ul[1]//text()'
        # ).extract()

        _item_point_cloud_density = (
            response.xpath(
                '//strong[text()="Point Density"]/following-sibling::text()[1]'
            )
            .extract()[0]
            .split()[1]
            .strip()
        )
        item["point_cloud_density"] = _item_point_cloud_density

        datum = (
            response.xpath('//text()[contains(., "Vertical:")]').extract()[0].strip()
        )
        _item_datum = search_string(r"Vertical: (\w+\s*\d+)", datum)
        item["datum"] = _item_datum

        item["dataset_url"] = response.url
        file_urls = response.xpath(
            # '//a[text()="ISO 19115 (Data)" or starts-with(@href, "/getKml")]/@href'
            f"""//a[text()="ISO 19115 (Data)" or text()="{item['name']}.geojson"]/@href"""
        ).extract()
        # file_urls = [response.urljoin(u) for u in file_urls]
        _item_urls = []
        for u in file_urls:
            if u.endswith("geojson") and "Point_Cloud" in u:
                _item_urls.append(response.urljoin(u))
            elif u.endswith("xml"):
                _item_urls.append(response.urljoin(u))
            else:
                logger.debug(f"File url {u} is not correct.")
        item["file_urls"] = _item_urls
        print("file_urls: ", item["file_urls"])

        data_path = pathlib.Path(self.data_dir) / item["name"]
        _item_meta_path = str(
            data_path / (item["name"] + "_Meta.xml")
        )
        item["meta_path"] = _item_meta_path

        _item_extent_path = str(
            data_path
            / (item["name"] + ".geojson")
            # / pathlib.Path(item["name"] + "_Extent.kml")
        )
        item["extent_path"] = _item_extent_path

        _item_tile_path = str(
            data_path
            / (item["name"] + "_TileIndex.zip")
        )
        item["tile_path"] = _item_tile_path

        if response.xpath('//text()[contains(., "Private Dataset")]').extract():
            _item_private = True
        elif response.xpath('//text()[contains(., "Temporary Embargo")]').extract():
            _item_private = True
        else:
            _item_private = False
        item["private"] = _item_private

        yield item


def crawl_dataset() -> None:
    """
    Crawl the data from the website, save in the database,
    and save the metadata and extent files in the local directory.
    """
    logger.info("Start crawling datasets from OpenTopography.")
    process = CrawlerProcess(
        {
            "USER_AGENT": "Mozilla/5.0 (Windows NT 6.1; WOW64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/34.0.1847.131 Safari/537.36",
            "DOWNLOAD_DELAY": 1.5,  # to avoid request too frequently and get incomplete response.
            "ITEM_PIPELINES": {"newzealidar.datasets.ExtraFilesPipeline": 1},
            "FILES_STORE": "./",
            # "LOG_ENABLED": True,
            # "LOG_LEVEL": "DEBUG",
        }
    )
    process.crawl(
        DatasetSpider,
        # data_dir for DatasetSpider init
        str(
            pathlib.Path(utils.get_env_variable("DATA_DIR"))
            / pathlib.Path(utils.get_env_variable("LIDAR_DIR"))
        ),
    )
    process.start()
    time.sleep(180)  # sleep 3 minutes for scrapy to finish downloading files.
    try:
        # use an exception to stop the process because process.stop() does not work in some cases.
        # which will cause multiple processes running for the following processes (such as `lidar` process).
        raise CloseSpider()
    except CloseSpider:
        logger.info("Finish crawling datasets from OpenTopography.")
        pass


def rename_file():
    """
    Change the name of the downloaded files.

    Scrapy does not overwrite the existing files, so the downloaded files
    will be named to make sure download files are latest for each crawling.
    """
    data_dir = pathlib.Path(utils.get_env_variable("DATA_DIR")) / pathlib.Path(
        utils.get_env_variable("LIDAR_DIR")
    )
    list_file = utils.get_files(["geojson", "xml"], data_dir)
    count = 0
    for file in list_file:
        file = pathlib.Path(file)
        if file.name.startswith("tmp_datasets__"):
            new_file = file.parent / file.name.replace("tmp_datasets__", "")
            if new_file.exists():
                new_file.unlink()
            file.rename(new_file)
            count += 1
    logger.debug(f"Finish renaming {count} .xml and .geojson files.")


def run():
    """
    Run the module.
    """
    crawl_dataset()
    rename_file()
    instructions_file = pathlib.Path(utils.get_env_variable("INSTRUCTIONS_FILE"))
    # generate dataset mapping info
    engine = utils.get_database()
    utils.map_dataset_name(engine, instructions_file)

    # generate lidar extent of all lidar datasets, to filter out catchments without lidar data
    lidar_extent = utils.gen_table_extent(engine, DATASET)
    # save lidar extent to check on QGIS
    utils.save_gpkg(lidar_extent, "lidar_extent")

    engine.dispose()
    gc.collect()
    logger.info("Finish processing datasets by scrapy.")


# for Digital-Twins
def main(gdf=None, log_level="INFO"):
    """Run the module."""
    logger.setLevel(log_level)
    crawl_dataset()
    rename_file()
    instructions_file = pathlib.Path(utils.get_env_variable("INSTRUCTIONS_FILE"))
    # generate dataset mapping info
    engine = utils.get_database()
    utils.map_dataset_name(engine, instructions_file)
    engine.dispose()
    gc.collect()
    logger.info("Finish processing datasets by scrapy.")


if __name__ == "__main__":
    run()
