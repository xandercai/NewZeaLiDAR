# -*- coding: utf-8 -*-
"""
A package for generating New Zealand nationwide LiDAR datasets collection, including an application
(see `process` module) that using GeoFabrics generate hydrologically conditioned DEMs from LiDAR.

Modules:
    * catchments: get sea draining catchments dataset from MFE data service
    * datasets, datasets_waikato: get LiDAR datasets information from opentopography.org and Waikato Regional Council
    * lidar, lidar_waikato: download and save LiDAR files to local storage and database.
    * process: generate hydrologically conditioned DEMs from LiDAR.
    * tables: definition and utility functions for database tables.
    * utils: utility functions for the package.
"""

from src import version

__version__ = version.__version__
