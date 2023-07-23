# -*- coding: utf-8 -*-
import os
import unittest
from unittest import mock, TestCase

from src import lidar_waikato, utils
from . import Base


class LidarWaikatoTests(Base, TestCase):
    """Tests the lidar waikato module."""

    def setUp(self):
        """override the parameters in base class if needed."""

        self.dataset_info = {
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
                r'tests/data/LiDAR_Waikato/LiDAR_2014_Hipaua_Thermal_Area',
                r'tests/data/LiDAR_Waikato/LiDAR_2012_2013_Coromandel',
                r'tests/data/LiDAR_Waikato/LiDAR_2010_2011/Northern_Waikato',
                r'tests/data/LiDAR_Waikato/LiDAR_2010_2011/Raglan_Harbour',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008/Area_1',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008/Area_1_Option_B',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008/Area_2',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008/Area_3',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008/Area_4',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008/Area_5',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008/Area_6',
                r'tests/data/LiDAR_Waikato/LiDAR_2006_Lake_Taupo',
            ],
            "tile_dir": [
                r'tests/data/LiDAR_Waikato/LiDAR_2014_Hipaua_Thermal_Area',
                r'tests/data/LiDAR_Waikato/LiDAR_2012_2013_Coromandel',
                r'tests/data/LiDAR_Waikato/LiDAR_2010_2011',
                r'tests/data/LiDAR_Waikato/LiDAR_2010_2011',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008',
                r'tests/data/LiDAR_Waikato/LiDAR_2007_2008',
                r'tests/data/LiDAR_Waikato/LiDAR_2006_Lake_Taupo',
            ]
        }

    @mock.patch.dict(os.environ, {'DATA_DIR': r'tests/data',
                                  'POSTGRES_PORT': utils.get_env_variable('POSTGRES_PORT_TEST')})
    def test_lidar_with_dataset_info(self):
        """
        basic test of lidar_waikato module.
        save input dataset information to lidar and tile table in local database.
        """
        lidar_waikato.run(self.dataset_info)

    @mock.patch.dict(os.environ, {'DATA_DIR': r'tests/data',
                                  'POSTGRES_PORT': utils.get_env_variable('POSTGRES_PORT_TEST')})
    def test_lidar_without_dataset_info(self):
        """
        basic test of lidar_waikato module.
        save default dataset information to lidar and tile table in local database.
        """
        lidar_waikato.run()


if __name__ == '__main__':
    unittest.main()
