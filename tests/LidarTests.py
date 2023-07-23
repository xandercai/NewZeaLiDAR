# -*- coding: utf-8 -*-
import os
import unittest
from unittest import mock, TestCase

from src import lidar, utils
from . import Base


class LidarTests(Base, TestCase):
    """Tests the lidar module."""

    @mock.patch.dict(os.environ, {'DATA_DIR': r'tests/data',
                                  'POSTGRES_PORT': utils.get_env_variable('POSTGRES_PORT_TEST')})
    def test_lidar(self):
        """
        basic test of lidar module.
        it will download the lidar data by API from opentopography website and
        save to lidar and tile table in local database.
        """
        lidar.run(self.catchment_list)


if __name__ == '__main__':
    unittest.main()
