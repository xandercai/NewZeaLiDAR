# -*- coding: utf-8 -*-
import os
import unittest
from unittest import mock, TestCase

from src import catchments, datasets, datasets_waikato, lidar, lidar_waikato, process, utils, tables
from . import Base


class EndToEndTests(Base, TestCase):
    """Tests all the module."""

    @mock.patch.dict(os.environ, {'DATA_DIR': r'tests/data',
                                  'POSTGRES_PORT': utils.get_env_variable('POSTGRES_PORT_TEST')})
    def test_end_to_end_api(self):
        """
        basic test of the pipeline which check (and download) all the data by api from OpenTopography.
        """
        catchments.run()
        datasets.run()
        datasets_waikato.run()
        lidar.run(self.catchment_list)
        lidar_waikato.run()
        process.run(self.catchment_list, mode='api')
        tables.check_all_table_duplicate()

    @mock.patch.dict(os.environ, {'DATA_DIR': r'tests/data',
                                  'POSTGRES_PORT': utils.get_env_variable('POSTGRES_PORT_TEST')})
    def test_end_to_end_local(self):
        """
        basic test of the pipeline which use the data from local storage and database.
        """
        catchments.run()
        datasets.run()
        datasets_waikato.run()
        lidar.run(self.catchment_list)
        lidar_waikato.run()
        process.run(self.catchment_list, mode='local')
        tables.check_all_table_duplicate()


if __name__ == '__main__':
    unittest.main()
