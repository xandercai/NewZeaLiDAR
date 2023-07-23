# -*- coding: utf-8 -*-
import os
import unittest
from unittest import mock, TestCase

from src import catchments, utils
from . import Base


class CatchmentsTests(Base, TestCase):
    """Tests the catchments module."""

    def setUp(self):
        """override the parameters in base class if needed."""

    @mock.patch.dict(os.environ, {'DATA_DIR': r'tests/data',
                                  'POSTGRES_PORT': utils.get_env_variable('POSTGRES_PORT_TEST')})
    def test_catchments_with_json(self):
        """
        basic test of catchments module.
        it will download the catchments data by API and save to local database,
        then generate the catchments json file following the catchment_list to the catchment_path.
        """
        catchments.run()


if __name__ == '__main__':
    unittest.main()
