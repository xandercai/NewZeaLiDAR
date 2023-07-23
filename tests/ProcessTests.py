# -*- coding: utf-8 -*-
import os
import unittest
from unittest import mock, TestCase

from src import process, utils
from . import Base


class ProcessTests(Base, TestCase):
    """Tests the process module."""

    @mock.patch.dict(os.environ, {'DATA_DIR': r'tests/data',
                                  'POSTGRES_PORT': utils.get_env_variable('POSTGRES_PORT_TEST')})
    def test_process(self):
        """
        basic test of process module.
        it will generate instruction json file following the catchment_list and
        save to lidar and tile table in local database.
        """
        process.run(self.catchment_list)


if __name__ == '__main__':
    unittest.main()
