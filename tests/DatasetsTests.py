# -*- coding: utf-8 -*-
import os
import unittest
from unittest import mock, TestCase

from src import datasets, utils
from . import Base


class DatasetsTests(Base, TestCase):
    """Tests the datasets module."""

    @mock.patch.dict(os.environ, {'DATA_DIR': r'tests/data',
                                  'POSTGRES_PORT': utils.get_env_variable('POSTGRES_PORT_TEST')})
    def test_datasets(self):
        """
        basic test of datasets module.
        it will scrape the opentopography website, save the related information to dataset table in database,
        and download dataset metadata and extent files to local storage.
        """
        datasets.run()


if __name__ == '__main__':
    unittest.main()
