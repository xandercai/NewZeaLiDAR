# -*- coding: utf-8 -*-
import logging
import pathlib
import unittest
import os


class Base(unittest.TestCase):
    """Base class"""

    work_dir = pathlib.Path.cwd().as_posix()
    test_path = pathlib.Path(__file__).parent.absolute().as_posix()

    @classmethod
    def setUpClass(cls):
        """setup the default test parameters"""
        cls.setUpLog()
        logging.info(f"*** run test case in work dir '{cls.work_dir}' "
                     f"and save output in test dir '{cls.test_path}/data'. ***")
        # cls.catchment_list = [1588, 1548, 1593, 1596]
        cls.catchment_list = [1548]

    @classmethod
    def tearDownClass(cls):
        pass

    @classmethod
    def setUpLog(cls):
        if not os.path.exists(cls.test_path / pathlib.Path('logs')):
            os.makedirs(cls.test_path / pathlib.Path('logs'))
        logging.basicConfig(
            filename=str(cls.test_path / pathlib.Path(f'logs/{cls.__name__}.log')),
            filemode="w",
            # format='%(asctime)s | %(levelname)s | %(module)s : %(message)s',
            # format='%(asctime)s | %(levelname)s | %(name)s : %(message)s',
            format='%(asctime)s %(levelname)7s %(name)6s %(module)10s::%(funcName)12s> %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            encoding="utf-8",
            level=logging.DEBUG,
            force=True,
        )
        logging.captureWarnings(True)
        logging.getLogger("py.warnings").setLevel(logging.ERROR)
        # logging.getLogger('scrapy').setLevel(logging.INFO)
        # logging.getLogger('scrapy').propagate = False
        logging.getLogger('fiona').propagate = False
        logging.getLogger('rasterio').propagate = False
        logging.getLogger('urllib3').propagate = False
        logging.getLogger('botocore').propagate = False
        logging.getLogger('boto3').propagate = False
        logging.getLogger('s3transfer').propagate = False
        logging.getLogger('paramiko').propagate = False
        logging.getLogger('asyncio').propagate = False

        logging.getLogger().addHandler(logging.StreamHandler())
