# -*- coding: utf-8 -*-
"""
This module contains logging functions for the package.
"""
import json
import logging
import logging.config
import os
from pathlib import Path
import time
import warnings
from typing import Union

from newzealidar import utils


class FilterRecords(logging.Filter):
    def __init__(self, name="", module="", func="", msg=""):
        super().__init__(name)
        # The name of the logger used to log the event represented by this LogRecord
        self.name = name
        # The full string path of the source file where the logging call was made
        self.module = module
        # The name of the function or method from which the logging call was invoked
        self.func = func
        # The event description message
        self.msg = msg

    def filter(self, record):
        if len(self.name):
            return not (self.name == record.name)
        if len(self.module):
            return not (self.module in record.pathname)
        if len(self.func):
            return not (self.func == record.funcName)
        if len(self.msg):
            return not (self.msg in record.msg)
        return True


def setup_logging(
    default_path=None,
    default_level=None,
    filter_warnings=True,
    env_key="LOG_CFG",
):
    """
    Setup logging configuration
    """
    if filter_warnings:
        warnings.filterwarnings("ignore")

    if default_level is not None:
        for name in logging.Logger.manager.loggerDict.keys():
            logging.getLogger(name).setLevel(logging.ERROR)

    path = utils.get_env_variable(env_key)
    if default_path:
        path = default_path
    if os.path.exists(path):
        print(f"Loading logging configuration from {path}")
        Path("./logs").mkdir(exist_ok=True)
        with open(path, "rt") as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    elif default_level is not None:
        print(f"No logging configuration file. Setting logging level to {default_level}")
        logging.basicConfig(level=default_level)
    else:
        print("No logging configuration file. Setting logging level to INFO")
        logging.basicConfig(level=logging.INFO)

    # add custom filters to the root logger
    logging.getLogger().addFilter(FilterRecords(module="dem"))


def print_logger():
    loggers = [logging.getLogger()]  # get the root logger
    loggers = loggers + [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for i, l in enumerate(loggers):
        print(f"{i} - logger: {l.name} - level: {l.level}, handlers: {l.handlers}")


# def log_setup(module, log_dir: Union[str, Path] = None, level=logging.DEBUG) -> None:
#     """
#     Setup logging for the package.
#     """
#     now = time.strftime("%Y%m%d-%H%M%S")
#     if log_dir is None:
#         # module = Path(__file__).stem
#         log_file = Path(__file__).parent.parent / "logs" / f"{module}-{now}.log"
#     else:
#         log_file = Path(log_dir) / f"{module}-{now}.log"
#     Path(log_file.parent).mkdir(parents=True, exist_ok=True)
#     logging.disable(logging.NOTSET)
#     logging.basicConfig(
#         level=level,
#         format="%(asctime)s %(levelname)7s %(name)6s %(module)10s::%(funcName)12s> %(message)s",
#         handlers=[logging.FileHandler(log_file, mode="w"), logging.StreamHandler()],
#         datefmt="%Y-%m-%d %H:%M:%S",
#         encoding="utf-8",
#         force=True,
#     )
#     logging.captureWarnings(True)
#     logging.getLogger("py.warnings").setLevel(logging.ERROR)
#     logging.getLogger("fiona").propagate = False
#     logging.getLogger("urllib3").propagate = False
#     logging.getLogger("botocore").propagate = False
#     logging.getLogger("rasterio").propagate = False
#     logging.getLogger("boto3").propagate = False
#     logging.getLogger("asyncio").propagate = False
#     logging.getLogger("scrapy").propagate = False
#     logging.getLogger("distributed").propagate = False
