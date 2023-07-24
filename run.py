import pathlib
import sys
import logging
import gc

# if working directory is out side of the project, add the project path to sys.path
# sys.path.insert(0, str(pathlib.Path(r'../ForkGeoFabrics/src/geofabrics')))
# sys.path.insert(0, str(pathlib.Path(r'./NewZeaLidar/src')))
# print(sys.path)

from src import catchments, datasets, datasets_waikato, lidar, lidar_waikato, process, tables, utils, logs


# set the default test parameters
# selected catchments
# catchment_list = [1599, 1547]
# catchment_list = [13070004, 21599001]
catchment_list = 1394
# full catchments
# nz_mainland = r'../NewZeaLidar/configs/nz_mainland.geojson'
# demo = r'../NewZeaLidar/configs/demo.geojson'
# catchment_list = -1
buffer = 20

if __name__ == '__main__':
    logs.setup_logging(default_level=logging.INFO)
    # logs.print_logger()
    # catchments.run(gpkg=True)
    catchments.run()
    datasets.run()
    datasets_waikato.run()
    # lidar.run(catchment_list)
    # lidar.run(nz_mainland)
    lidar.run(roi_id=catchment_list, buffer=buffer)
    lidar_waikato.run()
    # process.run(catch_id=catchment_list, mode='api', gpkg=True)
    # process.run(catch_id=catchment_list, mode='local', gpkg=True)
    # process.run(catch_id=catchment_list, mode='api')
    process.run(catch_id=catchment_list, mode='local')
    tables.check_all_table_duplicate()
