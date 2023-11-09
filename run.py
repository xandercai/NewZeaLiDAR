import pathlib
import sys
import logging

# if working directory is out side of the project, add the project path to sys.path
# sys.path.insert(0, str(pathlib.Path(r'../ForkGeoFabrics/src/geofabrics')))
# sys.path.insert(0, str(pathlib.Path(r'./NewZeaLidar/src')))
# print(sys.path)

from newzealidar import (
    catchments,
    datasets,
    datasets_waikato,
    lidar,
    lidar_waikato,
    process,
    tables,
    utils,
    logs,
)


# set the default test parameters
# selected catchments
# catchment_list = [1599, 1547]
# catchment_list = [13070004, 21599001]
# catchment_list = 1394
# catchment_list = 50
# full catchments
# nz_mainland = r'./NewZeaLiDAR/configs/nz_mainland.geojson'
demo = r"./configs/kaikoura.geojson"
# catchment_list = -1
buffer = 14

if __name__ == "__main__":
    logs.setup_logging()
    # logs.print_logger()
    # catchments.run(grid_only=True)
    catchments.run()
    datasets.run()
    # datasets_waikato.run()
    # lidar.run(roi_id=1581, grid=True)
    # lidar.run(roi_file=demo, buffer=buffer)
    # lidar.run(roi_id=catchment_list, buffer=buffer)
    lidar.run(name_base=True, buffer=buffer)
    # lidar_waikato.run()
    # process.run(catch_id=catchment_list, mode='api', gpkg=True)
    # process.run(catch_id=catchment_list, mode='local', gpkg=True)
    # process.run(catch_id=catchment_list, mode='api')
    # process.run(catch_id="2673", mode="local")
    # process.main(catchment_boundary=demo)
    # process.run_grid(grid_id=[120322, 120558], mode="local", gpkg=True)
    process.run_grid(mode="local", gpkg=True)
    tables.check_all_table_duplicate()
