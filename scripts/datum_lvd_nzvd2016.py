# -*- coding: utf-8 -*-
# usage: in prompt of conda environment containing pdal, set correct paths, run the script. e.g.:
#        > conda activate lidar
#        > python NewZeaLiDAR/scripts/datum_lvd_nzvd2016.py 3  # 3 is the index of path_list

import os
import sys
import pathlib
from functools import partial
import subprocess
import multiprocessing as mp
from multiprocessing.pool import ThreadPool


def get_gtxfile(src_dir: pathlib.Path):
    """Get gtx file according to source directory"""
    # gtx file source: https://github.com/linz/proj-datumgrid-nz
    # git clone https://github.com/linz/proj-datumgrid-nz.git
    gtxfile_moturiki_1953 = r'./proj-datumgrid-nz/files/moturiki_1953.gtx'
    gtxfile_auckland_1946 = r'./proj-datumgrid-nz/files/auckht1946-nzvd2016.gtx'
    gtxfile_lyttleton_1937 = r'./proj-datumgrid-nz/files/lyttht1937-nzvd2016.gtx'
    gtxfile_wellington_1953 = r'./proj-datumgrid-nz/files/wellht1953-nzvd2016.gtx'
    if 'NZ10_WHope' in str(src_dir) or 'NZ10_CAlpine' in str(src_dir):
        gtxfile = gtxfile_lyttleton_1937
    elif 'NZ10_Wellington' in str(src_dir):
        gtxfile = gtxfile_wellington_1953
    elif 'Auckland_1946' in str(src_dir):
        gtxfile = gtxfile_auckland_1946
    else:
        gtxfile = gtxfile_moturiki_1953
    return gtxfile


def get_pipeline(file_name: str):
    """Get pipeline according to file type"""
    # pipeline files
    pipeline_laz = r'./NewZeaLiDAR/configs/pipeline_laz.json'
    pipeline_las = r'./NewZeaLiDAR/configs/pipeline_las.json'
    pipeline_xyz = r'./NewZeaLiDAR/configs/pipeline_xyz.json'
    assert os.path.exists(pipeline_laz), 'pipeline_laz.json not found!'
    assert os.path.exists(pipeline_las), 'pipeline_las.json not found!'
    assert os.path.exists(pipeline_xyz), 'pipeline_xyz.json not found!'

    if file_name.lower().endswith('.laz'):
        pipeline = pipeline_laz
    elif file_name.lower().endswith('.las'):
        pipeline = pipeline_las
    elif file_name.lower().endswith('.xyz'):
        pipeline = pipeline_xyz
    else:
        raise ValueError('Unknown file type!')
    return pipeline


def gen_single_cmd(src_dir: pathlib.Path, file_name: str, horizontal_srs: str = "EPSG:2193"):
    """Generate pdal command for single file"""
    pipeline = get_pipeline(file_name)

    src_file = str(src_dir / pathlib.Path(file_name))

    if 'NZ10_WHope' in str(src_dir) or 'NZ10_CAlpine' in str(src_dir) or 'NZ10_Wellington' in str(src_dir):
        dest_dir = src_dir / pathlib.Path('NZVD2016')
    else:  # waikato_lidar
        dest_dir = src_dir.parent.parent / pathlib.Path('NZVD2016')
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest_file = str(dest_dir / pathlib.Path(file_name).with_suffix('.laz'))

    gtxfile = get_gtxfile(src_dir)

    pdal_cmd = 'pdal pipeline {} ' \
               '--readers.las.filename={} ' \
               '--writers.las.filename={} ' \
               '--filters.reprojection.out_srs="+init={} +geoidgrids={}"'.format(
        pipeline, src_file, dest_file, horizontal_srs, gtxfile)
    print(pdal_cmd)
    return pdal_cmd


# print('Work dir: ', pathlib.Path.cwd())
path_list = [
    # not support space in path, need modify directory name from 'Processed Point Cloud' to 'Processed_Point_Cloud'
    r'./datastorage/lidar/NZ10_WHope',
    r'./datastorage/lidar/NZ10_CAlpine',
    r'./datastorage/lidar/NZ10_Wellington',
    r'./datastorage/lidar_waikato/LiDAR_2014_Hipaua_Thermal_Area/Moturiki1953/Processed_Point_Cloud',
    r'./datastorage/lidar_waikato/LiDAR_2012_2013_Coromandel/Auckland_1946/Processed_Point_Cloud',
    r'./datastorage/lidar_waikato/LiDAR_2010_2011/Northern_Waikato/Processed_Point_Cloud/Moturiki_1953',
    r'./datastorage/lidar_waikato/LiDAR_2010_2011/Raglan_Harbour/Processed_Point_Cloud/Moturiki_1953',
    r'./datastorage/lidar_waikato/LiDAR_2007_2008/Area_1/XYZI/ground',
    r'./datastorage/lidar_waikato/LiDAR_2007_2008/Area_1_Option_B/XYZI/ground',
    r'./datastorage/lidar_waikato/LiDAR_2007_2008/Area_2/XYZI/ground',
    r'./datastorage/lidar_waikato/LiDAR_2007_2008/Area_3/XYZI/ground',
    r'./datastorage/lidar_waikato/LiDAR_2007_2008/Area_4/XYZI/ground',
    r'./datastorage/lidar_waikato/LiDAR_2007_2008/Area_5/XYZI/ground',
    r'./datastorage/lidar_waikato/LiDAR_2007_2008/Area_6/XYZI/ground',
    r'./datastorage/lidar_waikato/LiDAR_2006_Lake_Taupo/Moturiki_1953/ground',
]
assert int(sys.argv[1]) < len(path_list), 'Input Index out of range!'
src_dir = pathlib.Path(path_list[int(sys.argv[1])])
print('Transforming datum from LVD to NZVD2016 in dir:\n', src_dir)

pdal_cmd_list = []
for file in src_dir.glob('*.*'):
    pdal_cmd = gen_single_cmd(src_dir, file.name)
    pdal_cmd_list.append(pdal_cmd)

print(f'Transferring datum for {len(pdal_cmd_list)} lidar files...')

with ThreadPool(mp.cpu_count()) as pool:
    pool.map(partial(subprocess.run, shell=True, check=True, text=True, capture_output=True), pdal_cmd_list)

# if 'NZ10_WHope' in str(src_dir) or 'NZ10_CAlpine' in str(src_dir) or 'NZ10_Wellington' in str(src_dir):
#     dest_files = [f for f in dest_dir.glob('*.laz')]
#     src_files = [f for f in src_dir.glob('*.laz')]
#     if len(dest_files) != len(pdal_cmd_list):
#         raise Exception('Number of laz files not equal to number of pdal commands!')
#     # delete las files
#     for f in src_files:
#         f.unlink()
#     # move laz files to src_dir
#     for f in dest_files:
#         f.rename(src_dir / pathlib.Path(f.name))
#     dest_dir.rmdir()

with open(src_dir / 'datum.log', 'w') as file:
    file.write('\n'.join(pdal_cmd_list))

print('Done!')
