# -*- coding: utf-8 -*-
# usage: in prompt of conda environment containing pdal, set correct paths, run the script. e.g.:
#        > conda activate newzealidar
#        > cd NewZealandLiDAR
#        > python scripts/datum_lvd_nzvd2016.py 3  # 3 is the index of path_list

import os
import sys
import pathlib
import subprocess
import multiprocessing as mp
from multiprocessing.pool import ThreadPool


# print('Work dir: ', pathlib.Path.cwd())
path_list = [
    # not support space in path, need modify directory name from 'Processed Point Cloud' to 'Processed_Point_Cloud'
    r'../datastorage/LiDAR_Waikato/LiDAR_2014_Hipaua_Thermal_Area/Moturiki1953/Processed_Point_Cloud',
    r'../datastorage/LiDAR_Waikato/LiDAR_2012_2013_Coromandel/Auckland_1946/Processed_Point_Cloud',
    r'../datastorage/LiDAR_Waikato/LiDAR_2010_2011/Northern_Waikato/Processed_Point_Cloud/Moturiki_1953',
    r'../datastorage/LiDAR_Waikato/LiDAR_2010_2011/Raglan_Harbour/Processed_Point_Cloud/Moturiki_1953',
    r'../datastorage/LiDAR_Waikato/LiDAR_2007_2008/Area_1/XYZI/ground',
    r'../datastorage/LiDAR_Waikato/LiDAR_2007_2008/Area_1_Option_B/XYZI/ground',
    r'../datastorage/LiDAR_Waikato/LiDAR_2007_2008/Area_2/XYZI/ground',
    r'../datastorage/LiDAR_Waikato/LiDAR_2007_2008/Area_3/XYZI/ground',
    r'../datastorage/LiDAR_Waikato/LiDAR_2007_2008/Area_4/XYZI/ground',
    r'../datastorage/LiDAR_Waikato/LiDAR_2007_2008/Area_5/XYZI/ground',
    r'../datastorage/LiDAR_Waikato/LiDAR_2007_2008/Area_6/XYZI/ground',
    r'../datastorage/LiDAR_Waikato/LiDAR_2006_Lake_Taupo/Moturiki_1953/ground',
]
assert int(sys.argv[1]) < len(path_list), 'Input Index out of range!'
src_dir = pathlib.Path(path_list[int(sys.argv[1])])
print('Transforming datum from LVD to NZVD2016 in dir:\n', src_dir)

# gtx file source: https://github.com/linz/proj-datumgrid-nz
gtxfile_Moturiki_1953 = r'../proj-datumgrid-nz/files/moturiki_1953.gtx'
gtxfile_Auckland_1946 = r'../proj-datumgrid-nz/files/auckht1946-nzvd2016.gtx'
if 'Auckland_1946' in str(src_dir):
    gtxfile = gtxfile_Auckland_1946
else:
    gtxfile = gtxfile_Moturiki_1953

# pipeline files
pipeline_las = r'configs/pipeline_las.json'
pipeline_xyz = r'configs/pipeline_xyz.json'
horizontal_srs = 'EPSG:2193'

dest_dir = src_dir.parent.parent / pathlib.Path('NZVD2016')
if not os.path.exists(dest_dir):
    os.mkdir(dest_dir)

pdal_cmd_list = []

for (path, _, files) in os.walk(src_dir):
    for file in files:
        src_file = str(pathlib.Path(path) / pathlib.Path(file))
        pdal_cmd = ''

        if file.lower().endswith('.las'):
            pipeline = pipeline_las
            if '.las' in file:
                file = file.replace('.las', '.laz')
            if '.LAS' in file:
                file = file.replace('.LAS', '.laz')
            dest_file = str(dest_dir / pathlib.Path(file))
            # print(f'Re-projecting {file} with {pipeline} and {gtxfile}...')
            pdal_cmd = 'pdal pipeline {} ' \
                       '--readers.las.filename={} ' \
                       '--writers.las.filename={} ' \
                       '--filters.reprojection.out_srs="+init={} +geoidgrids={}"'.format(
                           pipeline, src_file, dest_file, horizontal_srs, gtxfile)

        if file.lower().endswith('.xyz') or file.lower().endswith('.xyzi'):
            pipeline = pipeline_xyz
            if '.XYZI' in file:
                file = file.replace('.XYZI', '.laz')
            if '.xyz' in file:
                file = file.replace('.xyz', '.laz')
            if '.XYZ' in file:
                file = file.replace('.XYZ', '.laz')
            dest_file = str(dest_dir / pathlib.Path(file))
            # print(f'Re-projecting {file} with {pipeline} and {gtxfile}...')
            pdal_cmd = 'pdal pipeline {} ' \
                       '--readers.text.filename={} ' \
                       '--writers.las.filename={} ' \
                       '--filters.reprojection.out_srs="+init={} +geoidgrids={}"'.format(
                           pipeline, src_file, dest_file, horizontal_srs, gtxfile)

        if pdal_cmd != '':
            pdal_cmd_list.append(pdal_cmd)

print(f'Tranfering datum for {len(pdal_cmd_list)} lidar files...')

with ThreadPool(mp.cpu_count()) as pool:
    pool.map(subprocess.run, pdal_cmd_list)

print('Done!')
