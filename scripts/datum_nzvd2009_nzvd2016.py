# -*- coding: utf-8 -*-
# usage: in prompt of conda environment containing pdal, set correct paths, run the script. e.g.:
#        > conda activate lidar
#        > python NewZeaLiDAR/scripts/datum_nzvd2009_nzvd2016.py 3  # 3 is the index of path_list

import os
import sys
import json
import pandas as pd
import pathlib
import laspy
import lasio
from functools import partial
import subprocess
import multiprocessing as mp
from multiprocessing.pool import ThreadPool

# LVD to NZVD2009 offset
# source:
# https://www.linz.govt.nz/guidance/geodetic-system/coordinate-systems-used-new-zealand/vertical-datums/new-zealand-vertical-datum-2009-nzvd2009
# 'one_tree_point_1963': 0.06,
# 'Auckland_1946': 0.34,
# 'Moturiki_1953': 0.24,
# 'Gisborne_1926': 0.34,
# 'Napier_1962': 0.20,
# 'Taranaki_1970': 0.32,
# 'Wellington_1953': 0.44,
# 'Nelson_1955': 0.29,
# 'Lyttelton_1937': 0.47,
# 'Dunedin_1958': 0.49,
# 'Dunedin-Bluff_1960': 0.38,
# 'Bluff_1955': 0.36,
# 'Stewart_Island_1977': 0.39,

# dataset info mapping: the dataset will transfer from NZVD2009 to LVD, then to NZVD2016.
dataset_info = {
    '0': {
        'name': 'NZ10_Wairarapa',
        'lvd': 'Wellington_1953',
        'path': r'../datastorage/lidar/NZ10_Wairarapa',
        'offset': 0.44,
    },
    '1': {
        'name': 'Auckland_2013',
        'lvd': 'Auckland_1946',
        'path': r'../datastorage/lidar/Auckland_2013',
        'offset': 0.34,
    },
    '2': {
        'name': 'NZ14_Dolan',
        'lvd': 'Lyttelton_1937',
        'path': r'../datastorage/lidar/NZ14_Dolan',
        'offset': 0.47,
    },
    '3': {
        'name': 'NZ10_HopeFault',
        'lvd': 'Lyttelton_1937',
        'path': r'../datastorage/lidar/NZ10_HopeFault',
        'offset': 0.47,
    },
    '4': {
        'name': 'NZ15_Alpine',
        'lvd': 'Lyttelton_1937',
        'path': r'../datastorage/lidar/NZ15_Alpine',
        'offset': 0.47,
    }
}

assert int(sys.argv[1]) < len(dataset_info), 'Input Index out of range!'
dataset = dataset_info[sys.argv[1]]

# def nzvd2009_to_lvd(input_file, offset):
#     """
#     Transform the elevation values in a LAZ file from NZVD2009 to LVD.
#     """
#     input_file = pathlib.Path(input_file)
#     output_file = input_file.parent / (input_file.stem + '_lvd.laz')
#     las = laspy.read(str(input_file))
#     las.z = las.z - offset
#     print(las.header.point_format)
#     print(las.header.version)
#     las_lvd = laspy.create(point_format=las.header.point_format, file_version=las.header.version)
#     las_lvd.points = las.points
#     las_lvd.write(str(output_file))


# # assert str(sys.argv[1]) in dataset_info.keys(), 'Input Index out of range!'
# # src_dir = pathlib.Path(dataset_info[str(sys.argv[1])]['path'])
# src_dir = pathlib.Path(dataset_info['0']['path'])
# print('Transforming datum from NZVD2009 to NZVD2016 in dir:\n', src_dir)
# # offset = offset_mapping[dataset_info[str(sys.argv[1])]['lvd']]
# offset = offset_mapping[dataset_info['0']['lvd']]
#
# laz_files = [f for f in src_dir.glob('*.laz')]
# print(laz_files)
#
# las_data = laspy.read(str(laz_files[0]))
# data = {
#     'X': las_data.x,
#     'Y': las_data.y,
#     'Z': las_data.z,
#     'Intensity': las_data.intensity,
#     'ReturnNumber': las_data.return_num,
#     'NumberOfReturns': las_data.num_returns,
#     'Classification': las_data.classification,
# }
# df_data_in = pd.DataFrame(data)
# z = df_data_in['Z'].values[0][0]
# print(z)
#
# # transform from NZVD2009 to LVD
# with ThreadPool(mp.cpu_count()) as pool:
#     pool.map(partial(nzvd2009_to_lvd, offset=offset), [laz_files[0]])
#
# laz_files = [f for f in src_dir.glob('*_lvd.laz')]
#
# with laspy.read(str(laz_files[0])) as las_data:
#     data = {
#         'X': las_data.x,
#         'Y': las_data.y,
#         'Z': las_data.z,
#         'Intensity': las_data.intensity,
#         'ReturnNumber': las_data.return_num,
#         'NumberOfReturns': las_data.num_returns,
#         'Classification': las_data.classification,
#     }
# df_data_out = pd.DataFrame(data)
# z = df_data_out['Z'].values[0][0]
# print(z)

src_dir = pathlib.Path(dataset['path'])
dest_dir = src_dir / pathlib.Path('NZVD2016')
dest_dir.mkdir(parents=True, exist_ok=True)

# pipeline files
pipeline_laz = r'./NewZeaLiDAR/configs/pipeline_laz.json'
pipeline_laz_out = dest_dir / pathlib.Path('pipeline_laz.json')
assert os.path.exists(pipeline_laz), 'pipeline_laz.json not found!'
horizontal_srs = 'EPSG:2193'

with open(pipeline_laz, 'r') as f:
    config = json.load(f)
    config['pipeline'][2]['offset_z'] = dataset['offset']
with open(pipeline_laz_out, 'w') as f:
    json.dump(config, f, indent=2)

# gtx file source: https://github.com/linz/proj-datumgrid-nz
# git clone https://github.com/linz/proj-datumgrid-nz.git
gtxfile_Auckland_1946 = r'./proj-datumgrid-nz/files/auckht1946-nzvd2016.gtx'
gtxfile_Lyttleton_1937 = r'./proj-datumgrid-nz/files/lyttht1937-nzvd2016.gtx'
gtxfile_Wellington_1953 = r'./proj-datumgrid-nz/files/wellht1953-nzvd2016.gtx'
if dataset['lvd'] == 'Lyttelton_1937':
    gtxfile = gtxfile_Lyttleton_1937
elif dataset['lvd'] == 'Wellington_1953':
    gtxfile = gtxfile_Wellington_1953
elif dataset['lvd'] == 'Auckland_1946':
    gtxfile = gtxfile_Auckland_1946
else:
    raise ValueError(f'LVD {dataset["lvd"]} not support!')

pdal_cmd_list = []
for (path, _, files) in os.walk(src_dir):
    for file in files:
        src_file = str(pathlib.Path(path) / pathlib.Path(file))
        assert os.path.exists(src_file), f'{src_file} not found!'
        pdal_cmd = ''

        if file.lower().endswith('.laz'):
            pipeline = pipeline_laz_out
            dest_file = str(dest_dir / pathlib.Path(file))
            # print(f'Re-projecting {file} with {pipeline} and {gtxfile}...')
            pdal_cmd = 'pdal pipeline {} ' \
                       '--readers.las.filename={} ' \
                       '--writers.las.filename={} ' \
                       '--filters.reprojection.out_srs="+init={} +geoidgrids={}"'.format(
                        pipeline, src_file, dest_file, horizontal_srs, gtxfile)

        if pdal_cmd != '':
            pdal_cmd_list.append(pdal_cmd)

print(f'Transferring datum for {len(pdal_cmd_list)} lidar files...')

with ThreadPool(mp.cpu_count()) as pool:
    pool.map(partial(subprocess.run, shell=True, check=True, text=True, capture_output=True), pdal_cmd_list)

dest_files = [f for f in dest_dir.glob('*.laz')]
src_files = [f for f in src_dir.glob('*.laz')]
if len(dest_files) != len(pdal_cmd_list):
    raise Exception('Number of laz files not equal to number of pdal commands!')
# delete las files
for f in src_files:
    f.unlink()
# move laz files to src_dir
for f in dest_files:
    f.rename(src_dir / pathlib.Path(f.name))
dest_dir.rmdir()

with open(src_dir / 'datum.log', 'w') as file:
    file.write('\n'.join(pdal_cmd_list))

print('Done!')
