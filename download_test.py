import geopandas as gpd
import glob
from pathlib import Path
import pandas as pd
import os
import zipfile
import earthaccess

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pathlib import Path

os.environ["EARTHDATA_USERNAME"] = "hkw20021009"
os.environ["EARTHDATA_PASSWORD"] = "@Hhkwh83426637"

earthaccess.login(strategy="environment")

print(earthaccess.__version__)

# SWOT 2 级 KaRIn 高速率版本 D 数据集：
# Water Mask Pixel Cloud NetCDF - SWOT_L2_HR_PIXC_D
# Water Mask Pixel Cloud Vector Attribute NetCDF - SWOT_L2_HR_PIXCVec_D
# River Vector Shapefile - SWOT_L2_HR_RiverSP_D
# Lake Vector Shapefile - SWOT_L2_HR_LakeSP_D
# Raster NetCDF - SWOT_L2_HR_Raster_D


# 让我们使用特定通道 013 开始搜索 River Vector Shapefile。
# SWOT 文件在同一个集合中分为“reach”和“node”版本，这里我们想要 10km 的 reach 而不是 nodes。
# 我们也将只获取北美或“NA”的文件，并调出我们想要的特定通行证编号。*LAKETILE*_013_AS* -106.62, 38.809, -106.54, 38.859 granule_name = '*034_493*',
results = earthaccess.search_data(short_name = 'SWOT_L2_HR_Raster_D',
                                  temporal = ('2025-05-01 00:00:00', '2025-05-30 23:59:59'), # can also specify by time
                                  # granule_name = '*034_493_208L*',
                                  bounding_box=(115.98,28.90,116.38,29.20)
                                  ) # here we filter by Reach files (not node), pass=013, continent code=NA

granule_list = list(results)
print(f"Total granules found: {len(granule_list)}")

download_dir = Path(".\\data_downloads\\PoYangHu_data\\")
download_dir.mkdir(parents=True, exist_ok=True)

folder = Path(".\\data_downloads\\PoYangHu_data")

# 创建一个带有重试机制的会话
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

# 下载数据
for granule in granule_list:
    try:
        earthaccess.download(granule, download_dir)
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error occurred while downloading {granule}. Retrying...")
        continue
    except Exception as e:
        print(f"An error occurred while downloading {granule}: {e}")
        continue

print("Download completed.")


for item in os.listdir(folder): # loop through items in dir
    if item.endswith(".zip"): # check for ".zip" extension
        zip_ref = zipfile.ZipFile(f"{folder}/{item}") # create zipfile object
        zip_ref.extractall(folder) # extract file to dir
        zip_ref.close() # close file

os.listdir(folder)