import subprocess
import os
import re
from datetime import datetime

"""
使用 podaac-data-downloader 批量下载 SWOT 数据。

参数说明:
----------
   
-c COLLECTION, --collection-shortname：数据集 shortname，必填
    Water Mask Pixel Cloud NetCDF - SWOT_L2_HR_PIXC_D
    Water Mask Pixel Cloud Vector Attribute NetCDF - SWOT_L2_HR_PIXCVec_D
    River Vector Shapefile - SWOT_L2_HR_RiverSP_D
    Lake Vector Shapefile - SWOT_L2_HR_LakeSP_D
    Raster NetCDF - SWOT_L2_HR_Raster_D
    ……
    
-d OUTPUTDIRECTORY, --data-dir：下载保存目录，必填
--start-date / -sd：开始日期
--end-date / -ed：结束日期

--cycle：指定下载 cycle 数，可重复
-f, --force：强制覆盖（即使已有文件且校验一致）
-b, --bounds：地理范围过滤（W,S,E,N），需以 -b="..." 传入
-dc, -dydoy, -dymd, -dy：设置目录命名方式
--offset OFFSET：时间偏移（小时）
-e, --extensions：文件后缀过滤（正则表达式）
-gr, --granule-name：指定颗粒名（支持 wildcard）
--process PROCESS_CMD：下载后运行额外命令
--limit LIMIT：下载颗粒数量上限
-p PROVIDER：指定数据源（如 ASF）
--dry-run：仅列出，将不实际下载
--verbose：打印详细日志
--version：显示版本并退出

示例调用:
----------
download_swot_data(
    collection_shortname="SWOT_L2_HR_PIXC_D",
    output_directory=".\\data_downloads\\downloader_data\\",
    start_date="2025-05-01T00:00:00Z",
    end_date="2025-05-30T00:00:00Z",
    bounding_box="116,28.9,116.4,29.2"
)
"""


def is_valid_iso_date(date_str):
    try:
        datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False

def validate_inputs(collection, output_dir, start_date, end_date):
    errors = []

    if not collection:
        errors.append("collection_shortname（-c）不能为空。")

    if not os.path.isdir(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
            print(f"创建目录：{output_dir}")
        except Exception as e:
            errors.append(f"无法创建目录 {output_dir}：{e}")

    for label, date in [("start_date", start_date), ("end_date", end_date)]:
        if not is_valid_iso_date(date):
            errors.append(f"{label} 格式错误：'{date}' 应为 2022-08-01T00:00:00Z")

    return errors



    # 构造命令
    cmd = [
        "podaac-data-downloader",
        "-c", collection_shortname,
        "-d", output_directory,
        "--start-date", start_date,
        "--end-date", end_date,
    ]

    if bounding_box:
        if not re.match(r"^-?\d+(\.\d+)?,-?\d+(\.\d+)?,-?\d+(\.\d+)?,-?\d+(\.\d+)?$", bounding_box):
            print("bounding_box 格式不规范，应为 'lonW,latS,lonE,latN'")
        cmd.append(f"-b={bounding_box}")
    if extensions:
        cmd.extend(["--extensions", extensions])
    if file_name_filter:
        cmd.extend(["--file-name-filter", file_name_filter])
    if checksum:
        cmd.append("--checksum")
    if subset:
        cmd.append("--subset")
    if verbose:
        cmd.append("--verbose")
    if dry_run:
        cmd.append("--dry-run")
    if skip_if_exists:
        cmd.append("--skip-download-if-exists")
    if max_download_attempts:
        cmd.extend(["--max-download-attempts", str(max_download_attempts)])
    if manifest:
        cmd.extend(["--manifest", manifest])
    if timeout:
        cmd.extend(["--timeout", str(timeout)])
    if threads:
        cmd.extend(["--threads", str(threads)])

    print("即将执行命令：", " ".join(cmd))

    # 执行命令
    try:
        subprocess.run(cmd, check=True)
        print("数据下载完成。")
    except subprocess.CalledProcessError as e:
        print(f"下载命令执行失败：{e}")
    except FileNotFoundError:
        print("未找到 podaac-data-downloader，请确认已正确安装该工具。")
    except Exception as e:
        print(f"未知错误：{e}")

def download_swot_data(
    collection_shortname: str,
    output_directory: str,
    start_date: str,
    end_date: str,
    bounding_box: str = None,         # -b
    extensions: str = None,           # -e
    granule_name: str = None,         # -gr
    force: bool = False,              # -f
    verbose: bool = False,            # --verbose
    dry_run: bool = False,            # --dry-run
    limit: int = None,                # --limit
    process_cmd: str = None           # --process
):
    # 校验输入
    errors = validate_inputs(collection_shortname, output_directory, start_date, end_date)
    if errors:
        for err in errors:
            print(err)
        return

    cmd = [
        "podaac-data-downloader",
        "-c", collection_shortname,
        "-d", output_directory,
        "--start-date", start_date,
        "--end-date", end_date,
    ]

    # 添加可选参数
    if bounding_box:
        if not re.match(r"^-?\d+(\.\d+)?,-?\d+(\.\d+)?,-?\d+(\.\d+)?,-?\d+(\.\d+)?$", bounding_box):
            print("⚠️ bounding_box 格式应为 'lonW,latS,lonE,latN'")
        cmd.append(f"-b={bounding_box}")
    if extensions:
        cmd.extend(["-e", extensions])
    if granule_name:
        cmd.extend(["-gr", granule_name])
    if force:
        cmd.append("-f")
    if verbose:
        cmd.append("--verbose")
    if dry_run:
        cmd.append("--dry-run")
    if limit:
        cmd.extend(["--limit", str(limit)])
    if process_cmd:
        cmd.extend(["--process", process_cmd])

    # 打印命令用于调试
    print("即将执行命令：\n" + " ".join(cmd))

    try:
        subprocess.run(cmd, check=True)
        print("下载完成")
    except subprocess.CalledProcessError as e:
        print(f"下载失败：{e}")
    except FileNotFoundError:
        print("未找到 podaac-data-downloader，请确认已正确安装")
    except Exception as e:
        print(f"未知错误：{e}")

"""
-c COLLECTION, --collection-shortname：数据集 shortname，必填
    Water Mask Pixel Cloud NetCDF - SWOT_L2_HR_PIXC_D
    Water Mask Pixel Cloud Vector Attribute NetCDF - SWOT_L2_HR_PIXCVec_D
    River Vector Shapefile - SWOT_L2_HR_RiverSP_D
    Lake Vector Shapefile - SWOT_L2_HR_LakeSP_D
    Raster NetCDF - SWOT_L2_HR_Raster_D
"""

# 示例用法
if __name__ == "__main__":
    download_swot_data(
        collection_shortname="SWOT_L2_HR_LakeSP_D",
        output_directory=".\\data_downloads\\downloader_data\\",
        start_date="2025-05-27T00:00:00Z",
        end_date="2025-05-28T00:00:00Z",
        bounding_box="116,28.9,116.4,29.2",
        # extensions="nc",
        granule_name="*Obs_033_228*",
        # verbose=True,
        # dry_run=True,
        # force=True,
        # limit=10
    )




