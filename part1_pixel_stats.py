# -*- coding: utf-8 -*-
import os
import re
from pathlib import Path
import pandas as pd
from typing import List, Tuple, Optional
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import freeze_support
import pyarrow as pa
import pyarrow.parquet as pq
import gc

import arcpy


# ------------------------- 通用工具 ------------------------- #
def sanitize_name(raw_name: str) -> str:
    # 简化文件名
    m = re.search(r"\.A(\d{7})\.", raw_name)
    date7 = m.group(1) if m else re.sub(r'[^0-9]', '', raw_name)[:7].ljust(7, '_')
    return f"d{date7}.dbf"


def _add_xy_id_field(fishnet_shp: str) -> None:
    # 给 fishnet 添加 xy_id = W/E + lon + N/S + lat
    fields = [f.name for f in arcpy.ListFields(fishnet_shp)]
    if "xy_id" in fields:
        return

    arcpy.management.AddField(fishnet_shp, "xy_id", "TEXT", field_length=50)
    with arcpy.da.UpdateCursor(fishnet_shp, ["SHAPE@XY", "xy_id"]) as cur:
        for row in cur:
            x, y = row[0]
            xy_id = f"{'W' if x<0 else 'E'}{abs(x):.5f}{'S' if y<0 else 'N'}{abs(y):.5f}"
            row[1] = xy_id
            cur.updateRow(row)


def _flush_chunk(chunk: list[pd.DataFrame], accumulated: Optional[pd.DataFrame]) -> pd.DataFrame:
    """把当前 chunk DataFrame 列表 concat → 解析日期 → pivot → 与累计宽表 outer merge"""
    merged_long = pd.concat(chunk, ignore_index=True)

    # 解析日期列
    merged_long["year"]        = merged_long["filename"].str[1:5]
    merged_long["day_of_year"] = merged_long["filename"].str[5:8].astype(int)
    merged_long["date"]        = pd.to_datetime(
        merged_long["year"] + merged_long["day_of_year"].astype(str),
        format="%Y%j"
    )
    merged_long.drop(columns=["year", "day_of_year"], inplace=True)

    # 宽表
    wide_part = (
        merged_long
        .pivot(index="xy_id", columns="date", values="MEAN")
        .reset_index()
    )

    # 第一次 flush
    if accumulated is None:
        return wide_part

    # 后续 flush：按 xy_id 外连接，把新出现的日期列 append 上
    accumulated = accumulated.merge(wide_part, on="xy_id", how="outer", copy=False)
    return accumulated

def _cleanup_dbf(dbf_folder: str, dbf_files: list[str]) -> None:
    suffixes = (".dbf", ".cpg", ".dbf.xml")
    for dbf in dbf_files:
        stem = os.path.splitext(dbf)[0]
        for suf in suffixes:
            p = os.path.join(dbf_folder, stem + suf)
            if os.path.exists(p):
                try:
                    os.remove(p)
                    print(f"已删除：{p}")
                except Exception as e:
                    print(f"无法删除 {p} → {e}")
    print("✓ DBF 清理完毕")

def find_sample_raster(raster_folder_path):
        # 获取文件夹下的所有文件，并筛选出以常见栅格格式结尾的文件，如 .tif
    raster_files = [f for f in os.listdir(raster_folder_path) if f.endswith(('.tif', '.tiff'))]

    # 检查是否有找到栅格文件
    if not raster_files:
        raise FileNotFoundError("未在指定文件夹中找到.tif或.tiff格式的栅格文件。")

    # 选取第一张作为 sample_raster
    sample_raster_path = os.path.join(raster_folder_path, raster_files[0])

    return sample_raster_path

# ----------------------- ① 生成鱼网 ------------------------ #
def build_shp_grid(
    sample_raster: str,
    out_shp: str,
    zone_field: str = "index",
    overwrite: bool = False
) -> str:

    out_shp = str(out_shp)
    if os.path.exists(out_shp) and not overwrite:
        print(f"fishnet 已存在：{out_shp}")
        _add_xy_id_field(out_shp)          # 确保 xy_id 字段存在
        return out_shp

    print(f"▶ 生成鱼网 {out_shp}")
    ras = arcpy.Raster(sample_raster)
    arcpy.env.snapRaster  = ras
    arcpy.env.extent      = ras.extent
    arcpy.env.outputCoordinateSystem = ras.spatialReference

    cell_w, cell_h = ras.meanCellWidth, ras.meanCellHeight
    xmin, ymin, xmax, ymax = ras.extent.XMin, ras.extent.YMin, ras.extent.XMax, ras.extent.YMax

    arcpy.management.CreateFishnet(
        out_feature_class = out_shp,
        origin_coord      = f"{xmin} {ymin}",
        y_axis_coord      = f"{xmin} {ymin+1}",
        cell_width        = cell_w,
        cell_height       = cell_h,
        number_rows       = "0",
        number_columns    = "0",
        corner_coord      = f"{xmax} {ymax}",
        labels            = "NO_LABELS",
        template          = sample_raster,
        geometry_type     = "POLYGON"
    )

    arcpy.management.AddField(out_shp, zone_field, "LONG")
    arcpy.management.CalculateField(out_shp, zone_field, "!FID!", "PYTHON3")

    _add_xy_id_field(out_shp)
    print("✓ fishnet 创建完成")
    return out_shp



# ---------------------- ② Zonal Statistics ----------------- #
def _zonal_one(
    tif_path: str,
    fishnet_shp: str,
    output_folder: str
) -> str:
    arcpy.CheckOutExtension("Spatial")
    fname = os.path.basename(tif_path)
    table_name = sanitize_name(fname.replace("_clip_filtered.tif", ""))
    out_table  = os.path.join(output_folder, table_name)

    if os.path.exists(out_table):
        return f"已存在：{table_name}"

    try:
        arcpy.sa.ZonalStatisticsAsTable(
            in_zone_data   = fishnet_shp,
            zone_field     = "xy_id",
            in_value_raster= tif_path,
            out_table      = out_table,
            ignore_nodata  = "DATA",
            statistics_type= "MEAN"
        )
        return f"完成：{table_name}"
    except Exception as e:
        return f"失败：{table_name} → {e}"


def run_zonal_statistics(
    raster_folder: str,
    fishnet_shp: str,
    output_folder: str,
    *,
    workers: int = 4        # ← 默认仍然 4；在子进程里传 1 可禁用并行
) -> None:

    # --- Spatial 扩展 ---
    if arcpy.CheckExtension("Spatial") != "Available":
        raise RuntimeError("Spatial Analyst 扩展不可用")
    arcpy.CheckOutExtension("Spatial")

    os.makedirs(output_folder, exist_ok=True)
    tif_list = [
        str(Path(raster_folder) / f)
        for f in os.listdir(raster_folder)
        if f.endswith("_clip_filtered.tif")
    ]

    if not tif_list:
        print("未找到 *_clip_filtered.tif")
        return

    print(f"开始 ZonalStatistics 共 {len(tif_list)} 幅")

    # ---------- 并行 or 串行 ----------
    if workers and workers > 1 and len(tif_list) > 1:
        with ProcessPoolExecutor(max_workers=workers) as exe:
            for res in exe.map(
                _zonal_one,
                tif_list,
                [fishnet_shp] * len(tif_list),
                [output_folder] * len(tif_list)
            ):
                print(res)
    else:
        for tif in tif_list:
            res = _zonal_one(tif, fishnet_shp, output_folder)
            print(res)

    print("全部 Zonal 处理完成")


def merge_dbf_tables(
    dbf_folder: str,
    output_csv: str,
    *,
    chunk_size: int = 200,          # ← 每 N 个 .dbf 做一次 flush
    delete_dbf: bool = False
) -> None:

    dbf_files = [f for f in os.listdir(dbf_folder) if f.lower().endswith(".dbf")]
    if not dbf_files:
        print("⚠ 未找到 .dbf 文件，跳过合并")
        return

    wide_df: Optional[pd.DataFrame] = None      # 累计的宽表
    chunk: list[pd.DataFrame] = []              # 当前待处理的小批次

    for idx, dbf in enumerate(dbf_files, 1):
        dbf_path = os.path.join(dbf_folder, dbf)
        try:
            arr = arcpy.da.TableToNumPyArray(dbf_path, "*")
            df  = pd.DataFrame(arr)
            df["filename"] = os.path.splitext(dbf)[0]
            chunk.append(df)
            print(f"已读取 {idx}/{len(dbf_files)} → {dbf}")
        except Exception as e:
            print(f"跳过：{dbf} → {e}")

        # 满一批就 flush
        if len(chunk) == chunk_size:
            wide_df = _flush_chunk(chunk, wide_df)    # ← 见下方私有函数
            chunk.clear(); gc.collect()

    # 处理最后不足一批的残留
    if chunk:
        wide_df = _flush_chunk(chunk, wide_df)
        chunk.clear(); gc.collect()

    if wide_df is None or wide_df.empty:
        print("⚠ 合并结果为空，终止")
        return

    # ---------- 导出 CSV ----------
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    wide_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"✓ 已导出 CSV：{output_csv}")

    # ---------- 可选删除源文件 ----------
    if delete_dbf:
        _cleanup_dbf(dbf_folder, dbf_files)