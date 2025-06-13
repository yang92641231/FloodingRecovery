# -*- coding: utf-8 -*-
import os
import shutil
import logging
import multiprocessing as mp
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import arcpy
import pandas as pd



# --- 常量路径 ---
ROOT            = r"E:\ProcessData_CA"
RAW_VIIRS_ROOT  = r"F:\数据\Flood_lighting\California"
COUNTY_CSV      = r"E:\OneDrive - National University of Singapore\研二下\Flooding\ca_counties.csv"
COUNTY_SHP      = r"E:\National University of Singapore\Yang Yang - flooding\Raw Data\SHP\selectedCounty.shp"
TILE_SHP        = r"E:\National University of Singapore\Yang Yang - flooding\Raw Data\SHP\BlackMarbleTiles.shp"

FISHNET_DIR     = os.path.join(ROOT, "fishnet")
CSV_DIR         = os.path.join(ROOT, "lighting_csv")
PROGRESS_FILE   = os.path.join(ROOT, "progress.txt")
LOG_FILE        = os.path.join(ROOT, "process.log")

# --- 外部模块函数 ---
from part1_clip_h52tiff import viirs_hdf_to_clipped_tif
from part1_pixel_stats import (
    build_shp_grid,
    find_sample_raster,
    run_zonal_statistics,
    merge_dbf_tables,
)

# --- 日志 ---
os.makedirs(FISHNET_DIR, exist_ok=True)
os.makedirs(CSV_DIR,     exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8"
)

def load_done_set() -> set[str]:
    if not os.path.exists(PROGRESS_FILE):
        return set()
    with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f)

def mark_done(code: str) -> None:
    with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
        f.write(f"{code}\n")

def _convert_one_hdf(h5_path: Path, clip_dir: Path, county_shp: str, tid: str):
    """
    单个 HDF → GeoTIFF，供多进程调用
    """
    try:
        viirs_hdf_to_clipped_tif(
            input_folder  = h5_path.parent,   # 只扫这一张
            output_folder = str(clip_dir),
            cutline_shp   = county_shp,
            tile_filter   = tid,
            dst_nodata    = -9999,
            overwrite     = False
        )
        return h5_path.name
    except Exception as e:
        return f"ERR:{h5_path.name}:{e}"
    

def process_one_tile(tid: str, county_shp: str, sub_dir: Path):
    """
    把原来 for tid in tile_list: 里的 3.1~3.3 全放进来
    注意不要再扫描父目录；直接用 sub_dir / "clip"
    """
    clip_dir = sub_dir / "clip"
    dbf_dir  = sub_dir / "dbf"
    clip_dir.mkdir(parents=True, exist_ok=True)
    dbf_dir.mkdir(exist_ok=True)

    # ——▶ 3.1： *单进程* 跑整目录
    viirs_hdf_to_clipped_tif(
        input_folder  = RAW_VIIRS_ROOT,  # 让它自己扫所有 HDF
        output_folder = str(clip_dir),
        cutline_shp   = county_shp,
        tile_filter   = tid,             # 只过滤该 tile
        dst_nodata    = -9999,
        overwrite     = False
    )

    # ——▶ 3.2 之后保持原来的串行逻辑
    sample_raster = find_sample_raster(str(clip_dir))
    if sample_raster is None:
        logging.warning(f"[{tid}] no raster – skipped")
        return None      # 返回值让县级函数判断是否成功

    fishnet_tile = sub_dir / f"fishnet_{tid}.shp"
    build_shp_grid(sample_raster, str(fishnet_tile))

    dbf_dir = sub_dir / "dbf"
    dbf_dir.mkdir(exist_ok=True)
    run_zonal_statistics(str(clip_dir), str(fishnet_tile), str(dbf_dir))

    csv_tile = sub_dir / f"csv_{tid}.csv"
    merge_dbf_tables(str(dbf_dir), str(csv_tile), delete_dbf=True)

    return fishnet_tile, csv_tile

# --- 单县完整流程 ---
def export_county_polygon(code: str, out_shp: str) -> bool:


    original_code_for_log = code
    code5 = code.zfill(5)  # 确保 5 位

    # —— 清理潜在的 ArcPy 缓存 —— 
    try:
        arcpy.ClearWorkspaceCache_management()
        logging.info(f"[{original_code_for_log}] Cleared workspace cache at start.")
    except Exception as e_cache:
        logging.warning(f"[{original_code_for_log}] Failed to clear cache: {e_cache}")

    # —— 确认字段 CTFIPS以及空间参考 —— 
    try:
        fields = arcpy.ListFields(COUNTY_SHP, "CTFIPS")
        if not fields:
            logging.error(f"[{original_code_for_log}] Field 'CTFIPS' 不存在于 {COUNTY_SHP} 中。")
            return False
        desc = arcpy.Describe(COUNTY_SHP)
        spatial_ref = desc.spatialReference
    except Exception as e_desc:
        logging.error(f"[{original_code_for_log}] Describe/字段检测失败: {e_desc}")
        return False

    # —— 用 SearchCursor 把所有 CTFIPS = 'code5' 的几何读出来 —— 
    matching_geoms = []
    where_clause = f"CTFIPS = '{code5}'"

    try:
        with arcpy.da.SearchCursor(COUNTY_SHP, ["CTFIPS", "SHAPE@"], where_clause=where_clause) as cursor:
            for row in cursor:
                # 这里再做一次“保险”：即便 DBF 里存的是 '06079'、'6079' 或其它格式，
                # 也按字符串 zfill 比对一遍，确保只能拿 code5 对应那条“本州/本县”。
                if str(row[0]).zfill(5) == code5:
                    matching_geoms.append(row[1])
    except Exception as e_sc:
        logging.error(f"[{original_code_for_log}] 用 SearchCursor 检索失败: {e_sc}")
        return False

    if not matching_geoms:
        logging.warning(f"[{original_code_for_log}] 在 {COUNTY_SHP} 中找不到 CTFIPS = '{code5}' 的要素。")
        return False

    # —— 如果同名的 out_shp 已存在 —— 
    if arcpy.Exists(out_shp):
        try:
            arcpy.Delete_management(out_shp)
        except Exception as e_del:
            logging.warning(f"[{original_code_for_log}] 删除已存在的 {out_shp} 失败: {e_del}")

    # —— CreateFeatureclass_management 新建一个空要素类 —— 
    out_dir  = os.path.dirname(out_shp)
    out_name = os.path.basename(out_shp)
    try:
        arcpy.CreateFeatureclass_management(
            out_path=out_dir,
            out_name=out_name,
            geometry_type="POLYGON",
            spatial_reference=spatial_ref
        )
    except Exception as e_cc:
        logging.error(f"[{original_code_for_log}] CreateFeatureclass_management 失败: {e_cc}")
        return False

    # —— InsertCursor 把 matching_geoms的几何插入到 out_shp —— 
    try:
        with arcpy.da.InsertCursor(out_shp, ["SHAPE@"]) as ic:
            for geom in matching_geoms:
                ic.insertRow([geom])
    except Exception as e_ic:
        logging.error(f"[{original_code_for_log}] InsertCursor 写要素失败: {e_ic}")
        # 如果写入出错，也删掉 out_shp，不留空图层
        if arcpy.Exists(out_shp):
            try:
                arcpy.Delete_management(out_shp)
            except:
                pass
        return False

    logging.info(f"[{original_code_for_log}] 成功生成 {out_shp}，共 {len(matching_geoms)} 个要素。")

    # —— 再次清理缓存 —— 
    try:
        arcpy.ClearWorkspaceCache_management()
    except:
        pass

    return True



def find_tile_ids(county_shp: str) -> list[str]:

    import arcpy
    from pathlib import Path

    original = Path(county_shp).stem

    # —— 验证 county_shp 是否存在 —— 
    try:
        desc_county = arcpy.Describe(county_shp)
        sr_county   = desc_county.spatialReference
        ext_c       = desc_county.extent
        logging.info(f"[{original}] County describe: SR={sr_county.name}, Extent=({ext_c.XMin:.6f}, {ext_c.YMin:.6f}, {ext_c.XMax:.6f}, {ext_c.YMax:.6f})")
    except Exception as e_desc:
        logging.error(f"[{original}] Describe({county_shp}) 失败: {e_desc}")
        return []

    # —— 读取 county_shp 内所有几何 —— 
    county_geoms = []
    try:
        with arcpy.da.SearchCursor(county_shp, ["SHAPE@"]) as cursor:
            for row in cursor:
                geom = row[0]
                if geom is not None:
                    county_geoms.append(geom)
    except Exception as e_sc:
        logging.error(f"[{original}] 在 {county_shp} 中读取几何失败: {e_sc}")
        return []

    if not county_geoms:
        logging.warning(f"[{original}] {county_shp} 中没有任何几何要素。")
        return []

    # —— 检查几何交集 —— 
    tile_ids = set()
    try:
        with arcpy.da.SearchCursor(TILE_SHP, ["TileID", "SHAPE@"]) as t_cursor:
            for t_row in t_cursor:
                tile_id   = t_row[0]
                tile_geom = t_row[1]
                if tile_geom is None:
                    continue
                # 对每个 county_geom 做 intersect
                for county_geom in county_geoms:
                    try:
                        inter = county_geom.intersect(tile_geom, 4)  # 4 = esriGeometryPolygon
                        if inter is not None and inter.area > 0:
                            tile_ids.add(tile_id)
                            break  
                    except Exception as e_int:
                        logging.warning(f"[{original}] County-Geom.intersect(TileID={tile_id}) 异常: {e_int}")
                        continue
    except Exception as e_tcur:
        logging.error(f"[{original}] 遍历瓦片 {TILE_SHP} 失败: {e_tcur}")
        return []

    if not tile_ids:
        logging.warning(f"[{original}] 经过几何运算，没有找到任何相交瓦片 (No tile found).")
    else:
        sorted_ids = sorted(tile_ids)
        logging.info(f"[{original}] 通过几何运算找到瓦片: {sorted_ids}")
        return sorted_ids

    return []



def process_one_county(code: str) -> None:
    tmp_dir       = Path(ROOT, code)
    fishnet_final = Path(FISHNET_DIR, f"{code}.shp")
    csv_final     = Path(CSV_DIR,     f"{code}.csv")

    try:
        tmp_dir.mkdir(exist_ok=True)

        county_shp = tmp_dir / f"{code}.shp"
        if not export_county_polygon(code, str(county_shp)):
            return

        tile_ids = find_tile_ids(str(county_shp))
        if not tile_ids:
            logging.warning(f"[{code}] No tile found – skipped")
            return
        logging.info(f"[{code}] Tiles: {tile_ids}")

        tile_fishnets = []
        tile_csvs     = []

        tile_list = tile_ids         

        for tid in tile_list:
            logging.info(f"[{code}] >>> Tile {tid} start")
            sub_dir  = tmp_dir / f"tile_{tid}"
            clip_dir = sub_dir / "clip"
            dbf_dir  = sub_dir / "dbf"
            clip_dir.mkdir(parents=True, exist_ok=True)
            dbf_dir.mkdir(exist_ok=True)

            # ---------- 3.1 并行裁剪 ----------
            # ① 列出所有 hdf 文件（按你的 RAW_VIIRS_ROOT 规则来）
            h5_paths = sorted(Path(RAW_VIIRS_ROOT).rglob(f"*{tid}*.h*"))

            # ② 已有 tif 就跳过
            existing_tif = any((clip_dir / f"{p.stem}_clip_filtered.tif").exists()
                               for p in h5_paths)
            if existing_tif:
                logging.info(f"[{tid}] 检测到已有 tif，跳过转换")
            else:
                logging.info(f"[{tid}] {len(h5_paths)} HDF 待转换（多进程…）")

                with ProcessPoolExecutor(max_workers=4) as ex:
                    futures = {}
                    for tid in tile_ids:
                        sub_dir = tmp_dir / f"tile_{tid}"
                        fut = ex.submit(process_one_tile, tid, str(county_shp), sub_dir)
                        futures[fut] = tid

                    for fut in as_completed(futures):
                        tid = futures[fut]
                        try:
                            res = fut.result()
                            if res is None:
                                logging.warning(f"[{code}] Tile {tid} skipped")
                                continue
                            fishnet_tile, csv_tile = res
                            tile_fishnets.append(str(fishnet_tile))
                            tile_csvs.append(str(csv_tile))
                        except Exception as e:
                            logging.exception(f"[{code}] Tile {tid} failed: {e}")

            # ---------- 3.2 以后维持串行 ----------
            sample_raster = find_sample_raster(str(clip_dir))
            if sample_raster is None:
                logging.warning(f"[{code}] Tile {tid} produced no raster – skipped")
                continue

            fishnet_tile = sub_dir / f"fishnet_{tid}.shp"
            build_shp_grid(sample_raster, str(fishnet_tile))

            dbf_dir = sub_dir / "dbf"         # 别忘了先定义
            dbf_dir.mkdir(exist_ok=True)

            run_zonal_statistics(str(clip_dir), str(fishnet_tile), str(dbf_dir))

            csv_tile = sub_dir / f"csv_{tid}.csv"
            merge_dbf_tables(str(dbf_dir), str(csv_tile), delete_dbf=True)

            tile_fishnets.append(str(fishnet_tile))
            tile_csvs.append(str(csv_tile))


        if not tile_fishnets:
            logging.warning(f"[{code}] No tile succeeded – skipped")
            return

        # 合并所有 tile 的 fishnet，生成最终 county fishnet
        if fishnet_final.exists():
            arcpy.Delete_management(str(fishnet_final))
        if len(tile_fishnets) == 1:
            arcpy.management.CopyFeatures(tile_fishnets[0], str(fishnet_final))
        else:
            arcpy.management.Merge(tile_fishnets, str(fishnet_final))

        # 合并所有 tile 的 CSV，生成最终 county CSV
        dfs = []
        for csvp in tile_csvs:
            df  = pd.read_csv(csvp, dtype={"xy_id": str})
            dfs.append(df)

        final_df = pd.concat(dfs, axis=0, ignore_index=True)
        final_df.to_csv(csv_final, index=False, encoding="utf-8-sig")

        logging.info(f"[{code}] Completed successfully with {len(tile_fishnets)} tiles")
        mark_done(code)

    except Exception as e:
        logging.exception(f"[{code}] FAILED: {e}")

    finally:
        try:
            arcpy.ClearWorkspaceCache_management()
        except Exception:
            pass

        try:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)
                logging.info(f"[{code}] Temp folder deleted.")
        except Exception as cleanup_err:
            logging.warning(f"[{code}] !! Failed to delete temp folder: {cleanup_err}")

def main() -> None:
    mp.freeze_support()


    all_ctfips = set()
    with arcpy.da.SearchCursor(COUNTY_SHP, ["CTFIPS"]) as cursor:
        for row in cursor:
            all_ctfips.add(row[0])

    df = pd.read_csv(COUNTY_CSV, dtype=str)
    df["countyCode"] = df["countyCode"].str.strip().str.zfill(5)
    all_from_csv = set(df["countyCode"]) 
    to_keep = sorted(all_from_csv & all_ctfips)
    to_skip = sorted(all_from_csv - all_ctfips)

    if to_skip:
        for code in to_skip:
            logging.warning(f"[{code}] 不在 {COUNTY_SHP} 中，跳过。")

    # 剔除已经做过的 
    done_set = load_done_set()
    pending  = [c for c in to_keep if c not in done_set]

    logging.info(f"总共有 {len(all_from_csv)} 个县；shapefile 可匹配 {len(to_keep)}；已完成 {len(done_set)}，剩余 {len(pending)}")

    for code in pending:
        logging.info(f"[{code}] === Start ===")
        process_one_county(code)

if __name__ == "__main__":
    main()