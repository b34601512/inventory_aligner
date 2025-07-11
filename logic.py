"""
表格处理逻辑模块
实现销售出库单和即时库存表的同步功能
"""

import pandas as pd
import re
from typing import Dict, List, Tuple, Optional, Callable
from utils import load_excel_file, save_excel_file, clean_dataframe, get_column_by_name, create_backup_file
from openpyxl import load_workbook
from openpyxl.styles import PatternFill


class StockSyncProcessor:
    """库存同步处理器"""
    
    def __init__(self, progress_callback: Optional[Callable] = None):
        self.progress_callback = progress_callback
        self.sales_df = None
        self.stock_df = None
        self.material_mapping = {}  # 物料编码映射 {旧料号: 新料号}
        self.modified_cells = []  # 记录修改的单元格位置
        self.mapping_config_file = 'material_mapping.json'
        
        # 自动加载保存的映射配置
        self._load_mapping_config()
        
    def set_progress_callback(self, callback: Callable):
        """设置进度回调函数"""
        self.progress_callback = callback
        
    def _update_progress(self, message: str):
        """更新进度"""
        if self.progress_callback:
            self.progress_callback(message)
    
    def load_sales_file(self, file_path: str) -> str:
        """
        加载销售出库单文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            错误信息，空字符串表示成功
        """
        try:
            self._update_progress("正在加载销售出库单...")
            df, error = load_excel_file(file_path)
            
            if error:
                return error
            
            # 不进行数据清理，保持原始结构（包括空行）
            self.sales_df = df
            self.sales_file_path = file_path
            
            # 验证必要的列是否存在（按列位置检查）
            required_columns = {
                'DZ': 129,  # 第130列 - 物料编码
                'EC': 132,  # 第133列 - 辅助属性1
                'ED': 133,  # 第134列 - 辅助属性2
                'FF': 161,  # 第162列 - 批号#主档
                'FG': 162,  # 第163列 - 批号#手工
                'GJ': 191,  # 第192列 - 仓库名称
                'HA': 208   # 第209列 - 销售数量
            }
            
            max_col_needed = max(required_columns.values())
            if len(self.sales_df.columns) <= max_col_needed:
                return f"销售出库单列数不足，需要至少 {max_col_needed + 1} 列，实际只有 {len(self.sales_df.columns)} 列"
            
            # 为方便后续处理，给需要的列添加别名
            for alias, col_idx in required_columns.items():
                if col_idx < len(self.sales_df.columns):
                    self.sales_df[alias] = self.sales_df.iloc[:, col_idx]
            
            self._update_progress(f"销售出库单加载成功，共 {len(self.sales_df)} 行数据")
            return ""
            
        except Exception as e:
            return f"加载销售出库单失败: {str(e)}"
    
    def load_stock_file(self, file_path: str) -> str:
        """
        加载即时库存表文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            错误信息，空字符串表示成功
        """
        try:
            self._update_progress("正在加载即时库存表...")
            df, error = load_excel_file(file_path)
            
            if error:
                return error
            
            # 不进行数据清理，保持原始结构（包括空行）
            self.stock_df = df
            
            # 验证必要的列是否存在（按列位置检查）
            required_columns = {
                'A': 0,   # 第1列 - 物料编码
                'G': 6,   # 第7列 - 仓库名称
                'H': 7,   # 第8列 - 批号
                'K': 10,  # 第11列 - 库存数量
                'D': 3,   # 第4列 - 辅助属性
                'E': 4,   # 第5列 - 辅助属性
                'F': 5    # 第6列 - 辅助属性
            }
            
            max_col_needed = max(required_columns.values())
            if len(self.stock_df.columns) <= max_col_needed:
                return f"即时库存表列数不足，需要至少 {max_col_needed + 1} 列，实际只有 {len(self.stock_df.columns)} 列"
            
            # 为方便后续处理，给需要的列添加别名
            for alias, col_idx in required_columns.items():
                if col_idx < len(self.stock_df.columns):
                    self.stock_df[alias] = self.stock_df.iloc[:, col_idx]
            
            self._update_progress(f"即时库存表加载成功，共 {len(self.stock_df)} 行数据")
            return ""
            
        except Exception as e:
            return f"加载即时库存表失败: {str(e)}"
    
    def set_material_mapping(self, old_code: str, new_code: str) -> str:
        """
        设置物料编码映射
        
        Args:
            old_code: 旧料号
            new_code: 新料号
            
        Returns:
            错误信息，空字符串表示成功
        """
        if not old_code or not new_code:
            return "物料编码不能为空"



        # 简单验证编码格式
        if not self._validate_material_code(old_code) or not self._validate_material_code(new_code):
            return "物料编码格式不正确，应为 x.xx.x.xx.xx.xxx 格式"

        self.material_mapping[old_code] = new_code
        # 保存映射配置到文件
        self._save_mapping_config()
        return ""
    
    def set_material_mappings(self, mappings: List[tuple]) -> str:
        """
        批量设置物料编码映射
        
        Args:
            mappings: 映射列表，每个元素为 (old_code, new_code) 元组
            
        Returns:
            错误信息，空字符串表示成功
        """
        if not mappings:
            return "映射列表不能为空"
        
        errors = []
        for old_code, new_code in mappings:
            if not old_code or not new_code:
                errors.append(f"物料编码不能为空: {old_code} -> {new_code}")
                continue
                
            # 简单验证编码格式
            if not self._validate_material_code(old_code) or not self._validate_material_code(new_code):
                errors.append(f"物料编码格式不正确: {old_code} -> {new_code}")
                continue
            


            self.material_mapping[old_code] = new_code
        
        if errors:
            return "; ".join(errors)
        
        # 保存映射配置到文件
        self._save_mapping_config()
        return ""
    
    def clear_material_mappings(self):
        """清空所有物料编码映射"""
        self.material_mapping = {}
        self._save_mapping_config()
    
    def get_material_mappings(self) -> dict:
        """获取当前的物料编码映射"""
        return self.material_mapping.copy()
    
    def _load_mapping_config(self):
        """从JSON文件加载映射配置"""
        try:
            import json
            import os
            
            if os.path.exists(self.mapping_config_file):
                with open(self.mapping_config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 对键和值进行规范化，避免因空白字符导致匹配失败
                    self.material_mapping = {
                        self._normalize_material_code(k): self._normalize_material_code(v)
                        for k, v in data.items()
                    }
                    self._update_progress(f"已加载 {len(self.material_mapping)} 个物料编码映射")
        except Exception as e:
            # 如果加载失败，保持空映射
            self.material_mapping = {}
            self._update_progress(f"加载映射配置失败: {e}")
    
    def _save_mapping_config(self):
        """保存映射配置到JSON文件"""
        try:
            import json

            with open(self.mapping_config_file, 'w', encoding='utf-8') as f:
                json.dump(self.material_mapping, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self._update_progress(f"保存映射配置失败: {e}")

    def _normalize_material_code(self, code: str) -> str:
        """规范化物料编码，去除空白字符"""
        if code is None:
            return ""
        return re.sub(r"\s+", "", str(code)).strip()

    def _validate_material_code(self, code: str) -> bool:
        """验证物料编码格式"""
        if not code or not isinstance(code, str):
            return False
        
        parts = code.split('.')
        if len(parts) != 6:
            return False
        
        for part in parts:
            if not part.isdigit():
                return False
        
        return True
    
    def process_synchronization(self) -> str:
        """
        执行同步处理
        
        Returns:
            错误信息，空字符串表示成功
        """
        try:
            if self.sales_df is None:
                return "请先加载销售出库单"
            
            if self.stock_df is None:
                return "请先加载即时库存表"
            
            if not self.material_mapping:
                return "请先设置物料编码映射"
            
            # 开始处理
            self._update_progress("开始处理数据同步...")
            
            # 1. 替换物料编码
            self._replace_material_codes()

            # 2. 同步辅助属性
            self._sync_auxiliary_attributes()

            # 3. 同步批次号
            self._sync_batch_numbers()
            
            # 3. 保存文件并高亮修改内容
            self._save_with_highlights()
            
            self._update_progress("数据同步完成！")
            return ""
            
        except Exception as e:
            return f"同步处理失败: {str(e)}"
    
    def _replace_material_codes(self):
        """根据映射表批量替换物料编码并记录详细日志"""
        self._update_progress("开始替换物料编码（字符串匹配）...")

        success_total = 0
        failed_total = 0

        # 记录失败原因，便于排查
        fail_reasons = []

        for old_code, new_code in self.material_mapping.items():
            self._update_progress(
                f"现在开始使用字符串替换旧料号 {old_code} 成新料号 {new_code}")
            old_norm = self._normalize_material_code(old_code)
            new_norm = self._normalize_material_code(new_code)

            if not old_norm or old_norm == new_norm:
                self._update_progress(
                    f"跳过 {old_code} -> {new_code}: 新旧料号相同或为空")
                failed_total += 1
                continue

            # 统计替换前数量
            before_count = len(
                self.sales_df[(self.sales_df['DZ'].astype(str).apply(self._normalize_material_code) == old_norm)
                               & (self.sales_df.index >= 2)])

            indices = self.sales_df[
                (self.sales_df['DZ'].astype(str).apply(self._normalize_material_code) == old_norm)
                & (self.sales_df.index >= 2)
            ].index

            if len(indices) == 0:
                msg = f"未找到需要替换的物料编码 {old_code}"
                self._update_progress(msg)
                fail_reasons.append(msg)
                failed_total += 1
                continue

            self.sales_df.loc[indices, 'DZ'] = new_norm
            col_idx = 129  # DZ 列实际位置

            if col_idx < len(self.sales_df.columns):
                self.sales_df.iloc[indices, col_idx] = new_norm

            for i in indices:
                self.modified_cells.append((i, col_idx))

            after_count = len(
                self.sales_df[(self.sales_df['DZ'].astype(str).apply(self._normalize_material_code) == new_norm)
                               & (self.sales_df.index >= 2)])

            success_total += len(indices)
            self._update_progress(
                f"已将 {old_code} 替换为 {new_code}，共 {len(indices)} 行，替换前 {before_count} 行，替换后 {after_count} 行")

        self._update_progress(
            f"物料编码替换完成，成功 {success_total} 行，失败 {failed_total} 行")

        if fail_reasons:
            for reason in fail_reasons:
                self._update_progress(f"失败原因: {reason}")

    def _sync_auxiliary_attributes(self):
        """同步销售出库单与即时库存表中的辅助属性"""
        self._update_progress("现在开始操作销售出库表中辅助属性...")

        warehouse_list = []
        for w in self.sales_df['GJ'].iloc[2:]:
            if pd.notna(w) and w not in warehouse_list:
                warehouse_list.append(w)

        for old_code, new_code in self.material_mapping.items():
            new_norm = self._normalize_material_code(new_code)
            for idx, warehouse in enumerate(warehouse_list, start=1):
                sales_rows = self.sales_df[
                    (self.sales_df.index >= 2) &
                    (self.sales_df['DZ'].apply(self._normalize_material_code) == new_norm) &
                    (self.sales_df['GJ'].astype(str).str.strip() == str(warehouse).strip())
                ]
                self._update_progress(
                    f"现在筛选出物料号 {new_norm}，仓库 {warehouse}，找到 {len(sales_rows)} 行")
                if sales_rows.empty:
                    self._update_progress(f"销售出库表未找到记录，跳过 {new_norm} {warehouse}")
                    continue

                stock_rows = self.stock_df[
                    (self.stock_df.index >= 1) &
                    (self.stock_df['A'].apply(self._normalize_material_code) == new_norm) &
                    (self.stock_df['G'].astype(str).str.strip() == str(warehouse).strip())
                ]

                self._update_progress(
                    f"即时库存表筛选结果 {len(stock_rows)} 行")

                if stock_rows.empty:
                    self._update_progress(f"未找到库存记录，跳过 {new_norm} {warehouse}")
                    continue

                stock_row = stock_rows.sort_values(by='K', ascending=False).iloc[0]
                aux_e = stock_row['E']
                aux_f = stock_row['F']

                indices = sales_rows.index
                self.sales_df.loc[indices, 'EC'] = aux_e
                self.sales_df.loc[indices, 'ED'] = aux_f

                ec_idx, ed_idx = 132, 133
                if ec_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[indices, ec_idx] = aux_e
                if ed_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[indices, ed_idx] = aux_f

                for i in indices:
                    self.modified_cells.extend([
                        (i, ec_idx),
                        (i, ed_idx)
                    ])

                self._update_progress(
                    f"现在对销售出库表进行操作: EC列修改 {len(indices)} 行, ED列修改 {len(indices)} 行")

    def _sync_batch_numbers(self):
        """同步销售出库单中的批次号"""
        self._update_progress("现在开始修改销售出库表中批次号...")

        warehouse_list = []
        for w in self.sales_df['GJ'].iloc[2:]:
            if pd.notna(w) and w not in warehouse_list:
                warehouse_list.append(w)

        for old_code, new_code in self.material_mapping.items():
            new_norm = self._normalize_material_code(new_code)
            for idx, warehouse in enumerate(warehouse_list, start=1):
                stock_rows = self.stock_df[
                    (self.stock_df.index >= 1) &
                    (self.stock_df['A'].apply(self._normalize_material_code) == new_norm) &
                    (self.stock_df['G'].astype(str).str.strip() == str(warehouse).strip())
                ]

                self._update_progress(
                    f"现在筛选即时库存表中 {new_norm}+{warehouse}，找到 {len(stock_rows)} 行")

                if stock_rows.empty:
                    self._update_progress(
                        f"未找到库存记录，跳过 {new_norm} {warehouse}")
                    continue

                stock_row = stock_rows.sort_values(by='K', ascending=False).iloc[0]
                batch = stock_row['H']
                self._update_progress(
                    f"取K列最大值所在行批号 {batch}")

                sales_rows = self.sales_df[
                    (self.sales_df.index >= 2) &
                    (self.sales_df['DZ'].apply(self._normalize_material_code) == new_norm) &
                    (self.sales_df['GJ'].astype(str).str.strip() == str(warehouse).strip())
                ]

                self._update_progress(
                    f"现在筛选销售出库表中 {new_norm}+{warehouse}，找到 {len(sales_rows)} 行")

                if sales_rows.empty:
                    continue

                indices = sales_rows.index
                self.sales_df.loc[indices, 'FF'] = batch
                self.sales_df.loc[indices, 'FG'] = batch

                ff_idx, fg_idx = 161, 162
                if ff_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[indices, ff_idx] = batch
                if fg_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[indices, fg_idx] = batch

                for i in indices:
                    self.modified_cells.extend([
                        (i, ff_idx),
                        (i, fg_idx)
                    ])

                self._update_progress(
                    f"现在修改销售出库表筛选结果中的批次号，已修改 {len(indices)} 行，批次号等于 {batch}")

    def _synchronize_by_flow(self):
        """按照给定流程同步批次号和辅助属性"""
        self._update_progress("正在同步批次和辅助属性...")

        mappings = list(self.material_mapping.items())
        total = len(mappings)
        n = 1
        processed_codes = set()

        # 收集销售出库单中出现的仓库顺序列表（跳过前两行）
        warehouse_list = []
        for w in self.sales_df['GJ'].iloc[2:]:
            if pd.notna(w) and w not in warehouse_list:
                warehouse_list.append(w)

        for old_code, new_code in mappings:
            new_norm = self._normalize_material_code(new_code)

            if new_norm in processed_codes:
                n += 1
                continue

            processed_codes.add(new_norm)
            self._update_progress(
                f"处理料号 {n}/{total}: {old_code} -> {new_code}")

            for warehouse in warehouse_list:
                sales_rows = self.sales_df[
                    (self.sales_df.index >= 2) &
                    (self.sales_df['DZ'].apply(self._normalize_material_code) == new_norm) &
                    (self.sales_df['GJ'].astype(str).str.strip() == str(warehouse).strip())
                ]
                if sales_rows.empty:
                    continue

                stock_rows = self.stock_df[
                    (self.stock_df.index >= 1) &
                    (self.stock_df['A'].apply(self._normalize_material_code) == new_norm) &
                    (self.stock_df['G'].astype(str).str.strip() == str(warehouse).strip())
                ]

                if stock_rows.empty:
                    self._update_progress(
                        f"警告: 库存表中没有找到仓库 {warehouse} 的料号 {new_code}")
                    continue

                stock_row = stock_rows.sort_values(by='K', ascending=False).iloc[0]
                batch = stock_row['H']
                aux_e = stock_row['E']
                aux_f = stock_row['F']

                indices = sales_rows.index

                self.sales_df.loc[indices, 'FF'] = batch
                self.sales_df.loc[indices, 'FG'] = batch
                self.sales_df.loc[indices, 'EC'] = aux_e
                self.sales_df.loc[indices, 'ED'] = aux_f

                ff_idx, fg_idx, ec_idx, ed_idx = 161, 162, 132, 133
                if ff_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[indices, ff_idx] = batch
                if fg_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[indices, fg_idx] = batch
                if ec_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[indices, ec_idx] = aux_e
                if ed_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[indices, ed_idx] = aux_f

                for idx in indices:
                    self.modified_cells.extend([
                        (idx, ff_idx),
                        (idx, fg_idx),
                        (idx, ec_idx),
                        (idx, ed_idx)
                    ])

            n += 1
    
    def _process_warehouses(self):
        """处理所有仓库的数据"""
        self._update_progress("正在处理仓库数据...")
        
        # 获取所有仓库列表（跳过标题行和空行）
        warehouse_data = []
        for idx in range(2, len(self.sales_df)):
            # 跳过完全空行
            if self.sales_df.iloc[idx].isna().all():
                continue
                
            warehouse = self.sales_df.at[idx, 'GJ']
            if pd.notna(warehouse) and warehouse != '':
                warehouse_data.append(warehouse)
        
        warehouses = list(set(warehouse_data))  # 去重
        
        for warehouse in warehouses:
            self._update_progress(f"正在处理仓库: {warehouse}")
            self._process_warehouse_data(warehouse)
    
    def _process_warehouse_data(self, warehouse: str):
        """处理单个仓库的数据"""
        # 获取该仓库的所有新料号（跳过标题行和空行）
        material_codes = []
        
        for idx in range(2, len(self.sales_df)):
            # 跳过完全空行
            if self.sales_df.iloc[idx].isna().all():
                continue

            row_warehouse = self.sales_df.at[idx, 'GJ']
            row_material = self._normalize_material_code(self.sales_df.at[idx, 'DZ'])
            
            if (pd.notna(row_warehouse) and row_warehouse == warehouse and
                pd.notna(row_material) and row_material != ''):
                material_codes.append(row_material)
        
        # 去重处理每个物料
        unique_materials = list(set(material_codes))
        
        for material_code in unique_materials:
            self._process_material_in_warehouse(material_code, warehouse)
    
    def _process_material_in_warehouse(self, material_code: str, warehouse: str):
        """处理仓库中的具体物料"""
        material_code = self._normalize_material_code(material_code)


        # 从即时库存表中获取该物料在该仓库的库存信息（跳过标题行和空行）
        stock_rows = []
        
        for idx in range(1, len(self.stock_df)):  # 即时库存表从第2行开始
            # 跳过完全空行
            if self.stock_df.iloc[idx].isna().all():
                continue

            row_material = self._normalize_material_code(self.stock_df.at[idx, 'A'])
            row_warehouse = str(self.stock_df.at[idx, 'G']).strip()
            
            if (pd.notna(row_material) and row_material == material_code and
                pd.notna(row_warehouse) and row_warehouse == str(warehouse)):
                stock_rows.append(idx)
        
        if not stock_rows:
            self._update_progress(f"警告: 在库存表中未找到物料 {material_code} 在仓库 {warehouse} 的信息")
            return
        
        # 获取销售出库单中对应的行（跳过标题行和空行）
        sales_rows = []
        
        for idx in range(2, len(self.sales_df)):  # 销售出库单从第3行开始
            # 跳过完全空行
            if self.sales_df.iloc[idx].isna().all():
                continue

            row_material = self._normalize_material_code(self.sales_df.at[idx, 'DZ'])
            row_warehouse = str(self.sales_df.at[idx, 'GJ']).strip()
            
            if (pd.notna(row_material) and row_material == material_code and
                pd.notna(row_warehouse) and row_warehouse == str(warehouse)):
                sales_rows.append(idx)
        
        if not sales_rows:
            return
        
        # 按库存量分配批次号
        self._allocate_batch_numbers(sales_rows, stock_rows, material_code, warehouse)
        
        # 更新辅助属性
        self._update_auxiliary_attributes(sales_rows, stock_rows, material_code, warehouse)
    
    def _allocate_batch_numbers(self, sales_row_indices: list, stock_row_indices: list,
                               material_code: str, warehouse: str):
        """分配批次号"""

        # 按行数计算需要分配的数量
        total_sales_qty = len(sales_row_indices)
        if total_sales_qty <= 0:
            return
        
        # 收集库存批次信息
        batch_info = {}
        for idx in stock_row_indices:
            batch_num = self.stock_df.at[idx, 'H']
            batch_qty = self.stock_df.at[idx, 'K']
            
            if pd.notna(batch_num) and pd.notna(batch_qty):
                try:
                    qty = float(batch_qty)
                    if batch_num not in batch_info:
                        batch_info[batch_num] = {
                            'quantity': 0,
                            'auxiliary_attrs': {
                                'D': self.stock_df.at[idx, 'D'],
                                'E': self.stock_df.at[idx, 'E'],
                                'F': self.stock_df.at[idx, 'F']
                            }
                        }
                    batch_info[batch_num]['quantity'] += qty
                except:
                    pass
        
        # 按库存量分配批次号
        allocated_qty = 0
        batch_allocation = []
        
        for batch_num, info in batch_info.items():
            if allocated_qty >= total_sales_qty:
                break
                
            batch_stock = int(info['quantity'])

            # 计算本批次可分配的行数
            remaining_qty = total_sales_qty - allocated_qty
            allocated_batch_qty = min(batch_stock, remaining_qty)
            
            if allocated_batch_qty > 0:
                batch_allocation.append({
                    'batch_num': batch_num,
                    'quantity': allocated_batch_qty,
                    'auxiliary_attrs': info['auxiliary_attrs']
                })
                
                allocated_qty += allocated_batch_qty
        
        # 应用分配结果到销售出库单
        self._apply_batch_allocation(sales_row_indices, batch_allocation)
    
    def _apply_batch_allocation(self, sales_row_indices: list, batch_allocation: List[Dict]):
        """应用批次分配结果"""
        row_idx = 0
        
        for allocation in batch_allocation:
            batch_num = allocation['batch_num']
            quantity = int(allocation['quantity'])
            auxiliary_attrs = allocation['auxiliary_attrs']

            rows_to_allocate = quantity

            while rows_to_allocate > 0 and row_idx < len(sales_row_indices):
                actual_idx = sales_row_indices[row_idx]

                # 更新批次号
                self.sales_df.at[actual_idx, 'FF'] = batch_num
                self.sales_df.at[actual_idx, 'FG'] = batch_num

                # 同时更新原始列
                ff_col_idx = 161  # FF列的实际位置
                fg_col_idx = 162  # FG列的实际位置

                if ff_col_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[actual_idx, ff_col_idx] = batch_num
                if fg_col_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[actual_idx, fg_col_idx] = batch_num

                # 记录修改的单元格
                self.modified_cells.extend([
                    (actual_idx, ff_col_idx),
                    (actual_idx, fg_col_idx)
                ])

                rows_to_allocate -= 1
                row_idx += 1
    
    def _update_auxiliary_attributes(self, sales_row_indices: list, stock_row_indices: list,
                                   material_code: str, warehouse: str):
        """更新辅助属性"""
        if not stock_row_indices:
            return
        
        # 获取库存表中的辅助属性（使用第一个库存行的属性）
        first_stock_idx = stock_row_indices[0]
        aux_attrs = {
            'D': self.stock_df.at[first_stock_idx, 'D'],
            'E': self.stock_df.at[first_stock_idx, 'E'],
            'F': self.stock_df.at[first_stock_idx, 'F']
        }
        
        # 更新销售出库单中的辅助属性
        # 根据需要可以在这里添加辅助属性的同步逻辑
        # 目前题目中没有明确销售出库单的辅助属性列位置，所以暂时跳过
        pass
    
    def _save_with_highlights(self):
        """保存文件并高亮修改内容"""
        self._update_progress("正在保存文件...")

        # 先创建备份
        backup_path = create_backup_file(self.sales_file_path)
        self._update_progress(f"已创建备份: {backup_path}")

        # 直接使用openpyxl打开原始文件
        wb = load_workbook(self.sales_file_path)
        ws = wb.active
        
        # 设置红色填充样式
        red_fill = PatternFill(start_color="FFFF0000", end_color="FFFF0000", fill_type="solid")
        
        # 应用所有修改并高亮修改的单元格
        for row_idx, col_idx in self.modified_cells:
            # 转换列索引到openpyxl格式（1-based）
            openpyxl_col_idx = self._get_column_index(col_idx)
            # 获取修改后的值
            new_value = self.sales_df.iloc[row_idx, col_idx]
            
            # 更新单元格值
            ws.cell(row=row_idx + 1, column=openpyxl_col_idx).value = new_value
            # 高亮修改的单元格
            ws.cell(row=row_idx + 1, column=openpyxl_col_idx).fill = red_fill
                
        # 保存文件
        wb.save(self.sales_file_path)

        self._update_progress("文件保存完成")

        # 提醒用户当前版本可能存在保存相关问题
        self._update_progress("执行保存动作，当前可能存在BUG，没保存")

        # 简单验证保存结果
        try:
            load_workbook(self.sales_file_path)
            self._update_progress("文件验证成功")
        except Exception as e:
            self._update_progress(f"文件验证失败: {e}")
    
    
    
    def _get_column_index(self, column_index: int) -> int:
        """将0-based列索引转换为1-based列索引（openpyxl使用）"""
        return column_index + 1
    
    def get_warehouses_count(self, material_code: str) -> int:
        """获取指定物料的仓库数量"""
        if self.stock_df is None:
            return 0

        code = self._normalize_material_code(material_code)
        warehouses = self.stock_df[self.stock_df['A'].apply(self._normalize_material_code) == code]['G'].nunique()
        return warehouses
    
    def get_batch_info(self, material_code: str, warehouse: str) -> List[Dict]:
        """获取指定物料在指定仓库的批次信息"""
        if self.stock_df is None:
            return []


        code = self._normalize_material_code(material_code)
        batch_info = self.stock_df[
            (self.stock_df['A'].apply(self._normalize_material_code) == code) &
            (self.stock_df['G'].astype(str).str.strip() == str(warehouse))
        ]
        
        result = []
        for _, row in batch_info.iterrows():
            result.append({
                'batch_num': row['H'],
                'quantity': row['K'],
                'auxiliary_attrs': {
                    'D': row['D'],
                    'E': row['E'],
                    'F': row['F']
                }
            })
        return result
