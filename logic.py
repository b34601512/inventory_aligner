"""
表格处理逻辑模块
实现销售出库单和即时库存表的同步功能
"""

import pandas as pd
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
                'DZ': 103,  # 第104列 (0-based index)
                'FF': 135,  # 第136列
                'FG': 136,  # 第137列  
                'GJ': 269,  # 第270列
                'HA': 208   # 第209列
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
                    self.material_mapping = json.load(f)
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
            
            # 2. 处理每个仓库的数据
            self._process_warehouses()
            
            # 3. 保存文件并高亮修改内容
            self._save_with_highlights()
            
            self._update_progress("数据同步完成！")
            return ""
            
        except Exception as e:
            return f"同步处理失败: {str(e)}"
    
    def _replace_material_codes(self):
        """替换物料编码"""
        self._update_progress("正在替换物料编码...")
        
        # 跳过前两行（标题行），从第三行开始处理
        for idx in range(2, len(self.sales_df)):
            # 检查是否为空行
            if self.sales_df.iloc[idx].isna().all():
                continue
                
            old_code = str(self.sales_df.at[idx, 'DZ'])
            
            # 跳过空值和标题行
            if pd.isna(old_code) or old_code == 'nan' or old_code == '':
                continue
                
            if old_code in self.material_mapping:
                new_code = self.material_mapping[old_code]
                self.sales_df.at[idx, 'DZ'] = new_code
                
                # 同时更新原始列
                original_col_idx = 103  # DZ列的实际位置
                if original_col_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[idx, original_col_idx] = new_code
                
                # 记录修改的单元格
                self.modified_cells.append((idx, original_col_idx))
                
                self._update_progress(f"已替换物料编码: {old_code} -> {new_code}")
    
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
            row_material = self.sales_df.at[idx, 'DZ']
            
            if (pd.notna(row_warehouse) and row_warehouse == warehouse and 
                pd.notna(row_material) and row_material != ''):
                material_codes.append(row_material)
        
        # 去重处理每个物料
        unique_materials = list(set(material_codes))
        
        for material_code in unique_materials:
            self._process_material_in_warehouse(material_code, warehouse)
    
    def _process_material_in_warehouse(self, material_code: str, warehouse: str):
        """处理仓库中的具体物料"""
        # 从即时库存表中获取该物料在该仓库的库存信息（跳过标题行和空行）
        stock_rows = []
        
        for idx in range(1, len(self.stock_df)):  # 即时库存表从第2行开始
            # 跳过完全空行
            if self.stock_df.iloc[idx].isna().all():
                continue
                
            row_material = self.stock_df.at[idx, 'A']
            row_warehouse = self.stock_df.at[idx, 'G']
            
            if (pd.notna(row_material) and str(row_material) == str(material_code) and
                pd.notna(row_warehouse) and str(row_warehouse) == str(warehouse)):
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
                
            row_material = self.sales_df.at[idx, 'DZ']
            row_warehouse = self.sales_df.at[idx, 'GJ']
            
            if (pd.notna(row_material) and str(row_material) == str(material_code) and
                pd.notna(row_warehouse) and str(row_warehouse) == str(warehouse)):
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
        # 计算总销售数量
        total_sales_qty = 0
        for idx in sales_row_indices:
            qty = self.sales_df.at[idx, 'HA']
            if pd.notna(qty):
                try:
                    total_sales_qty += float(qty)
                except:
                    pass
        
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
                
            batch_stock = info['quantity']
            
            # 计算本批次可分配的数量
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
            quantity = allocation['quantity']
            auxiliary_attrs = allocation['auxiliary_attrs']
            
            # 计算需要分配给当前批次的行数
            # 这里我们按照销售数量来分配，而不是简单的按行数
            remaining_qty = quantity
            
            while remaining_qty > 0 and row_idx < len(sales_row_indices):
                actual_idx = sales_row_indices[row_idx]
                
                # 获取当前行的销售数量
                row_qty = self.sales_df.at[actual_idx, 'HA']
                try:
                    row_qty = float(row_qty) if pd.notna(row_qty) else 1
                except:
                    row_qty = 1
                
                # 更新批次号
                self.sales_df.at[actual_idx, 'FF'] = batch_num
                self.sales_df.at[actual_idx, 'FG'] = batch_num
                
                # 同时更新原始列
                ff_col_idx = 135  # FF列的实际位置
                fg_col_idx = 136  # FG列的实际位置
                
                if ff_col_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[actual_idx, ff_col_idx] = batch_num
                if fg_col_idx < len(self.sales_df.columns):
                    self.sales_df.iloc[actual_idx, fg_col_idx] = batch_num
                
                # 记录修改的单元格
                self.modified_cells.extend([
                    (actual_idx, ff_col_idx),
                    (actual_idx, fg_col_idx)
                ])
                
                remaining_qty -= row_qty
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
    
    
    
    def _get_column_index(self, column_index: int) -> int:
        """将0-based列索引转换为1-based列索引（openpyxl使用）"""
        return column_index + 1
    
    def get_warehouses_count(self, material_code: str) -> int:
        """获取指定物料的仓库数量"""
        if self.stock_df is None:
            return 0
        
        warehouses = self.stock_df[self.stock_df['A'] == material_code]['G'].nunique()
        return warehouses
    
    def get_batch_info(self, material_code: str, warehouse: str) -> List[Dict]:
        """获取指定物料在指定仓库的批次信息"""
        if self.stock_df is None:
            return []
        
        batch_info = self.stock_df[
            (self.stock_df['A'] == material_code) & 
            (self.stock_df['G'] == warehouse)
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