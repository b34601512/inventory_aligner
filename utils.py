"""
工具函数模块
包含文件处理、数据验证等通用功能
"""

import os
import pandas as pd
import chardet
from typing import Tuple, Optional, Dict, Any
from PyQt5.QtWidgets import QMessageBox, QWidget


def detect_file_encoding(file_path: str) -> str:
    """
    检测文件编码格式
    
    Args:
        file_path: 文件路径
        
    Returns:
        检测到的编码格式
    """
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            return result.get('encoding', 'utf-8')
    except Exception:
        return 'utf-8'


def load_excel_file(file_path: str) -> Tuple[Optional[pd.DataFrame], str]:
    """
    加载Excel文件
    
    Args:
        file_path: Excel文件路径
        
    Returns:
        (DataFrame对象, 错误信息)
    """
    try:
        if not os.path.exists(file_path):
            return None, f"文件不存在: {file_path}"
        
        # 尝试读取Excel文件
        if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
            df = pd.read_excel(file_path, engine='openpyxl' if file_path.endswith('.xlsx') else 'xlrd')
        else:
            return None, "不支持的文件格式，请使用Excel文件"
            
        if df.empty:
            return None, "Excel文件为空"
            
        return df, ""
        
    except Exception as e:
        return None, f"读取Excel文件失败: {str(e)}"


def save_excel_file(df: pd.DataFrame, file_path: str, highlight_changes: bool = True) -> str:
    """
    保存Excel文件
    
    Args:
        df: DataFrame对象
        file_path: 保存路径
        highlight_changes: 是否高亮显示修改内容
        
    Returns:
        错误信息，空字符串表示成功
    """
    try:
        # 创建备份文件
        backup_path = file_path.replace('.xlsx', '_backup.xlsx').replace('.xls', '_backup.xls')
        
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            
            # 如果需要高亮显示修改内容
            if highlight_changes:
                from openpyxl.styles import PatternFill
                worksheet = writer.sheets['Sheet1']
                
                # 设置红色填充样式
                red_fill = PatternFill(start_color="FFFF0000", end_color="FFFF0000", fill_type="solid")
                
                # 这里可以添加具体的高亮逻辑
                # 由于需要知道具体哪些单元格被修改，这个功能在logic.py中实现
                
        return ""
        
    except Exception as e:
        return f"保存Excel文件失败: {str(e)}"


def validate_material_code(code: str) -> bool:
    """
    验证物料编码格式
    
    Args:
        code: 物料编码
        
    Returns:
        是否有效
    """
    if not code or not isinstance(code, str):
        return False
    
    # 检查是否符合格式 x.xx.x.xx.xx.xxx
    parts = code.split('.')
    if len(parts) != 6:
        return False
    
    # 检查每个部分是否为数字
    for part in parts:
        if not part.isdigit():
            return False
    
    return True


def show_message(parent: QWidget, title: str, message: str, msg_type: str = "info") -> None:
    """
    显示消息框
    
    Args:
        parent: 父窗口
        title: 标题
        message: 消息内容
        msg_type: 消息类型 (info, warning, error, success)
    """
    msg_box = QMessageBox(parent)
    msg_box.setWindowTitle(title)
    msg_box.setText(message)
    
    if msg_type == "error":
        msg_box.setIcon(QMessageBox.Critical)
    elif msg_type == "warning":
        msg_box.setIcon(QMessageBox.Warning)
    elif msg_type == "success":
        msg_box.setIcon(QMessageBox.Information)
    else:
        msg_box.setIcon(QMessageBox.Information)
    
    msg_box.exec_()


def format_progress_message(current: int, total: int, description: str) -> str:
    """
    格式化进度消息
    
    Args:
        current: 当前进度
        total: 总数
        description: 描述
        
    Returns:
        格式化的进度消息
    """
    percentage = int((current / total) * 100) if total > 0 else 0
    return f"{description} ({current}/{total}) - {percentage}%"


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    清理DataFrame数据
    
    Args:
        df: 原始DataFrame
        
    Returns:
        清理后的DataFrame
    """
    # 去除空行
    df = df.dropna(how='all')
    
    # 去除完全重复的行
    df = df.drop_duplicates()
    
    # 重置索引
    df = df.reset_index(drop=True)
    
    return df


def get_column_by_name(df: pd.DataFrame, column_names: list) -> Optional[str]:
    """
    根据列名列表找到第一个存在的列
    
    Args:
        df: DataFrame对象
        column_names: 可能的列名列表
        
    Returns:
        找到的列名，如果都不存在返回None
    """
    for col_name in column_names:
        if col_name in df.columns:
            return col_name
    return None


def create_backup_file(file_path: str) -> str:
    """
    创建备份文件
    
    Args:
        file_path: 原文件路径
        
    Returns:
        备份文件路径
    """
    import shutil
    import datetime
    
    # 生成备份文件名
    base_name = os.path.splitext(file_path)[0]
    ext = os.path.splitext(file_path)[1]
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{base_name}_backup_{timestamp}{ext}"
    
    # 复制文件
    shutil.copy2(file_path, backup_path)
    
    return backup_path 