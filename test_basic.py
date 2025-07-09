"""
基本功能测试脚本
"""

import sys
import os
import pandas as pd

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from logic import StockSyncProcessor
from utils import validate_material_code, load_excel_file

def test_material_code_validation():
    """测试物料编码验证"""
    print("测试物料编码验证...")
    
    # 有效的编码
    valid_codes = [
        "8.01.1.01.01.206",
        "8.01.1.01.01.233",
        "4.01.0.01.01.002"
    ]
    
    # 无效的编码
    invalid_codes = [
        "8.01.1.01.01",  # 缺少部分
        "8.01.1.01.01.206.123",  # 多余部分
        "8.01.a.01.01.206",  # 包含字母
        ""  # 空字符串
    ]
    
    for code in valid_codes:
        if validate_material_code(code):
            print(f"✓ {code} - 有效")
        else:
            print(f"✗ {code} - 应该有效但验证失败")
    
    for code in invalid_codes:
        if not validate_material_code(code):
            print(f"✓ {code} - 无效（正确）")
        else:
            print(f"✗ {code} - 应该无效但验证通过")


def test_processor_creation():
    """测试处理器创建"""
    print("\n测试处理器创建...")
    
    try:
        processor = StockSyncProcessor()
        print("✓ 处理器创建成功")
        
        # 测试物料编码映射
        error = processor.set_material_mapping("8.01.1.01.01.206", "8.01.1.01.01.233")
        if not error:
            print("✓ 物料编码映射设置成功")
        else:
            print(f"✗ 物料编码映射设置失败: {error}")
        
        # 测试无效编码
        error = processor.set_material_mapping("invalid", "8.01.1.01.01.233")
        if error:
            print("✓ 无效编码正确被拒绝")
        else:
            print("✗ 无效编码应该被拒绝")
            
    except Exception as e:
        print(f"✗ 处理器创建失败: {e}")


def create_test_data():
    """创建测试数据"""
    print("\n创建测试数据...")
    
    # 创建测试销售出库单
    sales_data = {
        'DZ': ['FMaterialID', '*(明细信息)物料编码#编码', '8.01.1.01.01.206', '8.01.1.01.01.206'],
        'FF': ['FLot', '(明细信息)批号#主档', 'Z25050086', 'Z25050086'],
        'FG': ['FLot#Text', '(明细信息)批号#手工', 'Z25050086', 'Z25050086'],
        'GJ': ['FStockID#Name', '(明细信息)仓库#名称', '成都亚一公共平台仓2号库', '成都亚一公共平台仓2号库'],
        'HA': ['FSALUNITQTY', '(明细信息)销售数量', '50', '50']
    }
    
    sales_df = pd.DataFrame(sales_data)
    sales_df.to_excel('test_sales.xlsx', index=False)
    print("✓ 测试销售出库单创建成功: test_sales.xlsx")
    
    # 创建测试即时库存表
    stock_data = {
        'A': ['物料编码', '8.01.1.01.01.233', '8.01.1.01.01.233'],
        'G': ['仓库名称', '成都亚一公共平台仓2号库', '成都亚一公共平台仓2号库'],
        'H': ['批号', 'Z25050091', 'Z25050092'],
        'K': ['可用量(主单位)', '70', '80'],
        'D': ['辅助属性', 'attr1', 'attr1'],
        'E': ['辅助属性.辅助属性.编码', 'code1', 'code1'],
        'F': ['辅助属性.辅助属性.名称', 'name1', 'name1']
    }
    
    stock_df = pd.DataFrame(stock_data)
    stock_df.to_excel('test_stock.xlsx', index=False)
    print("✓ 测试即时库存表创建成功: test_stock.xlsx")


def test_file_loading():
    """测试文件加载"""
    print("\n测试文件加载...")
    
    processor = StockSyncProcessor()
    
    # 测试加载销售出库单
    if os.path.exists('test_sales.xlsx'):
        error = processor.load_sales_file('test_sales.xlsx')
        if not error:
            print("✓ 销售出库单加载成功")
        else:
            print(f"✗ 销售出库单加载失败: {error}")
    else:
        print("✗ 测试销售出库单文件不存在")
    
    # 测试加载即时库存表
    if os.path.exists('test_stock.xlsx'):
        error = processor.load_stock_file('test_stock.xlsx')
        if not error:
            print("✓ 即时库存表加载成功")
        else:
            print(f"✗ 即时库存表加载失败: {error}")
    else:
        print("✗ 测试即时库存表文件不存在")


def main():
    """主测试函数"""
    print("=== StockSyncPro 基本功能测试 ===")
    
    test_material_code_validation()
    test_processor_creation()
    create_test_data()
    test_file_loading()
    
    print("\n=== 测试完成 ===")
    print("如果所有测试都通过，说明基本功能正常")
    print("可以运行 python main.py 启动图形界面")


if __name__ == "__main__":
    main() 