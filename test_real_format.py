"""
使用真实列位置的测试脚本
模拟实际的Excel表格结构
"""

import sys
import os
import pandas as pd
import numpy as np

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from logic import StockSyncProcessor


def create_real_format_sales_data():
    """创建真实格式的销售出库单数据"""
    print("创建真实格式的销售出库单数据...")
    
    # 创建一个包含270+列的DataFrame
    num_cols = 280  # 确保有足够的列
    num_rows = 10   # 包含标题行和数据行
    
    # 初始化所有列为空
    data = {}
    for i in range(num_cols):
        data[i] = [None] * num_rows
    
    # 设置标题行（第1行）
    data[103][0] = 'FMaterialID'  # DZ列 - 物料编码
    data[135][0] = 'FLot'         # FF列 - 批号
    data[136][0] = 'FLot#Text'    # FG列 - 批号
    data[269][0] = 'FStockID#Name'  # GJ列 - 仓库名称
    data[208][0] = 'FSALUNITQTY'  # HA列 - 销售数量
    
    # 设置第二行（说明行）
    data[103][1] = '*(明细信息)物料编码#编码'
    data[135][1] = '(明细信息)批号#主档'
    data[136][1] = '(明细信息)批号#手工'
    data[269][1] = '(明细信息)仓库#名称'
    data[208][1] = '(明细信息)销售数量'
    
    # 设置数据行
    # 第3行
    data[103][2] = '8.01.1.01.01.206'
    data[135][2] = 'OLD_BATCH1'
    data[136][2] = 'OLD_BATCH1'
    data[269][2] = '成都亚一公共平台仓2号库'
    data[208][2] = '30'
    
    # 第4行
    data[103][3] = '8.01.1.01.01.206'
    data[135][3] = 'OLD_BATCH1'
    data[136][3] = 'OLD_BATCH1'
    data[269][3] = '成都亚一公共平台仓2号库'
    data[208][3] = '20'
    
    # 第5行（空行）
    # 所有列都保持None
    
    # 第6行
    data[103][5] = '8.01.2.01.01.301'
    data[135][5] = 'OLD_BATCH2'
    data[136][5] = 'OLD_BATCH2'
    data[269][5] = '沈阳公共平台仓3号库'
    data[208][5] = '40'
    
    # 第7行
    data[103][6] = '8.01.2.01.01.301'
    data[135][6] = 'OLD_BATCH2'
    data[136][6] = 'OLD_BATCH2'
    data[269][6] = '沈阳公共平台仓3号库'
    data[208][6] = '25'
    
    df = pd.DataFrame(data)
    df.to_excel('test_real_sales.xlsx', index=False, header=False)
    print("✓ 真实格式销售出库单创建完成: test_real_sales.xlsx")
    return 'test_real_sales.xlsx'


def create_real_format_stock_data():
    """创建真实格式的即时库存表数据"""
    print("创建真实格式的即时库存表数据...")
    
    # 创建一个包含20列的DataFrame
    num_cols = 20
    num_rows = 8
    
    # 初始化所有列为空
    data = {}
    for i in range(num_cols):
        data[i] = [None] * num_rows
    
    # 设置标题行（第1行）
    data[0][0] = '物料编码'     # A列
    data[3][0] = '辅助属性'     # D列
    data[4][0] = '辅助属性.辅助属性.编码'  # E列
    data[5][0] = '辅助属性.辅助属性.名称'  # F列
    data[6][0] = '仓库名称'     # G列
    data[7][0] = '批号'        # H列
    data[10][0] = '可用量(主单位)'  # K列
    
    # 设置数据行
    # 第2行 - 第一个物料在成都仓库的第一个批次
    data[0][1] = '8.01.1.01.01.233'
    data[3][1] = 'attr1'
    data[4][1] = 'code1'
    data[5][1] = 'name1'
    data[6][1] = '成都亚一公共平台仓2号库'
    data[7][1] = 'Z25050091'
    data[10][1] = '35'
    
    # 第3行 - 第一个物料在成都仓库的第二个批次
    data[0][2] = '8.01.1.01.01.233'
    data[3][2] = 'attr1'
    data[4][2] = 'code1'
    data[5][2] = 'name1'
    data[6][2] = '成都亚一公共平台仓2号库'
    data[7][2] = 'Z25050092'
    data[10][2] = '40'
    
    # 第4行（空行）
    # 所有列都保持None
    
    # 第5行 - 第二个物料在沈阳仓库的第一个批次
    data[0][4] = '8.01.2.01.01.401'
    data[3][4] = 'attr2'
    data[4][4] = 'code2'
    data[5][4] = 'name2'
    data[6][4] = '沈阳公共平台仓3号库'
    data[7][4] = 'Z25050093'
    data[10][4] = '50'
    
    # 第6行 - 第二个物料在沈阳仓库的第二个批次
    data[0][5] = '8.01.2.01.01.401'
    data[3][5] = 'attr2'
    data[4][5] = 'code2'
    data[5][5] = 'name2'
    data[6][5] = '沈阳公共平台仓3号库'
    data[7][5] = 'Z25050094'
    data[10][5] = '80'
    
    df = pd.DataFrame(data)
    df.to_excel('test_real_stock.xlsx', index=False, header=False)
    print("✓ 真实格式即时库存表创建完成: test_real_stock.xlsx")
    return 'test_real_stock.xlsx'


def progress_callback(message):
    """进度回调函数"""
    print(f"    {message}")


def test_real_format():
    """测试真实格式的数据处理"""
    print("=== 真实格式数据处理测试 ===\n")
    
    # 1. 创建测试数据
    sales_file = create_real_format_sales_data()
    stock_file = create_real_format_stock_data()
    
    # 2. 创建处理器
    print("\n=== 初始化处理器 ===")
    processor = StockSyncProcessor(progress_callback)
    print("✓ 处理器创建成功")
    
    # 3. 设置物料编码映射
    print("\n=== 设置物料编码映射 ===")
    mappings = [
        ('8.01.1.01.01.206', '8.01.1.01.01.233'),
        ('8.01.2.01.01.301', '8.01.2.01.01.401')
    ]
    
    for old_code, new_code in mappings:
        error = processor.set_material_mapping(old_code, new_code)
        if error:
            print(f"✗ 映射设置失败: {error}")
            return
        else:
            print(f"✓ 映射设置成功: {old_code} -> {new_code}")
    
    # 4. 加载文件
    print("\n=== 加载文件 ===")
    
    # 加载销售出库单
    error = processor.load_sales_file(sales_file)
    if error:
        print(f"✗ 销售出库单加载失败: {error}")
        return
    else:
        print("✓ 销售出库单加载成功")
        print(f"  - 列数: {len(processor.sales_df.columns)}")
        print(f"  - 行数: {len(processor.sales_df)}")
    
    # 加载即时库存表
    error = processor.load_stock_file(stock_file)
    if error:
        print(f"✗ 即时库存表加载失败: {error}")
        return
    else:
        print("✓ 即时库存表加载成功")
        print(f"  - 列数: {len(processor.stock_df.columns)}")
        print(f"  - 行数: {len(processor.stock_df)}")
    
    # 5. 执行同步处理
    print("\n=== 开始数据同步处理 ===")
    error = processor.process_synchronization()
    
    if error:
        print(f"✗ 同步处理失败: {error}")
        return
    else:
        print("✓ 同步处理完成！")
    
    # 6. 显示处理结果
    print("\n=== 处理结果 ===")
    print(f"修改的单元格数量: {len(processor.modified_cells)}")
    print("修改的位置:")
    for row, col in processor.modified_cells:
        print(f"  - 行{row+1}, 列{col+1}")
    
    print("\n=== 测试完成 ===")
    print("可以打开 test_real_sales.xlsx 查看处理结果")


if __name__ == "__main__":
    try:
        test_real_format()
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()