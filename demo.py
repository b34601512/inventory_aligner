"""
StockSyncPro 功能演示脚本
展示完整的数据同步流程
"""

import sys
import os
import pandas as pd

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from logic import StockSyncProcessor


def create_demo_data():
    """创建演示数据"""
    print("=== 创建演示数据 ===")
    
    # 创建更真实的销售出库单数据
    sales_data = {
        'DZ': [
            'FMaterialID', 
            '*(明细信息)物料编码#编码', 
            '8.01.1.01.01.206',  # 第一个物料
            '8.01.1.01.01.206',
            '8.01.1.01.01.206',
            '8.01.1.01.01.206',
            '8.01.2.01.01.301',  # 第二个物料
            '8.01.2.01.01.301',
            '8.01.2.01.01.301'
        ],
        'FF': [
            'FLot', 
            '(明细信息)批号#主档', 
            'OLD_BATCH1', 'OLD_BATCH1', 'OLD_BATCH1', 'OLD_BATCH1',
            'OLD_BATCH2', 'OLD_BATCH2', 'OLD_BATCH2'
        ],
        'FG': [
            'FLot#Text', 
            '(明细信息)批号#手工', 
            'OLD_BATCH1', 'OLD_BATCH1', 'OLD_BATCH1', 'OLD_BATCH1',
            'OLD_BATCH2', 'OLD_BATCH2', 'OLD_BATCH2'
        ],
        'GJ': [
            'FStockID#Name', 
            '(明细信息)仓库#名称', 
            '成都亚一公共平台仓2号库', '成都亚一公共平台仓2号库', 
            '成都亚一公共平台仓2号库', '成都亚一公共平台仓2号库',
            '沈阳公共平台仓3号库', '沈阳公共平台仓3号库',
            '沈阳公共平台仓3号库'
        ],
        'HA': [
            'FSALUNITQTY', 
            '(明细信息)销售数量', 
            '25', '25', '25', '25',  # 总共100个第一个物料
            '20', '20', '20'         # 总共60个第二个物料
        ]
    }
    
    sales_df = pd.DataFrame(sales_data)
    sales_df.to_excel('demo_sales.xlsx', index=False)
    print("✓ 演示销售出库单创建完成: demo_sales.xlsx")
    
    # 创建对应的即时库存表数据
    stock_data = {
        'A': [
            '物料编码', 
            '8.01.1.01.01.233', '8.01.1.01.01.233', '8.01.1.01.01.233',  # 第一个物料的新编码
            '8.01.2.01.01.401', '8.01.2.01.01.401'   # 第二个物料的新编码
        ],
        'G': [
            '仓库名称', 
            '成都亚一公共平台仓2号库', '成都亚一公共平台仓2号库', '成都亚一公共平台仓2号库',
            '沈阳公共平台仓3号库', '沈阳公共平台仓3号库'
        ],
        'H': [
            '批号', 
            'Z25050091', 'Z25050092', 'Z25050093',  # 成都仓库的批次
            'Z25050094', 'Z25050095'                 # 沈阳仓库的批次
        ],
        'K': [
            '可用量(主单位)', 
            '40', '70', '50',    # 成都仓库总库存160，销售100
            '35', '80'           # 沈阳仓库总库存115，销售60
        ],
        'D': [
            '辅助属性', 
            'attr1', 'attr1', 'attr1',
            'attr2', 'attr2'
        ],
        'E': [
            '辅助属性.辅助属性.编码', 
            'code1', 'code1', 'code1',
            'code2', 'code2'
        ],
        'F': [
            '辅助属性.辅助属性.名称', 
            'name1', 'name1', 'name1',
            'name2', 'name2'
        ]
    }
    
    stock_df = pd.DataFrame(stock_data)
    stock_df.to_excel('demo_stock.xlsx', index=False)
    print("✓ 演示即时库存表创建完成: demo_stock.xlsx")
    
    return 'demo_sales.xlsx', 'demo_stock.xlsx'


def progress_callback(message):
    """进度回调函数"""
    print(f"    {message}")


def run_demo():
    """运行完整演示"""
    print("=== StockSyncPro 完整功能演示 ===\n")
    
    # 1. 创建演示数据
    sales_file, stock_file = create_demo_data()
    
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
    
    # 加载即时库存表
    error = processor.load_stock_file(stock_file)
    if error:
        print(f"✗ 即时库存表加载失败: {error}")
        return
    else:
        print("✓ 即时库存表加载成功")
    
    # 5. 显示处理前的数据统计
    print("\n=== 处理前数据统计 ===")
    print(f"销售出库单数据行数: {len(processor.sales_df)}")
    print(f"即时库存表数据行数: {len(processor.stock_df)}")
    print(f"物料编码映射数量: {len(processor.material_mapping)}")
    
    # 显示仓库信息
    warehouses = processor.sales_df['GJ'].unique()
    warehouses = [w for w in warehouses if pd.notna(w) and w != '' and w != '(明细信息)仓库#名称']
    print(f"涉及仓库数量: {len(warehouses)}")
    for warehouse in warehouses:
        print(f"  - {warehouse}")
    
    # 6. 执行同步处理
    print("\n=== 开始数据同步处理 ===")
    error = processor.process_synchronization()
    
    if error:
        print(f"✗ 同步处理失败: {error}")
        return
    else:
        print("✓ 同步处理完成！")
    
    # 7. 显示处理结果
    print("\n=== 处理结果统计 ===")
    print(f"修改的单元格数量: {len(processor.modified_cells)}")
    print("修改的单元格位置:")
    for row, col in processor.modified_cells:
        print(f"  - 行{row}, 列{col}")
    
    # 8. 验证结果
    print("\n=== 验证处理结果 ===")
    
    # 检查物料编码是否已替换
    material_codes = processor.sales_df['DZ'].unique()
    print("销售出库单中的物料编码:")
    for code in material_codes:
        if pd.notna(code) and code not in ['FMaterialID', '*(明细信息)物料编码#编码']:
            print(f"  - {code}")
    
    # 检查批次号是否已更新
    batch_numbers = processor.sales_df['FF'].unique()
    print("更新后的批次号:")
    for batch in batch_numbers:
        if pd.notna(batch) and batch not in ['FLot', '(明细信息)批号#主档']:
            print(f"  - {batch}")
    
    print("\n=== 演示完成 ===")
    print("处理后的文件已保存，修改内容已用红色高亮显示")
    print("可以打开 demo_sales.xlsx 查看处理结果")


if __name__ == "__main__":
    try:
        run_demo()
    except Exception as e:
        print(f"演示过程中发生错误: {e}")
        import traceback
        traceback.print_exc() 