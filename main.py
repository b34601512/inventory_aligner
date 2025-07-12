"""
StockSyncPro - 库存同步程序
主程序入口点
作者：黎路遥
时间：2025-07-12
版本：1.0.0

功能：
1. 同步销售出库单中的批次号，按库存分配
2. 同步销售出库单中的辅助属性，按库存分配
3. 同步销售出库单中的物料编码，按库存分配

"""

import sys
import os

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ui_main import main

if __name__ == "__main__":
    main()
