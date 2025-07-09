"""
StockSyncPro - 库存同步程序
主程序入口点
"""

import sys
import os

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ui_main import main

if __name__ == "__main__":
    main()
