"""
主用户界面模块
使用PyQt5实现图形界面
"""

import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                             QTextEdit, QFileDialog, QGroupBox, QProgressBar,
                             QGridLayout, QMessageBox, QFrame, QSplitter)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QFont, QIcon, QPalette, QColor
from logic import StockSyncProcessor
from utils import show_message, validate_material_code


class ProcessingThread(QThread):
    """处理线程"""
    progress_updated = pyqtSignal(str)
    finished = pyqtSignal(str)
    
    def __init__(self, processor):
        super().__init__()
        self.processor = processor
        
    def run(self):
        try:
            # 设置进度回调
            self.processor.set_progress_callback(self.progress_updated.emit)
            
            # 执行同步处理
            error = self.processor.process_synchronization()
            
            if error:
                self.finished.emit(f"处理失败: {error}")
            else:
                self.finished.emit("处理完成！")
                
        except Exception as e:
            self.finished.emit(f"处理异常: {str(e)}")


class StockSyncMainWindow(QMainWindow):
    """主窗口类"""
    
    def __init__(self):
        super().__init__()
        self.processor = StockSyncProcessor()
        self.processing_thread = None
        self.init_ui()
        self.setup_styles()
        
    def init_ui(self):
        """初始化用户界面"""
        self.setWindowTitle("StockSyncPro - 库存同步程序 v1.0")
        self.setGeometry(100, 100, 1000, 700)
        
        # 创建中央窗口
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # 创建主布局
        main_layout = QVBoxLayout(central_widget)
        
        # 创建分割器
        splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(splitter)
        
        # 左侧面板
        left_panel = self.create_left_panel()
        splitter.addWidget(left_panel)
        
        # 右侧面板
        right_panel = self.create_right_panel()
        splitter.addWidget(right_panel)
        
        # 设置分割器比例
        splitter.setSizes([400, 600])
        
        # 状态栏
        self.statusBar().showMessage("准备就绪")
        
    def create_left_panel(self):
        """创建左侧控制面板"""
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        
        # 物料编码映射区域
        mapping_group = QGroupBox("物料编码映射")
        mapping_layout = QGridLayout(mapping_group)
        
        # 旧料号输入
        mapping_layout.addWidget(QLabel("旧料号:"), 0, 0)
        self.old_material_code = QLineEdit()
        self.old_material_code.setPlaceholderText("例如: 8.01.1.01.01.206")
        mapping_layout.addWidget(self.old_material_code, 0, 1)
        
        # 新料号输入
        mapping_layout.addWidget(QLabel("新料号:"), 1, 0)
        self.new_material_code = QLineEdit()
        self.new_material_code.setPlaceholderText("例如: 8.01.1.01.01.233")
        mapping_layout.addWidget(self.new_material_code, 1, 1)
        
        # 添加映射按钮
        add_mapping_btn = QPushButton("添加映射")
        add_mapping_btn.clicked.connect(self.add_material_mapping)
        mapping_layout.addWidget(add_mapping_btn, 2, 0, 1, 2)
        
        # 映射列表显示
        self.mapping_display = QTextEdit()
        self.mapping_display.setMaximumHeight(100)
        self.mapping_display.setReadOnly(True)
        mapping_layout.addWidget(self.mapping_display, 3, 0, 1, 2)
        
        left_layout.addWidget(mapping_group)
        
        # 文件上传区域
        file_group = QGroupBox("文件上传")
        file_layout = QVBoxLayout(file_group)
        
        # 销售出库单文件
        sales_layout = QHBoxLayout()
        sales_layout.addWidget(QLabel("销售出库单:"))
        self.sales_file_path = QLineEdit()
        self.sales_file_path.setReadOnly(True)
        sales_layout.addWidget(self.sales_file_path)
        
        sales_browse_btn = QPushButton("浏览...")
        sales_browse_btn.clicked.connect(self.browse_sales_file)
        sales_layout.addWidget(sales_browse_btn)
        
        file_layout.addLayout(sales_layout)
        
        # 即时库存表文件
        stock_layout = QHBoxLayout()
        stock_layout.addWidget(QLabel("即时库存表:"))
        self.stock_file_path = QLineEdit()
        self.stock_file_path.setReadOnly(True)
        stock_layout.addWidget(self.stock_file_path)
        
        stock_browse_btn = QPushButton("浏览...")
        stock_browse_btn.clicked.connect(self.browse_stock_file)
        stock_layout.addWidget(stock_browse_btn)
        
        file_layout.addLayout(stock_layout)
        
        left_layout.addWidget(file_group)
        
        # 操作按钮区域
        button_group = QGroupBox("操作")
        button_layout = QVBoxLayout(button_group)
        
        # 开始处理按钮
        self.process_btn = QPushButton("开始处理")
        self.process_btn.clicked.connect(self.start_processing)
        self.process_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 10px;
                font-size: 16px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
            }
        """)
        button_layout.addWidget(self.process_btn)
        
        # 进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        button_layout.addWidget(self.progress_bar)
        
        # 清空按钮
        clear_btn = QPushButton("清空所有")
        clear_btn.clicked.connect(self.clear_all)
        button_layout.addWidget(clear_btn)
        
        left_layout.addWidget(button_group)
        
        # 添加弹性空间
        left_layout.addStretch()
        
        return left_widget
        
    def create_right_panel(self):
        """创建右侧日志面板"""
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        
        # 日志显示区域
        log_group = QGroupBox("处理日志")
        log_layout = QVBoxLayout(log_group)
        
        self.log_display = QTextEdit()
        self.log_display.setReadOnly(True)
        self.log_display.setFont(QFont("Consolas", 10))
        log_layout.addWidget(self.log_display)
        
        # 清空日志按钮
        clear_log_btn = QPushButton("清空日志")
        clear_log_btn.clicked.connect(self.clear_log)
        log_layout.addWidget(clear_log_btn)
        
        right_layout.addWidget(log_group)
        
        return right_widget
    
    def setup_styles(self):
        """设置样式"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f0f0f0;
            }
            QGroupBox {
                font-weight: bold;
                border: 2px solid #cccccc;
                border-radius: 5px;
                margin-top: 10px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
            QLineEdit {
                border: 1px solid #cccccc;
                border-radius: 3px;
                padding: 5px;
                font-size: 12px;
            }
            QPushButton {
                border: 1px solid #cccccc;
                border-radius: 3px;
                padding: 5px;
                font-size: 12px;
                background-color: #ffffff;
            }
            QPushButton:hover {
                background-color: #e0e0e0;
            }
            QPushButton:pressed {
                background-color: #d0d0d0;
            }
            QTextEdit {
                border: 1px solid #cccccc;
                border-radius: 3px;
                background-color: #ffffff;
            }
        """)
    
    def add_material_mapping(self):
        """添加物料编码映射"""
        old_code = self.old_material_code.text().strip()
        new_code = self.new_material_code.text().strip()
        
        if not old_code or not new_code:
            show_message(self, "错误", "请输入完整的物料编码", "error")
            return
        
        # 验证编码格式
        if not validate_material_code(old_code):
            show_message(self, "错误", "旧料号格式不正确，应为 x.xx.x.xx.xx.xxx 格式", "error")
            return
        
        if not validate_material_code(new_code):
            show_message(self, "错误", "新料号格式不正确，应为 x.xx.x.xx.xx.xxx 格式", "error")
            return
        
        # 添加映射
        error = self.processor.set_material_mapping(old_code, new_code)
        if error:
            show_message(self, "错误", error, "error")
            return
        
        # 更新显示
        self.update_mapping_display()
        
        # 清空输入框
        self.old_material_code.clear()
        self.new_material_code.clear()
        
        # 添加日志
        self.add_log(f"添加物料编码映射: {old_code} -> {new_code}")
        
    def update_mapping_display(self):
        """更新映射显示"""
        mappings = []
        for old_code, new_code in self.processor.material_mapping.items():
            mappings.append(f"{old_code} -> {new_code}")
        
        self.mapping_display.setPlainText("\n".join(mappings))
        
    def browse_sales_file(self):
        """浏览销售出库单文件"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择销售出库单文件", "", "Excel文件 (*.xlsx *.xls)")
        
        if file_path:
            self.sales_file_path.setText(file_path)
            
            # 尝试加载文件
            error = self.processor.load_sales_file(file_path)
            if error:
                show_message(self, "错误", error, "error")
                self.sales_file_path.clear()
            else:
                self.add_log(f"销售出库单文件加载成功: {os.path.basename(file_path)}")
                
    def browse_stock_file(self):
        """浏览即时库存表文件"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择即时库存表文件", "", "Excel文件 (*.xlsx *.xls)")
        
        if file_path:
            self.stock_file_path.setText(file_path)
            
            # 尝试加载文件
            error = self.processor.load_stock_file(file_path)
            if error:
                show_message(self, "错误", error, "error")
                self.stock_file_path.clear()
            else:
                self.add_log(f"即时库存表文件加载成功: {os.path.basename(file_path)}")
    
    def start_processing(self):
        """开始处理"""
        # 验证输入
        if not self.processor.material_mapping:
            show_message(self, "错误", "请先添加物料编码映射", "error")
            return
        
        if not self.sales_file_path.text():
            show_message(self, "错误", "请选择销售出库单文件", "error")
            return
        
        if not self.stock_file_path.text():
            show_message(self, "错误", "请选择即时库存表文件", "error")
            return
        
        # 禁用按钮和显示进度条
        self.process_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # 不确定进度
        
        # 启动处理线程
        self.processing_thread = ProcessingThread(self.processor)
        self.processing_thread.progress_updated.connect(self.update_progress)
        self.processing_thread.finished.connect(self.processing_finished)
        self.processing_thread.start()
        
        self.add_log("开始处理数据同步...")
        
    def update_progress(self, message):
        """更新进度"""
        self.add_log(message)
        self.statusBar().showMessage(message)
        
    def processing_finished(self, message):
        """处理完成"""
        self.add_log(message)
        self.statusBar().showMessage(message)
        
        # 恢复按钮状态
        self.process_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
        
        # 显示完成消息
        if "失败" in message or "异常" in message:
            show_message(self, "处理结果", message, "error")
        else:
            show_message(self, "处理结果", message, "success")
    
    def add_log(self, message):
        """添加日志"""
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        
        self.log_display.append(log_message)
        self.log_display.moveCursor(self.log_display.textCursor().End)
        
        # 限制日志行数
        if self.log_display.document().lineCount() > 1000:
            cursor = self.log_display.textCursor()
            cursor.movePosition(cursor.Start)
            cursor.movePosition(cursor.Down, cursor.KeepAnchor, 100)
            cursor.removeSelectedText()
    
    def clear_log(self):
        """清空日志"""
        self.log_display.clear()
        self.add_log("日志已清空")
        
    def clear_all(self):
        """清空所有"""
        reply = QMessageBox.question(self, "确认", "确定要清空所有数据吗？", 
                                   QMessageBox.Yes | QMessageBox.No)
        
        if reply == QMessageBox.Yes:
            # 清空所有输入
            self.old_material_code.clear()
            self.new_material_code.clear()
            self.sales_file_path.clear()
            self.stock_file_path.clear()
            self.mapping_display.clear()
            
            # 重置处理器
            self.processor = StockSyncProcessor()
            
            # 清空日志
            self.log_display.clear()
            
            self.add_log("所有数据已清空")
            self.statusBar().showMessage("已清空所有数据")
    
    def closeEvent(self, event):
        """关闭事件"""
        if self.processing_thread and self.processing_thread.isRunning():
            reply = QMessageBox.question(self, "确认", "正在处理数据，确定要退出吗？",
                                       QMessageBox.Yes | QMessageBox.No)
            
            if reply == QMessageBox.Yes:
                self.processing_thread.terminate()
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()


def main():
    """主函数"""
    app = QApplication(sys.argv)
    
    # 设置应用程序信息
    app.setApplicationName("StockSyncPro")
    app.setApplicationVersion("1.0")
    app.setOrganizationName("StockSyncPro")
    
    # 创建主窗口
    window = StockSyncMainWindow()
    window.show()
    
    # 运行应用程序
    sys.exit(app.exec_())


if __name__ == "__main__":
    main() 