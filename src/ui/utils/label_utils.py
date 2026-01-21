from PySide6.QtWidgets import QLabel, QHBoxLayout, QWidget

class LabelUtils:
    """
    标签工具类，用于处理标签的创建和更新
    """
    
    @staticmethod
    def create_label(text="", style_sheet=None):
        """
        创建一个新的标签
        
        Args:
            text: 标签初始文本
            style_sheet: 标签样式表
        
        Returns:
            创建的QLabel实例
        """
        label = QLabel(text)
        
        # 设置默认样式
        default_style = "font-family: Consolas, monospace; background-color: rgba(0, 0, 0, 0.5); padding: 5px; color: #C0C0C0;"
        
        if style_sheet:
            label.setStyleSheet(style_sheet)
        else:
            label.setStyleSheet(default_style)
        
        # 确保不换行
        label.setWordWrap(False)
        
        return label
    
    @staticmethod
    def create_label_container():
        """
        创建标签容器
        
        Returns:
            (容器widget, 布局layout)
        """
        container = QWidget()
        container.setStyleSheet("background-color: #222222;")
        layout = QHBoxLayout(container)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        
        return container, layout
    
    @staticmethod
    def update_label_style(label, style_sheet):
        """
        更新标签样式
        
        Args:
            label: 要更新的标签
            style_sheet: 新的样式表
        """
        label.setStyleSheet(style_sheet)
    
    @staticmethod
    def update_label_text(label, text):
        """
        更新标签文本
        
        Args:
            label: 要更新的标签
            text: 新的文本
        """
        label.setText(text)