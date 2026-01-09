#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
配置文档生成工具
用于生成插件配置文档
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.config import Config
from src.utils.plugin_config_manager import get_plugin_config_manager
from src.plugin.plugin_manager import PluginManager


def generate_all_plugin_config_docs(output_dir: str = "docs"):
    """
    生成所有插件配置文档
    
    Args:
        output_dir: 输出目录
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 初始化配置和插件管理器
    config = Config()
    plugin_manager = PluginManager(config)
    plugin_config_manager = get_plugin_config_manager(config)
    
    # 加载插件
    plugin_manager.load_plugins()
    plugin_manager.initialize_plugins()
    
    # 生成每个插件的配置文档
    for type_key in plugin_manager.plugin_instances:
        for plugin_name, plugin_instance in plugin_manager.plugin_instances[type_key].items():
            doc = plugin_instance.get_config_documentation()
            if doc:
                # 生成文档文件名
                doc_filename = os.path.join(output_dir, f"{plugin_name}_config.md")
                
                # 写入文档
                with open(doc_filename, 'w', encoding='utf-8') as f:
                    f.write(doc)
                
                print(f"已生成配置文档: {doc_filename}")
    
    # 生成系统配置文档
    generate_system_config_docs(config, os.path.join(output_dir, "system_config.md"))
    
    print(f"所有配置文档已生成到目录: {output_dir}")


def generate_system_config_docs(config: Config, output_file: str):
    """
    生成系统配置文档
    
    Args:
        config: 系统配置实例
        output_file: 输出文件路径
    """
    doc = "# 系统配置文档\n\n"
    doc += "## 系统配置结构\n\n"
    
    # 生成系统配置文档
    for field_name, field in config.model_fields.items():
        field_info = field.field_info
        doc += f"## {field_name}\n"
        doc += f"- **类型**: {field.annotation.__name__}\n"
        doc += f"- **默认值**: {field.default}\n"
        if field_info.description:
            doc += f"- **描述**: {field_info.description}\n"
        doc += "\n"
    
    # 写入文档
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(doc)
    
    print(f"已生成系统配置文档: {output_file}")


def main():
    """
    主函数
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="生成插件配置文档")
    parser.add_argument('--output', '-o', default="docs", help="输出目录")
    args = parser.parse_args()
    
    generate_all_plugin_config_docs(args.output)


if __name__ == "__main__":
    main()
