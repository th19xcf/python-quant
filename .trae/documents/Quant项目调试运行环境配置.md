# Quant 项目调试运行环境配置计划

## 环境信息
- **虚拟环境路径**: `e:\code\python\quant\venv`
- **Python版本**: 3.14.2
- **激活脚本**: `venv\Scripts\Activate.ps1`

## 配置步骤

### 1. 激活虚拟环境
在 PowerShell 中运行：
```powershell
e:\code\python\quant\venv\Scripts\Activate.ps1
```

### 2. 验证环境
检查 Python 和关键包：
```powershell
python --version
pip list | findstr -i "pyside6 polars pandas"
```

### 3. 运行程序
```powershell
cd e:\code\python\quant
python -m src.main
```

### 4. 调试配置（VS Code）
创建 `.vscode/launch.json`：
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Quant Debug",
            "type": "debugpy",
            "request": "launch",
            "module": "src.main",
            "cwd": "${workspaceFolder}",
            "python": "${workspaceFolder}/venv/Scripts/python.exe"
        }
    ]
}
```

## 预期结果
- 成功激活虚拟环境后，命令行提示符前会显示 `(venv)`
- 程序能够正常启动并加载 PySide6 界面