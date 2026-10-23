# 项目工作规则

## Git 提交规则

- **每次执行 git 提交前，必须先在聊天窗口展示待提交内容**，包括：
  - 待提交的文件清单（新增/修改/删除）
  - 拟定的 commit message
  - 备份同步状态（SHA256 校验）
- **必须等待用户人工确认后，才能执行 `git commit`**
- 用户未明确确认前，不得自动提交
- **每次执行 git commit 后，必须立即在聊天窗口输出完整的提交信息**，固定包含以下 5 块：

---

### Commit 输出模板（严格按此格式）

```
# ✅ Commit 完成（`<short_hash>`）

## 1. Commit 哈希
| 类型 | 值 |
|---|---|
| **短哈希** | `<short_hash>` |
| **长哈希** | `<long_hash>` |

---

## 2. 完整 Commit Message

```
<verbatim 原样输出 git log -1 --format="%B" HEAD>
```

---

## 3. 文件变更统计 + 清单

**统计**: `<git diff HEAD~1 --stat 第一行>`

| 状态 | 文件 | 变化 |
|---|---|---|
| **A**（新增） | `path/to/file.py` | +37 |
| **M**（修改） | `path/to/file.py` | +110 |
| **R**（重命名） | `old/path.py` → `new/path.py` | 100% rename |
| **R+M**（重命名+改） | `old/path.py` → `new/path.py` | +重写（80%） |
| **D**（删除） | `path/to/file.py` | -228 |

---

## 4. 工作区状态（`git status --short`）
```
(clean —— 无未提交变更)  或直接粘贴命令输出
```

---

## 5. 备份同步确认
✅ `<backup_cmd> → 目标路径`  
✅ `<backup_cmd> → 目标路径`  
✅ 关键文件 SHA256 **全部一致**（列出 3~5 个代表性文件名）
```

---

### 注意事项
- **Commit message 必须原样输出**：从 `git log -1 --format="%B"` 取，不做任何二次格式化（不要加"标题""正文"标签，不要改缩进）
- **不要伪造输出**：所有数值、哈希、文件状态都必须来自 `git` 和 `robocopy` 的真实输出
- **备份方式**：`robocopy <src_dir> <backup_dir> /MIR /R:1 /W:1`；backup 目标固定 `E:\git\python-quant\`；exit code=1 是 robocopy 正常复制成功，exit code>=8 是失败

## 备份同步规则

- **每次执行 git 提交前，必须先把本次修改的文件同步到备份目录 `e:\git\python-quant\` 下对应的位置**（保持与仓库相同的相对路径结构）
- 同步范围 = 本次提交涉及的全部文件（新增/修改）；删除的文件需从备份目录对应位置一并移除
- 备份同步完成后，再按上述 Git 提交规则请用户确认并提交
