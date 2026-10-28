# 项目工作规则

## Git 提交规则

- **用户已授权免确认提交**：提交前无需再向用户确认，直接完成「变更文件备份同步 → SHA256 校验 → `git commit` → 输出 5 块提交报告」全流程
- 提交前仍需整理待提交内容（文件清单、拟定 commit message、备份同步状态），整理结果随提交报告一并输出，供用户事后复核
- 用户当场明确提出异议或要求暂缓的，以用户指令为准
- **Commit message 必须使用中文**：标题与正文描述统一使用中文，仅保留约定式提交的类型前缀（feat/fix/refactor/perf/chore 等）与 scope 为英文，格式为 `type(scope): 中文描述`
- **每次执行 git commit 后，必须立即在聊天窗口输出完整的提交信息**，固定包含以下 5 块：

---

### Commit 输出模板（严格按此格式）

# ✅ Commit 完成（`<short_hash>`）

## 1. Commit 哈希
| 类型 | 值 |
|---|---|
| **短哈希** | `<short_hash>` |
| **长哈希** | `<long_hash>` |

---

## 2. 完整 Commit Message

```text
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

```text
(clean —— 无未提交变更)  或直接粘贴命令输出
```

---

## 5. 备份同步确认
✅ `<backup_cmd> → 目标路径`  
✅ `<backup_cmd> → 目标路径`  
✅ 关键文件 SHA256 **全部一致**（列出 3~5 个代表性文件名）

---

### 注意事项
- **Commit message 必须原样输出**：从 `git log -1 --format="%B"` 取，不做任何二次格式化（不要加"标题""正文"标签，不要改缩进）
- **第 2 块（完整 Commit Message）输出格式固定**：只用一个 `text` 代码块（` ```text ` 起始、` ``` ` 结束）直接贴原文，块内不加任何标签或缩进；**严禁把整个 5 块模板再包进外层代码块**——嵌套围栏会让聊天窗口把消息内容渲染吞掉
- **不要伪造输出**：所有数值、哈希、文件状态都必须来自 `git` 的真实输出
- **备份方式**：只同步本次 commit 的变更文件到备份目录，**不删目录、不碰 .git**，命令为：
  ```powershell
  python -c "
import subprocess, shutil, os
src=r'E:\code\python\quant'; dst=r'E:\git\python-quant'
# 1. 获取本次 commit 变更文件列表
r=subprocess.run(['git','-C',src,'diff','--name-only','HEAD~1','HEAD'],capture_output=True,text=True)
changed=[l for l in r.stdout.strip().split('\n') if l]
# 2. 获取被删除的文件列表
r2=subprocess.run(['git','-C',src,'diff','--name-only','--diff-filter=D','HEAD~1','HEAD'],capture_output=True,text=True)
deleted=set(l for l in r2.stdout.strip().split('\n') if l)
# 3. 同步修改/新增
for rel in changed:
    s=os.path.join(src,rel); d=os.path.join(dst,rel)
    if os.path.isfile(s):
        os.makedirs(os.path.dirname(d),exist_ok=True)
        shutil.copy2(s,d)
    elif rel in deleted and os.path.exists(d):
        os.remove(d)
print(f'synced {len(changed)} files, removed {len(deleted)} deleted')
"
  ```
  backup 目标固定 `E:\git\python-quant\`；备份目录有自己的 `.git`，**永远不要删整个目录**
- **核心原则：增量同步，其他不动**。只拷贝本次 commit 变更的文件过去，删除源仓库已删的文件；备份目录里的 `.git` 信息、untracked 文件、手动添加的内容全部保留
- **为什么不删整个目录**：备份目录是独立的 git 仓库（有自己的 `.git`），`shutil.rmtree` 会把备份仓库也干掉；之前的"精确匹配"方案过度设计了，用户只需要同步文件内容

## 备份同步规则

- **每次执行 git 提交前，必须先把本次修改的文件同步到备份目录 `e:\git\python-quant\` 下对应的位置**（保持与仓库相同的相对路径结构）
- 同步范围 = 本次提交涉及的全部文件（新增/修改）；删除的文件需从备份目录对应位置一并移除
- 备份同步完成后，再按上述 Git 提交规则请用户确认并提交
