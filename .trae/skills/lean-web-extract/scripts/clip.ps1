param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('backup', 'save', 'restore')]
    [string]$Action,
    [string]$OutFile,
    [string]$BackupFile = (Join-Path $env:TEMP 'clip_skill_backup.txt')
)
# 剪贴板三步工具：抓取前 backup 备份现状，页面复制后 save 把正文直落文件，收尾 restore 恢复原剪贴板并删除备份
# 全程正文只走文件不进模型上下文；save 产物仅输出字节数与字符数，禁止回显正文
Add-Type -AssemblyName System.Windows.Forms
function Read-Clip {
    if ([System.Windows.Forms.Clipboard]::ContainsText()) { [System.Windows.Forms.Clipboard]::GetText() } else { '' }
}
switch ($Action) {
    'backup' {
        $t = Read-Clip
        [System.IO.File]::WriteAllText($BackupFile, $t, (New-Object System.Text.UTF8Encoding($false)))
        "backup ok chars=$($t.Length) file=$BackupFile"
    }
    'save' {
        if (-not $OutFile) { throw 'save requires -OutFile' }
        $t = Read-Clip
        $dir = Split-Path -Parent $OutFile
        if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
        [System.IO.File]::WriteAllText($OutFile, $t, (New-Object System.Text.UTF8Encoding($true)))
        $fi = Get-Item $OutFile
        "saved bytes=$($fi.Length) chars=$($t.Length) file=$($fi.FullName)"
    }
    'restore' {
        if (Test-Path $BackupFile) {
            $b = [System.IO.File]::ReadAllText($BackupFile)
            if ($b) { [System.Windows.Forms.Clipboard]::SetText($b) }
            Remove-Item $BackupFile -Force
            if ($b) { "restore ok chars=$($b.Length) backup deleted" } else { "backup empty, restore skipped, backup deleted" }
        } else {
            "no backup file at $BackupFile"
        }
    }
}
