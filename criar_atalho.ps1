# =====================================================================
# Cria um atalho do SIMET na area de trabalho do usuario.
#
# Uso (clique direito no arquivo -> Executar com PowerShell)
#  ou no terminal:
#    powershell -ExecutionPolicy Bypass -File criar_atalho.ps1
# =====================================================================

$ErrorActionPreference = 'Stop'
$raiz = Split-Path -Parent $MyInvocation.MyCommand.Path
$batPath = Join-Path $raiz 'iniciar_simet.bat'
$pngPath = Join-Path $raiz 'frontend_react\public\favicon.png'
$icoPath = Join-Path $raiz 'simet.ico'

if (-not (Test-Path $batPath)) {
    Write-Host "[ERRO] iniciar_simet.bat nao encontrado em $batPath" -ForegroundColor Red
    exit 1
}

# 1. Converte favicon.png -> simet.ico (precisa do System.Drawing) se ainda nao existir
if (-not (Test-Path $icoPath)) {
    if (Test-Path $pngPath) {
        try {
            Add-Type -AssemblyName System.Drawing
            $bmp = [System.Drawing.Bitmap]::FromFile($pngPath)
            $icon = [System.Drawing.Icon]::FromHandle($bmp.GetHicon())
            $fs = [System.IO.File]::Create($icoPath)
            $icon.Save($fs)
            $fs.Close()
            $bmp.Dispose()
            Write-Host "[OK] Icone gerado em $icoPath" -ForegroundColor Green
        } catch {
            Write-Host "[AVISO] Nao foi possivel converter favicon.png em .ico ($_). O atalho usara o icone padrao." -ForegroundColor Yellow
            $icoPath = $null
        }
    } else {
        Write-Host "[AVISO] favicon.png nao encontrado. O atalho usara o icone padrao." -ForegroundColor Yellow
        $icoPath = $null
    }
}

# 2. Cria o atalho na Area de Trabalho
$desktop = [Environment]::GetFolderPath('Desktop')
$lnkPath = Join-Path $desktop 'SIMET.lnk'

$shell = New-Object -ComObject WScript.Shell
$lnk = $shell.CreateShortcut($lnkPath)
$lnk.TargetPath = $batPath
$lnk.WorkingDirectory = $raiz
$lnk.Description = 'SIMET - Sistema de Inteligencia de Mercado de Terras (INCRA)'
$lnk.WindowStyle = 1
if ($icoPath -and (Test-Path $icoPath)) {
    $lnk.IconLocation = $icoPath
}
$lnk.Save()

Write-Host ""
Write-Host "[OK] Atalho criado em: $lnkPath" -ForegroundColor Green
Write-Host "      Duplo-clique no atalho para iniciar o SIMET." -ForegroundColor Gray
