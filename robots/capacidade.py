# =====================================================================
# ARQUIVO: robots/capacidade.py
# MODULO: Detector de capacidade de hardware para escolha de modo.
#
# Decide entre 'ia' (com GPU suficiente) e 'deterministico' (CPU).
#
# Criterio: GPU dedicada com VRAM >= 8 GB, de qualquer marca suportada
# pelo Ollama oficial (NVIDIA CUDA / AMD ROCm). iGPUs (Intel Iris/Xe/Arc
# integrada, Radeon Graphics, UHD, etc.) NAO contam, mesmo que o BIOS
# aloque muita RAM compartilhada -- o Ollama oficial nao tem backend
# Intel e as iGPUs AMD nao passam no runtime do Ollama.
#
# Em CPU o modo IA trava o throughput, entao a heuristica privilegia
# voltar para 'deterministico' (regex + dicionario + dataLayer).
# =====================================================================
import json
import os
import subprocess


VRAM_MINIMA_MB = 8000


def _detectar_nvidia_vram_mb():
    """Retorna VRAM total (MB) da primeira GPU NVIDIA, ou None se nao houver.
    nvidia-smi so aparece quando ha driver CUDA instalado -- se existir, ja
    e garantia de GPU dedicada suportada pelo Ollama."""
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        if out.returncode != 0:
            return None
        linha = out.stdout.strip().splitlines()[0].strip() if out.stdout else ""
        return int(linha) if linha.isdigit() else None
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


def _detectar_amd_dedicada_linux():
    """Retorna {'nome': str, 'vram_mb': int} para a primeira AMD ROCm >=8 GB."""
    if os.name == "nt":
        return None
    try:
        out = subprocess.run(
            ["rocm-smi", "--showproductname", "--showmeminfo", "vram", "--json"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        if out.returncode != 0 or not out.stdout:
            return None
        data = json.loads(out.stdout)
        for _gpu_id, info in data.items():
            nome = info.get("Card Model") or info.get("Card series") or "AMD GPU"
            vram_total = info.get("VRAM Total Memory (B)") or info.get("vram total") or 0
            vram_mb = int(vram_total) // (1024 * 1024) if vram_total else 0
            if vram_mb >= VRAM_MINIMA_MB:
                return {"nome": str(nome).strip(), "vram_mb": vram_mb}
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError, ValueError, json.JSONDecodeError):
        return None


def _nome_indica_dedicada(nome):
    """Regras de nome para separar dGPU de iGPU. Usado no Windows quando so
    temos nome + VRAM relatada (o Windows reporta VRAM alocada no BIOS pra
    iGPU, o que pode passar de 8 GB sem ser GPU dedicada)."""
    baixo = nome.lower()
    # NVIDIA: toda placa nomeada e dedicada no desktop (sem iGPU NVIDIA atual)
    if any(tag in baixo for tag in ("nvidia", "geforce", "rtx", "gtx", "quadro", "tesla", "titan")):
        return True
    # Intel Arc: A-series e dedicada; "Arc Graphics" sem A<n> e iGPU (Meteor/Arrow Lake)
    if "arc" in baixo and any(f"a{n}" in baixo for n in range(300, 800)):
        return True
    # AMD: "RX", "Pro", "VII", "Instinct" sao dedicadas. "Radeon Graphics"/"Vega Graphics"
    # sem sufixo de modelo sao iGPU dos Ryzen APU.
    if "radeon" in baixo or "amd" in baixo:
        if any(tag in baixo for tag in (" rx ", "rx ", "radeon pro", "radeon vii", "radeon ai", "instinct")):
            return True
        return False
    return False


def _detectar_gpu_dedicada_windows():
    """Retorna {'nome': str, 'vram_mb': int} para a primeira GPU dedicada >=8 GB.

    Le o registro `HardwareInformation.qwMemorySize` (QWORD, 64 bits com a VRAM
    fisica) de cada adaptador de video -- o AdapterRAM do WMI e DWORD e estoura
    em 4 GB.
    """
    if os.name != "nt":
        return None
    script = (
        "$ErrorActionPreference='SilentlyContinue';"
        "$base='HKLM:\\SYSTEM\\CurrentControlSet\\Control\\Class\\{4d36e968-e325-11ce-bfc1-08002be10318}';"
        "Get-ChildItem $base | ForEach-Object {"
        "  $k=$_; $nome=$k.GetValue('DriverDesc');"
        "  $vram=$k.GetValue('HardwareInformation.qwMemorySize');"
        "  if (-not $vram) { $vram=$k.GetValue('HardwareInformation.MemorySize') }"
        "  if ($nome -and $vram) { [PSCustomObject]@{Name=$nome;VramBytes=[int64]$vram} }"
        "} | ConvertTo-Json -Compress"
    )
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            capture_output=True, text=True, timeout=8, check=False,
        )
        if out.returncode != 0 or not out.stdout.strip():
            return None
        data = json.loads(out.stdout)
        controllers = data if isinstance(data, list) else [data]
        for c in controllers:
            nome = (c.get("Name") or "").strip()
            vram_bytes = int(c.get("VramBytes") or 0)
            vram_mb = vram_bytes // (1024 * 1024)
            if vram_mb < VRAM_MINIMA_MB:
                continue
            if not _nome_indica_dedicada(nome):
                continue
            return {"nome": nome, "vram_mb": vram_mb}
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError, ValueError, json.JSONDecodeError):
        return None


def detectar_modo(forcar=None):
    """Retorna dict com {'modo': 'ia'|'deterministico', 'motivo': str, 'vram_mb': int|None}.

    forcar: se receber 'ia' ou 'deterministico', ignora deteccao.
    Tambem respeita env SIMET_MODO (auto|ia|deterministico, default auto).
    """
    escolha = (forcar or os.getenv("SIMET_MODO", "auto")).strip().lower()

    if escolha == "ia":
        return {"modo": "ia", "motivo": "forcado por SIMET_MODO=ia", "vram_mb": None}
    if escolha == "deterministico":
        return {"modo": "deterministico", "motivo": "forcado por SIMET_MODO=deterministico", "vram_mb": None}

    # 1. NVIDIA via nvidia-smi (tem prioridade -- driver CUDA e a referencia)
    vram = _detectar_nvidia_vram_mb()
    if vram and vram >= VRAM_MINIMA_MB:
        return {"modo": "ia", "motivo": f"GPU NVIDIA detectada ({vram} MB)", "vram_mb": vram}
    if vram:
        return {
            "modo": "deterministico",
            "motivo": f"GPU NVIDIA insuficiente ({vram} MB < {VRAM_MINIMA_MB})",
            "vram_mb": vram,
        }

    # 2. AMD via rocm-smi (Linux)
    amd = _detectar_amd_dedicada_linux()
    if amd:
        return {
            "modo": "ia",
            "motivo": f"GPU AMD ROCm detectada ({amd['nome']}, {amd['vram_mb']} MB)",
            "vram_mb": amd["vram_mb"],
        }

    # 3. Windows: qualquer dGPU (NVIDIA, AMD RX/Pro, Intel Arc A-series) com VRAM suficiente
    win = _detectar_gpu_dedicada_windows()
    if win:
        return {
            "modo": "ia",
            "motivo": f"GPU dedicada detectada ({win['nome']}, {win['vram_mb']} MB)",
            "vram_mb": win["vram_mb"],
        }

    return {"modo": "deterministico", "motivo": "sem GPU dedicada >=8 GB detectada", "vram_mb": None}


if __name__ == "__main__":
    info = detectar_modo()
    print(f"modo={info['modo']} | motivo={info['motivo']} | vram_mb={info['vram_mb']}")
