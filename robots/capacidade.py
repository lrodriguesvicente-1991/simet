# =====================================================================
# ARQUIVO: robots/capacidade.py
# MODULO: Detector de capacidade de hardware para escolha de modo.
#
# Decide entre 'ia' (Ollama) e 'deterministico' (CPU, regex + dicionario).
#
# Suporte de GPU do Ollama:
#   - NVIDIA (CUDA): build oficial, >=8 GB VRAM recomendado
#   - Intel Arc/Xe/Iris (SYCL): requer o fork IPEX-LLM da Intel
#     (github.com/intel/ipex-llm), o build oficial do Ollama ignora Intel
#   - AMD (ROCm): apenas no Linux
#
# A heuristica aqui recomenda 'ia' quando acha GPU suportada. Sempre da
# pra forcar com SIMET_MODO=ia/deterministico.
# =====================================================================
import os
import subprocess


def _detectar_nvidia_vram_mb():
    """Retorna VRAM total (MB) da primeira GPU NVIDIA, ou None se nao houver."""
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


def _detectar_gpu_intel_windows():
    """Retorna {'nome': str, 'dedicada': bool} se houver GPU Intel, ou None.

    'dedicada' True para Arc A-series (A3xx-A7xx), False para iGPU
    (Arc Xe integrada do Meteor/Arrow Lake, Iris Xe, UHD, HD Graphics).
    Usa PowerShell Get-CimInstance (wmic esta deprecado no Win 11).
    """
    if os.name != "nt":
        return None
    try:
        out = subprocess.run(
            [
                "powershell", "-NoProfile", "-Command",
                "Get-CimInstance Win32_VideoController | Select-Object -ExpandProperty Name",
            ],
            capture_output=True, text=True, timeout=8, check=False,
        )
        if out.returncode != 0 or not out.stdout:
            return None
        for linha in out.stdout.splitlines():
            nome = linha.strip()
            if not nome:
                continue
            baixo = nome.lower()
            if "intel" not in baixo:
                continue
            # Arc dedicada: "Intel(R) Arc(TM) A750 Graphics"
            # iGPU Arc (Meteor/Arrow Lake): "Intel(R) Arc(TM) Graphics" (sem A<n>)
            dedicada = "arc" in baixo and any(
                f"a{n}" in baixo for n in range(300, 800)
            )
            if "arc" in baixo or "iris" in baixo or "uhd" in baixo or " xe " in baixo:
                return {"nome": nome, "dedicada": dedicada}
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
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

    # NVIDIA tem prioridade (suporte nativo do Ollama)
    vram = _detectar_nvidia_vram_mb()
    if vram and vram >= 8000:
        return {"modo": "ia", "motivo": f"GPU NVIDIA detectada ({vram} MB)", "vram_mb": vram}
    if vram:
        return {"modo": "deterministico", "motivo": f"GPU NVIDIA insuficiente ({vram} MB < 8000)", "vram_mb": vram}

    # Intel Arc / Xe: requer IPEX-LLM Ollama para aproveitar a GPU.
    # O Ollama oficial nao usa Intel, entao rodar IA cai em CPU (lento).
    intel = _detectar_gpu_intel_windows()
    if intel:
        tipo = "dedicada" if intel["dedicada"] else "integrada"
        return {
            "modo": "ia",
            "motivo": f"GPU Intel {tipo} detectada ({intel['nome']}) - requer IPEX-LLM Ollama",
            "vram_mb": None,
        }

    return {"modo": "deterministico", "motivo": "sem GPU suportada detectada", "vram_mb": None}


if __name__ == "__main__":
    info = detectar_modo()
    print(f"modo={info['modo']} | motivo={info['motivo']} | vram_mb={info['vram_mb']}")
