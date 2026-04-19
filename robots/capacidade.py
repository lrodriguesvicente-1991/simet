# =====================================================================
# ARQUIVO: robots/capacidade.py
# MODULO: Detector de capacidade de hardware para escolha de modo.
#
# Decide entre 'ia' (com GPU suficiente) e 'deterministico' (CPU).
# A IA (Ollama) em CPU com modelos pequenos (3b) tem aritmetica fraca
# e trava o throughput; no servidor com GPU >=8GB, 7b/14b sao precisos
# e rapidos. O modo deterministico usa dataLayer + regex + dicionario
# do banco e nao chama LLM.
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

    vram = _detectar_nvidia_vram_mb()
    if vram and vram >= 8000:
        return {"modo": "ia", "motivo": f"GPU NVIDIA detectada ({vram} MB)", "vram_mb": vram}
    if vram:
        return {"modo": "deterministico", "motivo": f"GPU insuficiente ({vram} MB < 8000)", "vram_mb": vram}
    return {"modo": "deterministico", "motivo": "sem GPU NVIDIA detectada", "vram_mb": None}


if __name__ == "__main__":
    info = detectar_modo()
    print(f"modo={info['modo']} | motivo={info['motivo']} | vram_mb={info['vram_mb']}")
