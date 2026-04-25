@echo off
REM ====================================================================
REM SIMET - Inicializador
REM Sobe a API + serve o front buildado num unico processo (porta 8000)
REM e abre o navegador automaticamente.
REM
REM Duplo-clique para rodar. Para parar: feche esta janela.
REM ====================================================================

setlocal enableextensions
cd /d "%~dp0"
title SIMET

set "PORTA=8000"
set "URL=http://localhost:%PORTA%"
set "VENV_PY=venv\Scripts\python.exe"
set "DIST=frontend_react\dist"

echo.
echo ====================================================================
echo  SIMET - Iniciando
echo ====================================================================
echo.

REM ----- 1. Abre o splash IMEDIATAMENTE para feedback visual -----
if exist "splash.html" (
    echo [1/5] Abrindo tela de carregamento...
    start "" "%~dp0splash.html"
) else (
    echo [1/5] splash.html nao encontrado, pulando.
)

REM ----- 2. Verifica venv do Python -----
echo [2/5] Verificando ambiente Python...
if not exist "%VENV_PY%" (
    echo.
    echo [ERRO] venv nao encontrada em "%VENV_PY%".
    echo        Crie com: python -m venv venv
    echo        E instale com: venv\Scripts\pip install -r requirements.txt
    echo.
    pause
    exit /b 1
)

REM Confirma que uvicorn esta instalado
"%VENV_PY%" -c "import uvicorn" >nul 2>&1
if errorlevel 1 (
    echo.
    echo [ERRO] uvicorn nao instalado na venv.
    echo        Instale com: venv\Scripts\pip install -r requirements.txt
    echo.
    pause
    exit /b 1
)

REM ----- 3. Build do front se ainda nao existe ou se ha codigo mais novo -----
echo [3/5] Verificando build do front...
set "PRECISA_BUILD=0"
if not exist "%DIST%\index.html" (
    set "PRECISA_BUILD=1"
    echo       Build inexistente — vou construir.
) else (
    REM Compara timestamp de dist/index.html com qualquer arquivo de fonte
    REM (src/, public/, index.html, package.json, vite.config.ts, tsconfig*).
    REM Se algo no fonte for mais recente, rebuild.
    powershell -NoProfile -Command "$dist=(Get-Item 'frontend_react\dist\index.html').LastWriteTime; $alvos=@('frontend_react\src','frontend_react\public','frontend_react\index.html','frontend_react\package.json','frontend_react\vite.config.ts','frontend_react\tsconfig.json','frontend_react\tsconfig.app.json'); $novo = Get-ChildItem $alvos -Recurse -File -ErrorAction SilentlyContinue | Where-Object { $_.LastWriteTime -gt $dist } | Select-Object -First 1; if ($novo) { exit 1 } else { exit 0 }"
    if errorlevel 1 (
        set "PRECISA_BUILD=1"
        echo       Codigo-fonte mais recente que o build — vou reconstruir.
    ) else (
        echo       OK ^(build atualizado em %DIST%^)
    )
)

if "%PRECISA_BUILD%"=="1" (
    echo       Construindo agora ^(uns 30-60s^)...
    pushd frontend_react
    if not exist "node_modules" (
        echo       Instalando dependencias do npm...
        call npm install
        if errorlevel 1 (
            echo [ERRO] npm install falhou.
            popd
            pause
            exit /b 1
        )
    )
    call npm run build
    if errorlevel 1 (
        echo [ERRO] npm run build falhou.
        popd
        pause
        exit /b 1
    )
    popd
    echo       Build concluido.
)

REM ----- 4. Mata uvicorn antigo na mesma porta (se houver) -----
echo [4/5] Liberando porta %PORTA% se ocupada...
for /f "tokens=5" %%P in ('netstat -ano ^| findstr ":%PORTA% " ^| findstr LISTENING') do (
    echo       Encerrando processo PID %%P na porta %PORTA%...
    taskkill /F /PID %%P >nul 2>&1
)

REM ----- 5. Sobe o uvicorn EM PRIMEIRO PLANO -----
echo [5/5] Iniciando servidor em %URL%
echo.
echo ====================================================================
echo  Servidor rodando. A tela de carregamento abre o navegador
echo  automaticamente quando o servidor responder.
echo  Para encerrar tudo: feche esta janela ^(Ctrl+C ou X^).
echo ====================================================================
echo.

"%VENV_PY%" -m uvicorn api:app --host 127.0.0.1 --port %PORTA%

REM Se chegou aqui, uvicorn parou. Mantem janela aberta para ler erros.
echo.
echo [SIMET] Servidor encerrado.
pause
