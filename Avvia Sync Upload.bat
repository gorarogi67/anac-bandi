@echo off
cd /d "%~dp0"
python sync_upload_gui.py
if %errorlevel% neq 0 (
    echo.
    echo ERRORE: impossibile avviare la GUI.
    echo Assicurati che Python e le dipendenze siano installate.
    pause
)
