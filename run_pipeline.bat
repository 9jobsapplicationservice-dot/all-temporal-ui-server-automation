@echo off
cd /d "%~dp0"
python -m pipeline.launch --auto-run --no-ui
