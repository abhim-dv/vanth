@echo off
set "VANTH_HOME=%USERPROFILE%\.vanth"
uv run --directory "%~dp0.." vanthd
