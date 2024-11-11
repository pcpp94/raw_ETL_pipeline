@echo on
setlocal enabledelayedexpansion

rem Get the current date in YYYY-MM-DD format
for /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set current_date=%%c-%%a-%%b)

rem Get the current time in HH-MM-SS format (remove milliseconds by taking only the first 8 characters)
set current_time=%time:~0,8%
set current_time=%current_time::=-%

rem Getting the batch file name without extension and path
set "script_name=%~n0"

rem Getting the directory where the batch file is located
set "script_dir=%~dp0"

rem Create the 'bat_logs' directory if it doesn't exist
if not exist "%script_dir%bat_logs" (
    mkdir "%script_dir%bat_logs"
)

rem Create log file name inside 'bat_logs' directory with current date, time, and script name
set log_file=%script_dir%bat_logs\%current_date%_%current_time%_%script_name%.log

rem Create python file variable that is going to be ran.
set python_file=%script_dir%run_daily_etl_pipeline.py

rem Clear the log file at the start
echo Logging ETL process started at %date% %time% > "%log_file%"

rem ETL 1: Running all the python packages:
echo ETL 1: Running Python programs. >> "%log_file%" 2>&1

call "C:\ProgramData\anaconda3\Scripts\activate.bat" >> "%log_file%" 2>&1 || pause
echo Anaconda batch file ran. >> "%log_file%" 2>&1

call conda activate ppp >> "%log_file%" 2>&1 || pause

rem Only uncomment if the requirements need to be installed.
echo Checking required packages are installed...
pip install -r "requirements.txt" || goto :error1

echo Starting python Script execution...
python -u "%python_file%" >> "%log_file%" 2>&1 || goto :error2

goto :end

:error1
Error installing the required packages, exiting... >> "%log_file%" 2>&1
exit 1

:error2
echo An error occured running the ETL programs, exiting... >> "%log_file%" 2>&1
exit 1

:end
echo All scripts executed successfully. >> "%log_file%" 2>&1
exit 1