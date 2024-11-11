@echo on
setlocal enabledelayedexpansion

rem Directory to the Anaconda .bat to enable it.
call "C:\ProgramData\anaconda3\Scripts\activate.bat" || pause
echo Anaconda batch file ran.

call conda activate ppp || pause
echo !conda_env_name! environment activated.

rem Getting the PATH to the Folder from the person who is running the .bat computer.
set output_dir=C:\Users\pcpp94\Documents
echo Folder Directory set to: %output_dir%

rem Input Directory is where the .bat file is.
set input_dir=C:\Users\pcpp94\Documents\ETL
echo Input Directory set to: %input_dir%

rem The variables for each directory structure to loop through.
set compiled_outputs_dirs=webscraping_asp.net_form meteostat pcweather_f demanda_limpia web_scraping_jwt_complex_auth
set outputs_dirs=demanda_from_messy_excels_thefuzz demanda_geografia parsing_wrongly_formatted_excels arcgis_api_data_parsing_double_authentication

rem Looping
for %%n in (%compiled_outputs_dirs%) do (
    xcopy "%input_dir%\%%n\compiled_outputs\*" "%output_dir%\ETL_tables\%%n\" /s /e /y
    echo Copied %%n outputs.  
)
for %%n in (%outputs_dirs%) do (
    xcopy "%input_dir%\%%n\outputs\*" "%output_dir%\ETL_tables\%%n\" /s /e /y
    echo Copied %%n outputs.  
)

echo ETL Outputs Copied succesfully
pause