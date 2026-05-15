@echo off
:: Check if the .env file exists in the current directory
if not exist ".env" (
    echo [ERROR] .env file not found.
    exit /b 1
)

:: Read the file line by line
:: usebackq handles potential spaces in file paths
:: tokens=* ensures the entire line is captured
for /f "usebackq tokens=*" %%i in (".env") do (
    set "line=%%i"

    :: Skip empty lines and comments starting with #
    if not "!line!"=="" (
        if "!line:~0,1!" neq "#" (
            :: Assign the variable (Expected format: KEY=VALUE)
            set "%%i"
        )
    )
)

echo [INFO] Environment variables loaded successfully.
exit /b 0