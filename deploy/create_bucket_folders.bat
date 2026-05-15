@echo off
:: Expected input: A list of folder names passed as arguments

:loop
if "%~1"=="" goto :done
    set "FOLDER_NAME=%~1"

    echo [INFO] Checking for folder: %FOLDER_NAME%...
    :: Check if the "folder" (prefix) exists
    aws s3 ls "s3://%BUCKET_NAME%/%FOLDER_NAME%/" 2>nul

    if %ERRORLEVEL% neq 0 (
        echo [INFO] Folder '%FOLDER_NAME%' does not exist. Creating it...
        aws s3api put-object --bucket %BUCKET_NAME% --key %FOLDER_NAME%/ >nul
        if !ERRORLEVEL! equ 0 (
            echo [INFO] Folder '%FOLDER_NAME%' created successfully.
        ) else (
            echo [ERROR] Failed to create folder '%FOLDER_NAME%'.
        )
    ) else (
        echo [INFO] Folder '%FOLDER_NAME%' already exists.
    )

    :: Shift to the next argument and repeat
    shift
    goto :loop

:done
exit /b 0