: @echo off

: Check if cl is available and in x64 mode
where cl
if %errorlevel% neq 0 (
    echo "cl is not available. Please launch the script from a Visual Studio Developer Command Prompt (x64)."
    exit /b 1
)
cl /? | findstr /i "x64"
if %errorlevel% neq 0 (
    echo cl is not in x64 mode
    exit /b 1
)

: Make the bin, lib, and obj directories if they do not exist
if not exist bin mkdir bin
if not exist lib mkdir lib
if not exist obj mkdir obj

: Download SQLite
set INCLUDE_DIRS=lib\sqlite-amalgamation-3500400
if not exist %INCLUDE_DIRS%\sqlite3.h (
    if not exist lib\sqlite-amalgamation.zip (
        curl -L -o lib\sqlite-amalgamation.zip https://www.sqlite.org/2025/sqlite-amalgamation-3500400.zip
    )
    tar xf lib\sqlite-amalgamation.zip -C lib
)
set LIBS=lib\sqlite-dll-win-x64-3500400\sqlite3.lib
if not exist %LIBS% (
    if not exist lib\sqlite-dll-win-x64-3500400.zip (
        curl -L -o lib\sqlite-dll-win-x64-3500400.zip https://www.sqlite.org/2025/sqlite-dll-win-x64-3500400.zip
    )
    mkdir lib\sqlite-dll-win-x64-3500400
    tar xf lib\sqlite-dll-win-x64-3500400.zip -C lib\sqlite-dll-win-x64-3500400
    LIB /DEF:lib\sqlite-dll-win-x64-3500400\sqlite3.def /MACHINE:X64 /OUT:%LIBS%
    copy lib\sqlite-dll-win-x64-3500400\sqlite3.dll bin
)

: Define the targets
: set TARGETS=schema1 schema4 schema7 schema10 pragmas parallel
set TARGETS=parallel
set LINKFLAGS=/MACHINE:X64
set COMPILEFLAGS=/std:c++20 /EHsc /favor:AMD64 /O2 /openmp

: For each target, compile using cl
for %%t in (%TARGETS%) do (
    cl /c /Foobj\%%t.obj /I %INCLUDE_DIRS% /I cpp\ cpp\%%t.cpp %COMPILEFLAGS%
    link /OUT:bin\%%t.exe obj\%%t.obj %LIBS% %LINKFLAGS%
)