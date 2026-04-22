@echo off
echo ========================================
echo Retail Sales Analytics Pipeline
echo ========================================
echo.

echo [1/5] Generating synthetic retail data...
python src/01_data_generation.py
if %errorlevel% neq 0 (
    echo ERROR: Data generation failed!
    pause
    exit /b %errorlevel%
)
echo.

echo [2/5] Running ETL pipeline...
python src/02_etl_pipeline.py
if %errorlevel% neq 0 (
    echo ERROR: ETL pipeline failed!
    pause
    exit /b %errorlevel%
)
echo.

echo [3/5] Executing SQL analytics...
python src/03_sql_analytics.py
if %errorlevel% neq 0 (
    echo ERROR: SQL analytics failed!
    pause
    exit /b %errorlevel%
)
echo.

echo [4/5] Generating visualizations...
python src/04_visualization.py
if %errorlevel% neq 0 (
    echo ERROR: Visualization failed!
    pause
    exit /b %errorlevel%
)
echo.

echo [5/5] Exporting Power BI data...
python src/05_power_bi_export.py
if %errorlevel% neq 0 (
    echo ERROR: Power BI export failed!
    pause
    exit /b %errorlevel%
)
echo.

echo ========================================
echo Pipeline completed successfully!
echo ========================================
echo.
echo Check the following outputs:
echo - Database: data/retail_analytics.db
echo - Reports: reports/executive_summary.txt
echo - Charts: reports/charts/
echo - Power BI: data/processed/*.csv
echo.
pause