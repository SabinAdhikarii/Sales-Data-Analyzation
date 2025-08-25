# Sales-Data-Analyzation

# Instacart Sales Data Analysis - ETL Pipeline & Dashboard

## Project Overview

This project is a complete ETL (Extract, Transform, Load) pipeline for analyzing Instacart market basket data. It includes:

1. **Data Extraction**: Downloads the Instacart dataset from Kaggle
2. **Data Transformation**: Processes and enriches the data using PySpark
3. **Data Loading**: Stores the transformed data in PostgreSQL
4. **Dashboard**: A Flask web application with visualizations and monitoring

## Prerequisites

Before setting up this project, ensure you have the following installed:

### 1. Python 3.8+
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3 python3-pip python3-venv

# macOS with Homebrew
brew install python

# Verify installation
python3 --version
pip3 --version
```

### 2. Java 8 or 11 (Required for PySpark)
```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk

# macOS with Homebrew
brew install openjdk@11

# Verify installation
java -version
```

### 3. PostgreSQL
```bash
# Ubuntu/Debian
sudo apt install postgresql postgresql-contrib

# macOS with Homebrew
brew install postgresql

# Start PostgreSQL service
sudo service postgresql start  # Ubuntu/Debian
brew services start postgresql # macOS
```

### 4. pgAdmin4 (Optional but recommended)
Download from [pgadmin.org](https://www.pgadmin.org/download/) or install via package manager:

```bash
# Ubuntu/Debian
sudo apt install pgadmin4

# macOS with Homebrew
brew install --cask pgadmin4
```

## Installation Guide

### 1. Clone the Repository
```bash
git clone <your-repository-url>
cd Sales-Data-Analyzation
```

### 2. Create a Virtual Environment
```bash
python3 -m venv pyspark.venv
source pyspark.venv/bin/activate  # On Windows: pyspark.venv\Scripts\activate
```

### 3. Install Python Dependencies
```bash
pip install --upgrade pip
pip install pyspark==3.3.0 flask==2.3.0 psycopg2-binary==2.9.6 kaggle==1.5.12
```

### 4. Set Up Kaggle API (For Data Extraction)
1. Create a Kaggle account at [kaggle.com](https://www.kaggle.com/)
2. Go to your account settings → API → Create New API Token
3. This will download a `kaggle.json` file
4. Place it in `~/.kaggle/kaggle.json` (on Windows: `C:\Users\<username>\.kaggle\kaggle.json`)
5. Set appropriate permissions:
```bash
chmod 600 ~/.kaggle/kaggle.json
```

### 5. Set Up PostgreSQL Database
```bash
# Connect to PostgreSQL
sudo -u postgres psql

# Create database and user
CREATE DATABASE instacart;
CREATE USER postgres WITH PASSWORD 'postgres';
ALTER USER postgres WITH SUPERUSER;
\q
```

### 6. Download PostgreSQL JDBC Driver
Download the PostgreSQL JDBC driver from [https://jdbc.postgresql.org/download/](https://jdbc.postgresql.org/download/) and place it in your PySpark jars directory or update the path in `/load/execute.py`.

## Project Structure

```
Sales-Data-Analyzation/
├── app.py                 # Flask web application
├── extract/execute.py     # Data extraction from Kaggle
├── transform/execute.py   # Data transformation with PySpark
├── load/execute.py        # Data loading to PostgreSQL
├── utility/utility.py     # Common utility functions
├── templates/index.html   # Dashboard frontend
├── logs/                  # Log files directory
└── .vscode/launch.json    # VS Code debug configurations
```

## Running the ETL Pipeline

### Option 1: Run Individual Steps Manually

1. **Extract Data**:
```bash
python extract/execute.py /home/sabin-adhikari/Project-Data/extraction
```

2. **Transform Data**:
```bash
python transform/execute.py /home/sabin-adhikari/Project-Data/extraction /home/sabin-adhikari/Project-Data/transform
```

3. **Load Data to PostgreSQL**:
```bash
python load/execute.py /home/sabin-adhikari/Project-Data/transform postgres postgres
```

### Option 2: Use VS Code Debug Configurations

The project includes pre-configured launch configurations in `.vscode/launch.json`. You can run each ETL step directly from VS Code's debug panel.

### Option 3: Run the Web Dashboard

```bash
python app.py
```

Then open your browser and navigate to `http://localhost:5001`

## Dashboard Features

The web dashboard provides:

1. **ETL Status Monitoring**: Real-time status of each ETL step
2. **Key Performance Indicators**: Total orders, unique users, products, etc.
3. **Data Visualizations**:
   - Orders per department
   - Top products
   - Orders per day of week
4. **Auto-refresh**: Data updates every 15 seconds
<img width="1850" height="826" alt="Screenshot from 2025-08-25 14-10-28" src="https://github.com/user-attachments/assets/f9b11193-0712-4108-be76-651052291e9b" />
<img width="1850" height="826" alt="Screenshot from 2025-08-25 14-10-46" src="https://github.com/user-attachments/assets/2b43e7c2-cf41-42d6-901b-dd4b172ec132" />


## Configuration

### Environment Variables

You can customize the application behavior using environment variables:

```bash
export PG_DB=instacart
export PG_USER=postgres
export PG_PW=postgres
export PG_HOST=localhost
export PG_PORT=5432
export LOGS_DIR=logs
export EXTRACT_OUT_DIR=/path/to/extraction
export TRANSFORM_OUT_DIR=/path/to/transform
```

### Database Configuration

Update the database connection settings in `/load/execute.py` and `app.py` if your PostgreSQL setup differs from the default.

## Troubleshooting

### Common Issues

1. **Kaggle API Errors**:
   - Ensure `kaggle.json` is in the correct location with proper permissions
   - Verify your Kaggle account has access to the dataset

2. **PySpark Memory Issues**:
   - Adjust memory settings in `transform/execute.py` if needed
   - Increase `spark.driver.memory` and `spark.executor.memory` for large datasets

3. **PostgreSQL Connection Issues**:
   - Verify PostgreSQL is running: `sudo service postgresql status`
   - Check database credentials in the code

4. **JDBC Driver Issues**:
   - Ensure the PostgreSQL JDBC driver path is correct in `/load/execute.py`
   - Download the appropriate version from https://jdbc.postgresql.org/download/

### Logs

Check the log files in the `logs/` directory for detailed error information:
- `extract.log`: Data extraction process logs
- `transform.log`: Data transformation process logs
- `load.log`: Data loading process logs

## Data Schema

The ETL pipeline creates the following tables in PostgreSQL:

1. **orders_fact**: Fact table with order details and product information
2. **products_dim**: Dimension table for products
3. **aisles_dim**: Dimension table for aisles
4. **departments_dim**: Dimension table for departments

## Monitoring

The dashboard automatically monitors:
- ETL process status through log files
- Data artifact presence in output directories
- Database table existence and data counts

## Support

For issues related to this project, please check:
1. The log files in the `logs/` directory
2. Database connectivity settings
3. Kaggle API configuration
4. PySpark and Java compatibility

## License

This project is for educational and demonstration purposes. The Instacart dataset is provided by Kaggle and subject to their terms of use.

You can download it from kaggle. 
