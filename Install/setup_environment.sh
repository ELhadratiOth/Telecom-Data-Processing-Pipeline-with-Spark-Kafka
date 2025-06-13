#!/bin/bash

echo "# Check if requirements.txt exists and install dependencies
REQUIREMENTS_FILE="$USER_HOME/Desktop/spark/requirements.txt"
if [ -f "$REQUIREMENTS_FILE" ]; then
    echo "Installing Python dependencies from requirements.txt..."
    pip install --upgrade pip
    pip uninstall -y pyspark  # Remove any existing PySpark version
    pip install -r "$REQUIREMENTS_FILE"
    echo "Dependencies installed successfully"
else
    echo "Warning: requirements.txt not found. Installing basic dependencies..."
    pip install --upgrade pip
    pip uninstall -y pyspark  # Remove any existing PySpark version
    pip install pyspark==3.5.5 kafka-python==2.0.2 psycopg2-binary==2.9.9 PyYAML==6.0.1 python-dotenv==1.0.0 jinja2==3.1.2 weasyprint==60.0
fi

# Set Spark environment variables
echo "Configuring Spark environment..."
export SPARK_HOME=/home/othman/spark
export PYSPARK_PYTHON="$VENV_PATH/bin/python3"
export PYSPARK_DRIVER_PYTHON="$VENV_PATH/bin/python3"
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATHelecom Pipeline Environment"

# Get the current user's home directory (works even with sudo)
USER_HOME=$(eval echo ~${SUDO_USER:-$USER})
VENV_PATH="$USER_HOME/Desktop/spark/venv"

# Check if virtual environment exists, create if not
if [ ! -d "$VENV_PATH" ]; then
    echo "Virtual environment not found. Creating new virtual environment..."
    python3 -m venv "$VENV_PATH"
    echo "Virtual environment created at $VENV_PATH"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source "$VENV_PATH/bin/activate"

# Check if requirements.txt exists and install dependencies
REQUIREMENTS_FILE="$USER_HOME/Desktop/spark/requirements.txt"
if [ -f "$REQUIREMENTS_FILE" ]; then
    echo "Installing Python dependencies from requirements.txt..."
    pip install --upgrade pip
    pip install -r "$REQUIREMENTS_FILE"
    echo "Dependencies installed successfully"
else
    echo "Warning: requirements.txt not found. Installing basic dependencies..."
    pip install --upgrade pip
    pip install -r requirements.txt 
fi

# Set Spark environment variables
echo "Configuring Spark environment..."
export SPARK_HOME=/home/othman/spark
export PYSPARK_PYTHON=python3
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH


echo "Configuring Java environment..."
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Add Spark binaries to PATH
export PATH=$SPARK_HOME/bin:$PATH

# Set additional Spark configurations
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 pyspark-shell"

echo "Environment configured successfully!"


# Verify Java version
java_version=$(java -version 2>&1 | head -n 1)
echo "Java version: $java_version"

# Verify Spark installation
if command -v spark-submit &> /dev/null; then
    echo "Spark is ready: $(spark-submit --version 2>&1 | head -n 1)"
else
    echo "Warning: Spark command not found in PATH"
fi

echo "Environment setup complete!"

