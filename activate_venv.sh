#!/bin/bash

# Telecom Project Virtual Environment Activation Script
echo " Activating Telecom Project Virtual Environment..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo " Virtual environment not found!"
    echo " Creating virtual environment..."
    python3 -m venv venv
    echo " Virtual environment created!"
fi

# Activate virtual environment
source venv/bin/activate

# Check if activation was successful
if [ "$VIRTUAL_ENV" != "" ]; then
    echo " Virtual environment activated successfully!"
    echo " Virtual Environment: $VIRTUAL_ENV"
    echo " Python Version: $(python --version)"
    echo ""
    echo " Installing/Updating required packages..."
    
    # Install required packages
    pip install --upgrade pip
    pip install pyspark kafka-python psycopg2-binary pyyaml pandas boto3 python-dotenv
    
    echo ""
    echo "🎉 Environment ready for Telecom Project!"
    echo ""
    echo "Available commands:"
    echo "  - Data Generation: cd 1_Data-Generation-Phase && python3 producer.py"
    echo "  - Customer Generation: cd 1_Data-Generation-Phase && python3 customer_generator.py" 
    echo "  - Mediation: cd 2_Mediation-Phase && python3 consumer_mediation.py"
    echo "  - Rating: cd 3_Rating-Engine-Phase && python3 batch_rating.py"
    echo "  - Billing: cd 4_Billing_Engine_Phase && python3 batch_billing.py"
    echo "  - Reporting: cd 5_Reporting_Phase && python3 batch_reporting.py"
    echo "  - S3 Upload: cd 6_Reporting_Phase && python3 s3_uploader.py"
    echo ""
    echo " To deactivate: type 'deactivate'"
    echo " To activate again: source activate_venv.sh"
else
    echo " Failed to activate virtual environment!"
    exit 1
fi
