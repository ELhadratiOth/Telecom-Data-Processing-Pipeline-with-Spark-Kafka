INSTALL FOLDER - README

This folder contains installation and activation scripts for the Spark telecom pipeline.

FILES IN THIS FOLDER:

1. steps.txt
   - Contains Kafka installation and basic commands
   - Includes download, extraction, and server startup commands
   - Has topic management commands (create, list, delete)
   - Troubleshooting commands for port conflicts

2. setup_environment.sh
   - Complete environment setup script
   - Activates virtual environment and sets Spark variables
   - Configures Java paths and Python settings
   - One-command setup for complete environment configuration
   - Recommended for daily use

3. Py_scripts/
   con.py - Simple Kafka consumer script for testing
   prod.py - Simple Kafka producer script for testing
   s3_uploader.py - AWS S3 upload utility script
   topic.py - Kafka topic creator script



NOTES:
------
- Always activate virtual environment before running Spark applications
- Ensure Kafka is running before starting data pipeline
