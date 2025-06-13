import boto3
import os
import yaml
from datetime import datetime
import glob
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

load_dotenv(override=True)

def load_config(yaml_file="config.yaml"):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

class S3Uploader:
    def __init__(self, config):
        self.config = config
        self.s3_config = config['reporting']['s3']
        
        self.s3_enabled = self.check_s3_credentials()
        
        if self.s3_enabled:
            try:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                    region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
                )
            except Exception as e:
                print(f"Failed to initialize S3 client: {e}")
                self.s3_enabled = False
        else:
            print("S3 credentials not found. Using local storage mode.")
            self.s3_client = None

    def check_s3_credentials(self):
        if os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('AWS_SECRET_ACCESS_KEY'):
            return True
        
        try:
            session = boto3.Session()
            credentials = session.get_credentials()
            if credentials and credentials.access_key and credentials.secret_key:
                return True
        except:
            pass
        
        return False

    def create_bucket_if_not_exists(self):
        if not self.s3_enabled:
            return False
            
        bucket_name = self.s3_config['bucket_name']
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                try:
                    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
                    if region == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': region}
                        )
                    return True
                except Exception as create_error:
                    print(f"Error creating bucket: {create_error}")
                    return False
            else:
                print(f"Error checking bucket: {e}")
                return False
        except NoCredentialsError:
            print("AWS credentials not found")
            self.s3_enabled = False
            return False

    def get_csv_files(self):
        reports_path = self.config['reporting']['output']['base_path']
        
        csv_files = glob.glob(os.path.join(reports_path, "**/*.csv"), recursive=True)
        
        actual_files = []
        for file_path in csv_files:
            if os.path.isfile(file_path):
                actual_files.append(file_path)
        
        if not actual_files:
            print(f"No CSV files found in {reports_path}")
            return []
        
        print(f"Found {len(actual_files)} CSV files to upload:")
        for file in actual_files:
            print(f"  - {file}")
        
        return actual_files
    
    def upload_file_to_s3(self, local_file_path, s3_key):
        bucket_name = self.s3_config['bucket_name']
        
        if os.path.isdir(local_file_path):
            print(f"Skipping directory: {local_file_path}")
            return False
        
        if not os.path.exists(local_file_path):
            print(f"File not found: {local_file_path}")
            return False
        
        try:
            metadata = {
                'upload_date': datetime.now().isoformat(),
                'source': 'telecom_reporting_system',
                'file_type': 'csv_report'
            }
            
            self.s3_client.upload_file(
                local_file_path, 
                bucket_name, 
                s3_key,
                ExtraArgs={
                    'Metadata': metadata,
                    'ServerSideEncryption': 'AES256' 
                }
            )
            
            print(f"Uploaded: {local_file_path} → s3://{bucket_name}/{s3_key}")
            return True
            
        except ClientError as e:
            print(f"Failed to upload {local_file_path}: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error uploading {local_file_path}: {e}")
            return False
    
    def generate_s3_key(self, local_file_path):
        file_name = os.path.basename(local_file_path)
        
        table_name = file_name.replace('.csv', '')
        s3_key = f"{table_name}.csv"
        
        return s3_key
    
    def upload_all_reports(self):
        
        if self.s3_enabled:
            if not self.create_bucket_if_not_exists():
                print("Falling back to local storage...")
                self.s3_enabled = False
        
        if not self.s3_enabled:
            print("S3 not available - reports remain in local directory")
            return

        csv_files = self.get_csv_files()
        
        if not csv_files:
            print("No files to upload")
            return
        
        successful_uploads = 0
        failed_uploads = 0
        
        for csv_file in csv_files:
            s3_key = self.generate_s3_key(csv_file)
            
            if self.upload_file_to_s3(csv_file, s3_key):
                successful_uploads += 1
            else:
                failed_uploads += 1

    def list_s3_objects(self, prefix=None):
        bucket_name = self.s3_config['bucket_name']
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name
            )
            
            if 'Contents' in response:
                print(f"\nObjects in s3://{bucket_name}/:")
                for obj in response['Contents']:
                    print(f"  - {obj['Key']} ({obj['Size']} bytes, {obj['LastModified']})")
            else:
                print(f"No objects found in s3://{bucket_name}/")
                
        except ClientError as e:
            print(f"Error listing S3 objects: {e}")

def main():
    config = load_config()
    
    try:
        uploader = S3Uploader(config)
        
        uploader.upload_all_reports()
        
        if config['reporting']['s3'].get('verify_uploads', True):
            uploader.list_s3_objects()
            
    except Exception as e:
        print(f"Error during S3 upload process: {e}")
        raise

if __name__ == "__main__":
    main()
