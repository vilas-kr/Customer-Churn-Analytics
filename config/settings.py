import os
from dotenv import load_dotenv

load_dotenv()
 
BUCKET_NAME = os.getenv('BUCKET_NAME')
WORK_GROUP_NAME = os.getenv('WORK_GROUP_NAME')
DATABASE = os.getenv('DATABASE')
IAM_ROLE_ARN = os.getenv('IAM_ROLE_ARN')