This repository contains AWS Lambda code designed for efficient handling of large log streams, specifically focused on log aggregation and analytics. The code processes real-time log batches subscribed from CloudWatch Logs Groups, providing a scalable and cost-effective solution. The Lambda function seamlessly integrates with AWS services and performs batch processing to extract valuable insights from raw log data.

#Functionality:

Log Stream Unzipping: The unzip_stream function extracts and decodes log data received in gzip format from CloudWatch Logs.
Log Data Transformation: The transform function processes and transforms the log events to meaningful insights. It parses SQL queries, extracts user details, timestamps, and other attributes to generate actionable intelligence.
AWS EC2 Metadata Extraction: The code fetches EC2 metadata against host IP addresses from AWS EC2 using the extract_ec2_against_host and fetch_ec2_metadata_against functions. The metadata includes team names and environment names.
Log Data Aggregation at Database Level: The code implements a Composite Key-based approach for aggregating log data at the database level. This allows updating query counts and essential attributes in real-time, enabling seamless data-driven decision-making.
Data Dump to Database: The dump_query_stats_to function dumps the processed log data to a MySQL database to facilitate further analysis and reporting.

#Usage:

Upload the main.py file to an AWS Lambda function.
Subscribe the Lambda function to relevant CloudWatch Logs Groups to receive log batches.
Ensure the necessary IAM permissions and configurations are set up to access AWS services.
Update the destination database URL and other configurations as required.

#Dependencies:

The code relies on the following Python libraries:

gzip: For decompressing the log data received in gzip format.
json: For parsing and handling JSON data.
base64: For decoding base64-encoded log data.
re: For pattern matching and text processing.
boto3: For interaction with AWS services (e.g., EC2, Secrets Manager).
botocore: For handling AWS configurations and settings.
pymysql: For connecting and interacting with the MySQL database.
Please note that the sql_metadata.py module provides additional functions for parsing SQL queries and extracting table names and columns.

Feel free to utilize and customize this Lambda code to efficiently handle your log streams and derive valuable insights from your log data.