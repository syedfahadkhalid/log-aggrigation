from datetime import datetime
import gzip
import json
import base64
import sql_metadata
import re
import boto3
import botocore
import time
import pymysql


client_config = botocore.config.Config(max_pool_connections=200)
ec2_client = boto3.client('ec2', config=client_config)


def unzip_stream(event):
    input_in_base64 = base64.b64decode(event['awslogs']['data'])  
    #input_in_base64 = base64.b64decode(event)
    file_content = gzip.decompress(input_in_base64)
    return file_content

def transform(logs_data):
    logs = json.loads(logs_data)['logEvents']
    undesired_strings = ['DROP TABLE `','SELECT @@collation_database;','SELECT CONNECTION_ID()','set @@sql_select_limit=?','SELECT @@session.transaction_isolation','commit', 'set session transaction read only', 'SELECT @@session.transaction_read_only', 'SELECT ?','set session transaction read write','SET ']
    actions = [
      {
        "service_name": "scripts-read-db",#record['message'].split('|')[0],
        "user_name": record['message'].split('|')[3].split(':')[0][:-1],
        "host": record['message'].split('|')[3].split(':')[3],
        "sql_text": clean_sql(extract_sql_statement(record['message'].split('|')[5])),
        "reply_time": float(record['message'].split('|')[4]),
        "table_name": sql_metadata.get_query_tables(record['message'].split('|')[5]),
        "columns": sql_metadata.get_query_columns(record['message'].split('|')[5]),
        "date": datetime.strptime(record['message'].split('|')[2], '%Y-%m-%d %H:%M:%S'),
        "query_count": 1,
        "team_name":"",
        "environment_name":""
      }
      for record in logs
    ]
    actions = [action for action in actions if not any(string in action['sql_text'] for string in undesired_strings)]

    # Extract host values into a separate list
    hosts = list(set([action['host'] for action in actions]))
    

    ec2_stats = []
    ip_addresses = list(divide_ip_addresses_in_chunks(hosts, 200))
    for addresses in ip_addresses:
        ec2_response = extract_ec2_against_host(addresses)
        res = fetch_ec2_metadata_against(ec2_response)
        if len(res)>0:
            ec2_stats.append(res)
        if len(ip_addresses)>1:
            time.sleep(5)

    
    if len(ec2_stats)>0:
        for action in actions:
            ip = action['host']
            matching_result = next((res for res in ec2_stats[0] if res['ip'] == ip), None)
            if matching_result:
                action['team_name'] = matching_result['team_name']
                action['environment_name'] = matching_result['environment_name']


    return actions


def divide_ip_addresses_in_chunks(hosts, chunk_size):
    divided_chunks = []
    for i in range(0, len(hosts), chunk_size):
        chunk = hosts[i:i+chunk_size]
        divided_chunks.append(chunk)
    return divided_chunks


def clean_sql(sql_statement):
    # Match data values (strings, numbers, etc.) between single quotes or double quotes
    pattern = r"'[^']*'|\\[^\\']*'|\d+"
    # Replace data values with question marks
    normalized = re.sub(pattern, '?', sql_statement)
    return normalized

def extract_sql_statement(sql_text):
    pattern = r"(?ms)(--|#).*?$|/\*.*?\*/"
    sql_statement = re.sub(pattern, "", sql_text)
    sql_statement = sql_statement.strip()
    return sql_statement

def extract_ec2_against_host(host):
    host = list(filter(lambda x: x != "", host))
    try:
        response = ec2_client.describe_instances(
            Filters=[
                    {
                        'Name': 'private-ip-address',
                        'Values': host
                    },
                ],
            )
    except botocore.exceptions.ClientError as e:
        print(str(e))
        response = ''   
    return response
    
def EC2Metadata():
    result = {}
    result["ip"] = ""
    result["launch_time"] = ""
    result["environmen_name"] = ""
    result["team_name"] = ""
    return result
 
def fetch_ec2_metadata_against(ec2_response):
        ec2_instances_metadata = []
        for reservation in ec2_response.get('Reservations'):
            for instance in reservation.get('Instances'):
                ec2_metadata = EC2Metadata()
                ec2_metadata["ip"] = instance.get('PrivateIpAddress')
                ec2_metadata["launch_time"] = instance.get('LaunchTime').strftime('%Y-%m-%d %H:%M:%S')
                for tag in instance.get('Tags'):
                    if tag.get('Key') == 'Name':
                        ec2_metadata["environment_name"] = tag.get('Value')
                    if tag.get('Key') == 'c-team':
                        ec2_metadata["team_name"] = tag.get('Value')
            ec2_instances_metadata.append(ec2_metadata)
        return ec2_instances_metadata


def get_query_stats_tuple_list_against(query_stats):
    query_stats_tuple_list = []
    for stats in query_stats:
        query_stats_tuple_list.append(
            (stats["service_name"], stats["user_name"], stats["host"], stats["sql_text"], stats["table_name"], stats["date"],
             stats["team_name"], stats["environment_name"], stats["query_count"]))
    return query_stats_tuple_list

def get_db_connection(database_url):
    db_creds = json.loads(get_secrets_from_secret_manager('<secret_id>')['SecretString'])
    username = db_creds['database_username']
    password = db_creds['database_password']

    conn = pymysql.connect(host=database_url, user=username, password=password, connect_timeout=5)
    return conn

def dump_query_stats_to(destination_db_url, query_stats):
    dump_sql = """
        INSERT INTO statistics.maxscale_logs_script_read (`service_name`,`user_name`,`host`,`sql_text`,`table_name`,`date`,`team_name`,`environment_name`,`query_count`)
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE query_count = query_count + 1 """
    conn =  get_db_connection(destination_db_url)
    with conn.cursor() as cursor:
        cursor.executemany(dump_sql, query_stats)
    conn.commit()
    conn.close()

def get_secrets_from_secret_manager(secret_id):
    secret_manager = boto3.client('secretsmanager')
    secrets = secret_manager.get_secret_value(
        SecretId=secret_id
    )
    return secrets

def lambda_handler(event, context):
    try:
        data = unzip_stream(event)
        response = transform(data)
        query_stats_tuple_list = get_query_stats_tuple_list_against(response)
        dump_query_stats_to("<db-endpoint>", query_stats_tuple_list)

        print("Data Pushed")
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
    except Exception as e:
        print(f"Exception: {str(e)}")

