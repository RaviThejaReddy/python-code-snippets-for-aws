import boto3
import json


def lambda_handler(event, context):
    # ***************************************** #
    # give the table name and bucket name here .#
    # and bucket should be created in region #
    # ***************************************** #

    table_name = 'your table name'
    bucket_name = 'your bucket name'
    region = 'your resources region
    # ***************************************** #
    # give the table name and bucket name here .#
    # and bucket should be created in region #
    # ***************************************** #
    s3 = boto3.client('s3', region)
    dynamodb = boto3.resource('dynamodb', region)
    table = dynamodb.Table(table_name)
    key_name = 'dynamodb_backup/' + table_name.replace('.', '_') + '.json'
    backup_dynamodb_json = []
    scan = table.scan()
    backup_dynamodb_json.append(scan['Items'])
    loop_count = 0
    while True:
        sub_counter = 0
        with table.batch_writer() as batch:
            for each in scan['Items']:
                backup_dynamodb_json.append(each)
                sub_counter += 1
                batch.delete_item(
                    Key={
                        'id': each['id'],
                        'sortId': each['sortId']
                    }
                )
        loop_count += 1
        print('sub_counter value after batch deleting', sub_counter)
        print('loop_count value after batch deleting', loop_count)
        scan = table.scan()
        counter = len(scan['Items'])
        print('deleted this many items', counter)
        if counter == 0:
            break
    s3.put_object(
        Body=str(json.dumps(backup_dynamodb_json)),
        Bucket=bucket_name,
        Key=key_name
    )
    return {
        's3 url path': f'https://{bucket_name}.s3-{region}.amazonaws.com/{key_name}',
        'Descritption': f'Find backup in this bucket {bucket_name} and find this file to get your data  *** {key_name} ***'
    }
