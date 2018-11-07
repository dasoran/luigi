import datetime
import logging

from luigi import six

import luigi

logger = logging.getLogger('luigi-interface')

try:
    from ConfigParser import NoSectionError
except ImportError:
    from configparser import NoSectionError

# TODO: consider to import boto3 on global space or not
try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    logger.warning(
        "Loading DynamoDB module without boto3 installed. Will crash at runtime if DynamoDB functionality is used.")


class DynamoDBTarget(luigi.Target):
    """
    Target for a resource in DynamoDB.
    """
    marker_table = luigi.configuration.get_config().get('dynamodb', 'marker-table', 'table_updates')
    _dynamodb = None

    def __init__(self, table, update_id, dynamodb=None, aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
        """
        :param table: DynamoDB table
        :type table: str
        :param update_id: an identifier for this data hash
        :type update_id: str
        :param dynamodb: boto3-resource of dynamodb
        :type dynamodb: boto3.resources.base.ServiceResource
        :param aws_access_key_id: AWS access key id
        :type aws_access_key_id: str
        :param aws_secret_access_key: AWS secret access key
        :type aws_secret_access_key: str
        :param kwargs: keyword arguments
        """
        self.table = table
        self.update_id = update_id

        options = self._get_dynamodb_config()
        options.update(kwargs)

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        if aws_access_key_id:
            options['aws_access_key_id'] = aws_access_key_id
        if aws_secret_access_key:
            options['aws_secret_access_key'] = aws_secret_access_key

        self._options = options

        self.dynamodb = dynamodb

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.
        """
        dynamodb = self.dynamodb
        try:
            dynamodb.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': 'update_id',
                        'AttributeType': 'S'
                    },
                ],
                TableName=self.marker_table,
                KeySchema=[
                    {
                        'AttributeName': 'update_id',
                        'KeyType': 'HASH'
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 1,
                    'WriteCapacityUnits': 1
                })
        # If table already exist, it proceeds through exception.
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                pass
            else:
                raise

    def touch(self):
        """
        Mark this update as complete.
        """
        self.create_marker_table()

        dynamodb = self.dynamodb
        dynamodb.put_item(
            TableName=self.marker_table,
            Item={
                'update_id': {
                    'S': self.update_id
                },
                'target_table': {
                    'S': self.table
                },
                'created_at': {
                    'S': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            })

    def exists(self):
        """
        Test, if this task has been run.
        """
        dynamodb = self.dynamodb
        try:
            row = dynamodb.query(
                TableName=self.marker_table,
                ExpressionAttributeNames={'#name0': 'hash_key'},
                ExpressionAttributeValues={':value0': {
                    'S': self.update_id
                }},
                KeyConditionExpression='#name0 = :value0')
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                row = None
            else:
                raise
        return row is not None and row['Count'] > 0

    @property
    def dynamodb(self):
        """
        Get a AWS resource object.
        """
        options = dict(self._options)

        if self._dynamodb:
            return self._dynamodb

        if 'marker_table' in options:
            options.pop('marker-table')

        aws_access_key_id = options.get('aws_access_key_id')
        aws_secret_access_key = options.get('aws_secret_access_key')

        # Removing key args would break backwards compability
        role_arn = options.get('aws_role_arn')
        role_session_name = options.get('aws_role_session_name')

        aws_session_token = None

        if role_arn and role_session_name:
            sts_client = boto3.client('sts')
            assumed_role = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=role_session_name)
            aws_secret_access_key = assumed_role['Credentials'].get('SecretAccessKey')
            aws_access_key_id = assumed_role['Credentials'].get('AccessKeyId')
            aws_session_token = assumed_role['Credentials'].get('SessionToken')
            logger.debug(
                'using aws credentials via assumed role {} as defined in luigi config'.format(role_session_name))

        for key in ['aws_access_key_id', 'aws_secret_access_key', 'aws_role_session_name', 'aws_role_arn']:
            if key in options:
                options.pop(key)

        if not (aws_access_key_id and aws_secret_access_key):
            logger.debug('no credentials provided, delegating credentials resolution to boto3')

        self._dynamodb = boto3.client(
            'dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token, **options)

        return self._dynamodb

    @dynamodb.setter
    def dynamodb(self, value):
        self._dynamodb = value

    def open(self, mode):
        raise NotImplementedError("Cannot open() DynamoDBTarget")

    @staticmethod
    def _get_dynamodb_config(key=None):
        defaults = dict(luigi.configuration.get_config().defaults())
        try:
            config = dict(luigi.configuration.get_config().items('dynamodb'))
        except (NoSectionError, KeyError):
            return {}
        # So what ports etc can be read without us having to specify all dtypes
        for k, v in six.iteritems(config):
            try:
                config[k] = int(v)
            except ValueError:
                pass
        if key:
            return config.get(key)
        section_only = {k: v for k, v in config.items() if k not in defaults or v != defaults[k]}

        return section_only

    @staticmethod
    def _get_dynamo_value_dict(value):
        if isinstance(value, str):
            return {'S': value}
        elif isinstance(value, int):
            return {'N': str(value)}
        elif isinstance(value, bool):
            return {"BOOL": value}
        elif value is None:
            return {'NULL': True}
        else:
            raise NotImplementedError('value: {0} is unknown type'.format(value))
