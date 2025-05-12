import boto3
import logging
import requests

class ProducerOnboarding:

    def __init__(self, properties):
        self.properties = properties
        self.s3_client = boto3.client('s3')

    def validate_metadata(self, data):
        s3_bucket = data.get("s3_bucket")

        validators = [
            self.check_replication_rule_exists,
            self.check_replication_rule_applied,
            self.validate_directories_structure,
            self.validate_application_subdirectories,
            self.validate_data_subdirectories,
            self.validate_bucket_tags,
            self.validate_event_notification,
            self.trigger_internal_api
        ]

        for validator in validators:
            result, message = validator(s3_bucket)
            if not result:
                return {"status": "Fail", "message": message}

        return {"status": "Success", "message": "All validations passed."}

    def check_replication_rule_exists(self, bucket):
        try:
            response = self.s3_client.get_bucket_replication(Bucket=bucket)
            if 'ReplicationConfiguration' in response:
                return True, "Replication rule exists."
            return False, "No replication rule found."
        except Exception as e:
            return False, str(e)

    def check_replication_rule_applied(self, bucket):
        try:
            response = self.s3_client.get_bucket_replication(Bucket=bucket)
            rules = response.get('ReplicationConfiguration', {}).get('Rules', [])
            if rules and any(rule.get('Status') == 'Enabled' for rule in rules):
                return True, "Replication rule applied."
            return False, "Replication rule not applied."
        except Exception as e:
            return False, str(e)

    def validate_directories_structure(self, bucket):
        try:
            objects = self.s3_client.list_objects_v2(Bucket=bucket).get('Contents', [])
            prefixes = set([obj['Key'].split('/')[0] for obj in objects])
            if 'application' in prefixes and 'data' in prefixes:
                return True, "application and data directories found."
            return False, "Missing 'application' or 'data' directory."
        except Exception as e:
            return False, str(e)

    def validate_application_subdirectories(self, bucket):
        try:
            expected = {'application/config-dataops/', 'application/jks/', 'application/runtime/'}
            keys = set(obj['Key'] for obj in self.s3_client.list_objects_v2(Bucket=bucket, Prefix='application/').get('Contents', []))
            if expected.issubset(keys):
                return True, "Application subdirectories valid."
            return False, "Missing application subdirectories."
        except Exception as e:
            return False, str(e)

    def validate_data_subdirectories(self, bucket):
        try:
            expected = {'data/error/', 'data/inbound/'}
            keys = set(obj['Key'] for obj in self.s3_client.list_objects_v2(Bucket=bucket, Prefix='data/').get('Contents', []))
            if expected.issubset(keys):
                return True, "Data subdirectories valid."
            return False, "Missing data subdirectories."
        except Exception as e:
            return False, str(e)

    def validate_bucket_tags(self, bucket):
        try:
            response = self.s3_client.get_bucket_tagging(Bucket=bucket)
            tags = {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
            if tags.get('tag1') == 'val1':
                return True, "Required tags are present."
            return False, "Required tag not found."
        except Exception as e:
            return False, str(e)

    def validate_event_notification(self, bucket):
        try:
            response = self.s3_client.get_bucket_notification_configuration(Bucket=bucket)
            lambda_configs = response.get('LambdaFunctionConfigurations', [])
            for config in lambda_configs:
                if 'data/error' in str(config) and 's3 file monitoring' in config.get('Id', ''):
                    return True, "Event notification configured correctly."
            return False, "Event notification not found."
        except Exception as e:
            return False, str(e)

    def trigger_internal_api(self, bucket):
        try:
            url = f"http://localhost:5000/trigger?bucket={bucket}"
            response = requests.get(url)
            if response.status_code == 200:
                return True, "Internal API triggered successfully."
            return False, f"API call failed: {response.text}"
        except Exception as e:
            return False, str(e)
