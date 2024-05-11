import boto3
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def move_files(source_bucket, source_prefix, dest_bucket, dest_prefix, sns_topic_arn):
    """
  Moves files from a source S3 folder to a destination S3 folder based on a predefined prefix.

  Args:
    source_bucket: The name of the source S3 bucket.
    source_prefix: The prefix of the files to move in the source bucket.
    dest_bucket: The name of the destination S3 bucket.
    dest_prefix: The prefix to apply to the files in the destination bucket.
    sns_topic_arn: The ARN of the SNS topic to use for notifications.
  """
    
    s3 = boto3.client('s3', region_name='us-west-2')    # Replace the region with the region specified to your account.
    sns = boto3.client('sns', region_name='us-west-2')  # Replace the region with the region specified to your account.

    moved_files = []
    errors = []

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=source_bucket, Prefix=source_prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            source_key = obj['Key']
            filename = source_key.split('/')[-1]
            dest_key = f"{dest_prefix}{filename}"

            try:
                # Copy the object from source to destination
                copy_source = {'Bucket': source_bucket, 'Key': source_key}
                s3.copy(copy_source, dest_bucket, dest_key)
                logging.info(f"File copied: {source_key} to {dest_key}")

                # Delete the object from the source bucket
                s3.delete_object(Bucket=source_bucket, Key=source_key)
                logging.info(f"File deleted: {source_key}")
                moved_files.append(source_key)


            except Exception as e:
                errors.append(f"Error moving file {source_key}: {str(e)}")
                logging.error(f"Error moving file {source_key}: {str(e)}")

    message = ""
    if moved_files:
        message += f"Successfully moved the following files to {dest_bucket}/{dest_prefix}:\n"
        message += "\n".join(moved_files) + "\n"
    if errors:
        message += f"Errors encountered:\n"
        message += "\n".join(errors)

    if message:
        sns.publish(TopicArn=sns_topic_arn, Message=message, Subject="S3 File Move Notification")
        logging.info("SNS notification sent.")


if __name__ == "__main__":
    # Replace these values with your actual configuration
    source_bucket = "customer-details-aws"
    source_prefix = "customer-details/sr1_"
    dest_bucket = "sales-rep-1"
    dest_prefix = "sr1/"
    sns_topic_arn = " " # Coppy here the topic arn.
    move_files(source_bucket, source_prefix, dest_bucket, dest_prefix, sns_topic_arn)

    print("Script execution completed.")
