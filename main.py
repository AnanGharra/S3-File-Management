import boto3


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
    s3 = boto3.client('s3')
    sns = boto3.client('sns')

    moved_files = []
    errors = []

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=source_bucket, Prefix=source_prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            source_key = obj['Key']
            dest_key = f"{dest_prefix}{source_key}"

            try:
                # Copy the object from source to destination
                copy_source = {'Bucket': source_bucket, 'Key': source_key}
                s3.copy(copy_source, dest_bucket, dest_key)

                # Delete the object from the source bucket
                s3.delete_object(Bucket=source_bucket, Key=source_key)
                moved_files.append(source_key)

            except Exception as e:
                errors.append(f"Error moving file {source_key}: {e}")

    message = ""
    if moved_files:
        message += f"Successfully moved the following files to {dest_bucket}/{dest_prefix}:\n"
        message += "\n".join(moved_files) + "\n"
    if errors:
        message += f"Errors encountered:\n"
        message += "\n".join(errors)

    if message:
        sns.publish(TopicArn=sns_topic_arn, Message=message, Subject="S3 File Move Notification")


if __name__ == "__main__":
    print("hi")
    # Replace these values with your actual configuration
    source_bucket = "customer-details-aws"
    source_prefix = "customer-details/sr1_"
    dest_bucket = "sales-rep-1"
    dest_prefix = "sr1/"
    sns_topic_arn = "arn:aws:sns:us-west-2:533267062975:AWS-moving-notification"

    move_files(source_bucket, source_prefix, dest_bucket, dest_prefix, sns_topic_arn)

    print("Script execution completed.")
