import json
import rx
from rx import operators as ops
import boto3
from botocore.exceptions import ClientError


def transfer_file(source_bucket,
                   destination_bucket, filename):
    result = {'Filename' : filename}
    try:
        s3_resource = boto3.resource('s3')
        source = {
            'Bucket':source_bucket,
            'Key':filename
        }
        s3_resource.meta.client.copy(source,destination_bucket,filename)
        result['Status'] = 'Success'
    except ClientError as e:
        print(e)
        result['Status'] = 'Error' + e.response['Error']['Code']


    return result

def transfer_between_buckets(source_bucket,destination_bucket):

        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=source_bucket)
        print(response)


        return rx.from_(response['Contents']).pipe(
            ops.map(lambda obj: obj['Key']),
            ops.map(lambda filename: transfer_file(source_bucket, destination_bucket, filename))
        )











def main():
    print("halo s3")
    transfer_between_buckets('muthuishere-bucket-a', 'muthuishere-bucket-b').subscribe(lambda res: print(res))




if __name__ == "__main__":
    main()