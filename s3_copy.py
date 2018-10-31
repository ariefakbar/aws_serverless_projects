import boto3, botocore, boto3, json, os


class s3_move():
    def __init__(self):
        self.local_dir = "/tmp/"
        self.bucket = "source_bucket"
        self.bucket_folder = "source_folder/"
        self.move_bucket = "target_bucket"
        self.move_folder ="target_folder/"
     

        return

    ### FILES FUNCTIONS ###
    # -- download from bucket --
    def download_from_bucket(self, bucket, bucket_folder, file_name, new_file_name):
        s3 = boto3.client('s3')

        s3.download_file(bucket, bucket_folder + file_name, self.local_dir + new_file_name)

        return

    # -- upload file to bucket --
    def upload_to_bucket(self, origin_file, bucket, bucket_folder, dest_file):
        s3 = boto3.client('s3')

        s3.upload_file(self.local_dir + origin_file, bucket, bucket_folder + dest_file)

        return

    # -- list files on bucket folder --
    def list_files_bucket(self, bucket, bucket_folder):
        files_list = []

        s3 = boto3.client('s3')

        bucket_content = s3.list_objects(
            Bucket=bucket,
            Prefix=bucket_folder[0:-1]
        )

        for i in bucket_content['Contents']:
            # print(i)
            if i['Key'] != bucket_folder:
                tmp = i['Key'].split("/")
                files_list.append(tmp[-1])

        return bucket_content, files_list



  


sm = s3_move()


def lambda_handler(event, context):
    bucket_content, files_list = sm.list_files_bucket(sm.bucket, sm.bucket_folder)
    
    for i in files_list:
        sm.download_from_bucket(sm.bucket, sm.bucket_folder, i, i)
    
    files_in_tmp = os.listdir("/tmp/")
    
    for i in files_in_tmp:
        sm.upload_to_bucket(i, sm.move_bucket, sm.move_folder, i )
        
        