#!/bin/env python2
# coding: utf-8

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config


class BotoClient(object):

    def __init__(self, host, access_key, secret_key):

        session = boto3.session.Session()
        self.cli = session.client(
            's3',
            use_ssl=False,
            endpoint_url="http://%s:%s" % (host, 80),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(s3={'addressing_style': 'path'}),
        )

    def boto_put(self, fpath, bucket_name, key_name, extra_args):

        MB = 1024**2
        GB = 1024**3

        config = TransferConfig(multipart_threshold=1 * GB,
                                multipart_chunksize=512 * MB, )

        self.cli.upload_file(
            Filename=fpath,
            Bucket=bucket_name,
            Key=key_name,
            Config=config,
            ExtraArgs=extra_args,
        )

    def boto_head(self, bucket_name, key_name):

        resp = self.cli.head_object(
            Bucket=bucket_name,
            Key=key_name,
        )

        return resp

    def boto_copy(self, bucket_name, source_key, key_name):

        resp = self.cli.copy_object(
            Bucket=bucket_name,
            CopySource={
                'Bucket': bucket_name,
                'Key': source_key},
            Key=key_name,
        )

        return resp

    def boto_get(self, fpath, bucket_name, key_name):

        obj = self.cli.get_object(Bucket=bucket_name, Key=key_name)

        # obj = {
        #         'Body': <botocore.response.StreamingBody object at 0x1bc7d10>,
        #         'ContentType': 'application/octet-stream',
        #         'ResponseMetadata': {
        #                 'HTTPStatusCode': 200, 'RetryAttempts': 0, 'HostId': '',
        #                 'RequestId': '00079534-1610-1919-2642-00163e03bb03',
        #                 'HTTPHeaders': {
        #                         'access-control-allow-headers': 'Origin, Content-Type, Accept, Content-Length',
        #                         'access-control-allow-methods': 'GET, PUT, POST, DELETE, OPTIONS, HEAD',
        #                         'access-control-allow-origin': '*',
        #                         'access-control-max-age': '31536000',
        #                         'cache-control': 'max-age=31536000',
        #                         'connection': 'keep-alive',
        #                         'content-length': '3508888',
        #                         'content-type': 'application/octet-stream',
        #                         'date': 'Wed, 19 Oct 2016 11:26:42 GMT',
        #                         'etag': '"12619d55847bb120b903b7b7998be1fb"',
        #                         'last-modified': 'Wed, 19 Oct 2016 11:14:11 GMT',
        #                         'server': 'openresty/1.9.7.4',
        #                         'x-amz-meta-md5': '12619d55847bb120b903b7b7998be1fb',
        #                         'x-amz-meta-s2-crc32': 'c96376ab',
        #                         'x-amz-meta-s2-size': '3508888'
        #                         'x-amz-meta-sha1': 'aba30b9b9da5ea743d52c85db7ff82f7c7dc41eb',
        #                         'x-amz-request-id': '00079534-1610-1919-2642-00163e03bb03',
        #                         'x-amz-s2-requester': 'drdrxp',
        #                 }
        #         },
        #         'LastModified': datetime.datetime(2016, 10, 19, 11, 14, 11, tzinfo=tzutc()),
        #         'ContentLength': 3508888,
        #         'ETag': '"12619d55847bb120b903b7b7998be1fb"',
        #         'CacheControl': 'max-age=31536000',
        #         'Metadata': {
        #                 's2-size': '3508888',
        #                 's2-crc32': 'c96376ab',
        #                 'sha1': 'aba30b9b9da5ea743d52c85db7ff82f7c7dc41eb',
        #                 'md5': '12619d55847bb120b903b7b7998be1fb'
        #         }
        # }

        with open(fpath, 'wb') as f:
            while True:
                buf = obj['Body'].read(1024 * 1024 * 8)
                if buf == '':
                    break

                f.write(buf)

        return obj

    def boto_list(self, bucket_name):

        resp = self.cli.list_objects(Bucket=bucket_name)

        if 'Contents' not in resp:
            return []
        else:
            return resp['Contents']
