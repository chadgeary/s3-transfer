#!/usr/bin/env python3
# A method to migrate S3 files from one account to another with two IAM profiles.
# The sender uses environment-based credentials (e.g. IAM instance profile or ~/.aws/credentials).
# The receiver is an assumed role the environment-based credentials can assume.

# Files are operated on in series, parallelize via prefixes, e.g.:
###
#!/bin/bash
#for dayofmonth in {01..31}
#do
# python3 s3-to-s3-assumerole.py my_s3_prefix/$dayofmonth/ &
#done
###

import botocore
import boto3
import datetime
from dateutil.tz import tzlocal
import time
import os
import sys

# log, buckets, prefix, role
work_dir='/tmp/'
logpath=work_dir+'/s3-to-s3-assumerole.log'

sender_bucket='senderbucket1'
sender_prefix=sys.argv[1]

receiver_bucket='receiverbucket1'
receiver_role_arn='arn:aws:iam::0123456789:role/myrole1'

# local storage - make directory if not exists
if not os.path.exists(work_dir+sender_prefix):
  os.makedirs(work_dir+sender_prefix)

# receiver assume role with autorenewal
assume_role_cache: dict = {}
def assumed_role_session(role_arn: str, base_session: botocore.session.Session = None):
  base_session = base_session or boto3.session.Session()._session
  fetcher = botocore.credentials.AssumeRoleCredentialFetcher(
    client_creator = base_session.create_client,
    source_credentials = base_session.get_credentials(),
    role_arn = role_arn,
  )
  creds = botocore.credentials.DeferredRefreshableCredentials(
    method = 'assume-role',
    refresh_using = fetcher.fetch_credentials,
    time_fetcher = lambda: datetime.datetime.now(tzlocal())
  )
  botocore_session = botocore.session.Session()
  botocore_session._credentials = creds
  return boto3.Session(botocore_session = botocore_session)
receiver_session = assumed_role_session(receiver_role_arn)

# sender s3 connect and paginate/iterate over s3 objects in sender_bucket/sender_prefix/
sender_client = boto3.client('s3')
sender_paginator = sender_client.get_paginator('list_objects')
sender_iterator = sender_paginator.paginate(Bucket=sender_bucket,Prefix=sender_prefix,Delimiter='/', PaginationConfig={'PageSize': None})
sender_get = boto3.resource('s3')

# for each S3 object in sender_bucket/sender_prefix/, get from sender->local, put from local->sender, remove from local
for sender_response in sender_iterator:
  for key, response in sender_response.items():
    if key == 'Contents':
      for json_data in response:
        for key, prefix in json_data.items():
          if key == 'Key' and prefix != sender_prefix:
            # get from sender to local
            time_start = time.process_time()
            sender_get.meta.client.download_file(sender_bucket,prefix,work_dir+prefix)
            time_get = time.process_time() - time_start
            with open(logpath, "a") as logfile:
              logfile.write(prefix + " get completed in " + str(time_get) + " seconds\n")

            # put from local to receiver
            receiver_s3 = receiver_session.client('s3')
            receiver_s3.put_object(Bucket=receiver_bucket,Key=prefix,Body=work_dir+prefix)
            time_put = time.process_time() - time_get
            with open(logpath, "a") as logfile:
              logfile.write(prefix + " put completed in " + str(time_put) + " seconds\n")

            # remove local
            os.remove(work_dir+prefix)
            time_remove = time.process_time() - time_put
            with open(logpath, "a") as logfile:
              logfile.write(prefix + " local_remove completed in " + str(time_remove) + " seconds\n")
