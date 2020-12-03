# Reference
A method to migrate S3 files from one account to another with two IAM profiles.

# Notes
- The sender uses environment-based credentials, e.g. IAM instance profile or ~/.aws/credentials.
- The receiver is an assumed role the environment-based credentials can assume.

# Deployment
This script works particularly well in parallel for large amounts of files spread across large amounts of S3 prefixes where files are stored under a range of s3 directories, e.g.:
```
!/bin/bash
for dayofmonth in {01..31}
do
 python3 s3-to-s3-assumerole.py my_s3_prefix/$dayofmonth/ &
done
``` 
