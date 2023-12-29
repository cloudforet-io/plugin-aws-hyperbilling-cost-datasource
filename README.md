# plugin-aws-hyperbilling-cost-datasource
Plugin for collecting AWS HyperBilling data

---

## Secret Data
*Schema*
* spaceone_endpoint (str): SpaceONE Identity Endpoint (grpc or http)
* spaceone_client_secret (str): Credentials for SpaceONE authentication
* aws_access_key_id (str): AWS Access Key to access HyperBilling data
* aws_secret_access_key (str): AWS Secret Key to access HyperBilling data
* aws_region (str): AWS Region to access HyperBilling data
* aws_s3_bucket (str): S3 Bucket with HyperBilling data

*Example*
<pre>
<code>
{
    "spaceone_endpoint": "grpc://identity.spaceone.svc.cluster.local:50051 or http://example.spaceone.com/identity:8000",
    "spaceone_client_secret": "*****",
    "aws_access_key_id": "*****",
    "aws_secret_access_key": "*****",
    "aws_region": "ap-northeast-2",
    "aws_s3_bucket": "*****"
}
</code>
</pre>

## Options
Currently, not required.
