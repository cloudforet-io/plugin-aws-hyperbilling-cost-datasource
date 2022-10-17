# plugin-aws-hyperbilling-cost-datasource
Plugin for collecting AWS HyperBilling data

---

## Secret Data
*Schema*
* spaceone_endpoint (str): SpaceONE Identity Endpoint 
* spaceone_api_key (str): Credentials for SpaceONE authentication
* spaceone_domain_id (str): SpaceONE Billing Domain ID
* aws_access_key_id (str): AWS Access Key to access HyperBilling data
* aws_secret_access_key (str): AWS Secret Key to access HyperBilling data
* aws_region (str): AWS Region to access HyperBilling data
* aws_s3_bucket (str): S3 Bucket with HyperBilling data

*Example*
<pre>
<code>
{
    "spaceone_endpoint": "grpc://identity.spaceone.svc.cluster.local:50051",
    "spaceone_api_key": "*****",
    "spaceone_domain_id": "domain-12345678",
    "aws_access_key_id": "*****",
    "aws_secret_access_key": "*****",
    "aws_region": "ap-northeast-2",
    "aws_s3_bucket": "*****"
}
</code>
</pre>

## Options
Currently, not required.
