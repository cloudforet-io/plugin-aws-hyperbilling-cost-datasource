# plugin-aws-hyperbilling-cost-datasource
Plugin for collecting AWS HyperBilling data

---

## Secret Data
*Schema*
* client_id (str): HyperBilling login ID 
* secret (str): Credentials for authentication
* endpoint (str): AWS HyperBilling service endpoint 

*Example*
<pre>
<code>
{
    "client_id": "*****",
    "client_secret": "*****",
    "endpoint": "https://{url}
}
</code>
</pre>

## Options
Currently, not required.
