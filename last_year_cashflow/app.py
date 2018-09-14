import json
import os
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

es_host = os.getenv('ELASTICSEARCH_URL')

def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        {
            "resource": "Resource path",
            "path": "Path parameter",
            "httpMethod": "Incoming request's method name"
            "headers": {Incoming request headers}
            "queryStringParameters": {query string parameters }
            "pathParameters":  {path parameters}
            "stageVariables": {Applicable stage variables}
            "requestContext": {Request context, including authorizer-returned key-value pairs}
            "body": "A JSON string of the request payload."
            "isBase64Encoded": "A boolean flag to indicate if the applicable request payload is Base64-encode"
        }

        https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

    Attributes
    ----------

    context.aws_request_id: str
         Lambda request ID
    context.client_context: object
         Additional context when invoked through AWS Mobile SDK
    context.function_name: str
         Lambda function name
    context.function_version: str
         Function version identifier
    context.get_remaining_time_in_millis: function
         Time in milliseconds before function times out
    context.identity:
         Cognito identity provider context when invoked through AWS Mobile SDK
    context.invoked_function_arn: str
         Function ARN
    context.log_group_name: str
         Cloudwatch Log group name
    context.log_stream_name: str
         Cloudwatch Log stream name
    context.memory_limit_in_mb: int
        Function memory

        https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict
        'statusCode' and 'body' are required

        {
            "isBase64Encoded": true | false,
            "statusCode": httpStatusCode,
            "headers": {"headerName": "headerValue", ...},
            "body": "..."
        }

        # api-gateway-simple-proxy-for-lambda-output-format
        https: // docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    es = Elasticsearch([es_host])
    query = {
        "size": 1000,
        "query": {
            "bool": {
            "must": [
                {
                "match_all": {}
                },
                {
                "range": {
                    "EVENT_DT": {
                    "gte": "now-12M",
                    "lte": "now",
                    "format": "epoch_millis"
                    }
                }
                },
                {
                "match_phrase": {
                    "MERCHANT_ID.keyword": {
                    "query": "21623137"
                    }
                }
                }
            ],
            "filter": [],
            "should": [],
            "must_not": []
            }
        }
    }
    data = es.search(index="debitcard", doc_type="DebitCardType", body=query)
    meses = {}
    for doc in data['hits']['hits']:
        mes = doc['_source']['EVENT_DT'][:7]
        if  mes not in meses.keys():
            meses[mes] = 1
        else:
            meses[mes] = meses[mes] + 1
    print(meses)
    res = {
        "data": [
            {
                "date": "2017-09-01",
                "amount": 122453.12
            },
            {
                "date": "2017-10-01",
                "amount": 123449.22
            },
            {
                "date": "2017-11-01",
                "amount": 125001.43
            },
            {
                "date": "2017-12-01",
                "amount": 129234.93
            },
            {
                "date": "2018-01-01",
                "amount": 115023.98
            },
            {
                "date": "2018-02-01",
                "amount": 97586.43
            },
            {
                "date": "2018-03-01",
                "amount": 95473.21
            },
            {
                "date": "2018-04-01",
                "amount": 93212.31
            },
            {
                "date": "2018-05-01",
                "amount": 84302.13
            },
            {
                "date": "2018-06-01",
                "amount": 97232.21
            },
            {
                "date": "2018-07-01",
                "amount": 82012.13
            },
            {
                "date": "2018-08-01",
                "amount": 79543.10
            }
        ]
    }

    return {
        "statusCode": 200,
        "body": json.dumps(res),
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        }
    }
