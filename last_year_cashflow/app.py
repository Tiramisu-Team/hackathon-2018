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
        mes = doc['_source']['EVENT_DT'][:7] + '-01'
        monto = float(str.replace(doc['_source']['EVENT_AMT'],",","."))
        if  mes not in meses.keys():
            meses[mes] = monto
        else:
            meses[mes] = meses[mes] + monto
    datos = []
    for mes in meses:
        datos.append({"date": mes, "amount": meses[mes]})
    res = {
        "data": json.dumps(datos)
    }

    return {
        "statusCode": 200,
        "body": json.dumps(res),
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        }
    }
