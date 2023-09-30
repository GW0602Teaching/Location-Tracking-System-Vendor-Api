import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { APIGatewayEvent, APIGatewayProxyResult } from 'aws-lambda';
import { AWSError, DynamoDB } from 'aws-sdk';
import { PromiseResult } from 'aws-sdk/lib/request';
import { dynamodbScanTable } from './aws';

export const handler = async (
  event: APIGatewayEvent
): Promise<APIGatewayProxyResult> => {
  const tableName = process.env.AWS_VENDOR_TABLE_NAME ?? 'vendors';

  const pageLimit = event.queryStringParameters?.limit;

  const lastEvaluatedKey = event.queryStringParameters
    ?.lastEvaluatedKey
    ? marshall(
        JSON.parse(event.queryStringParameters?.lastEvaluatedKey)
      )
    : undefined;

  let scanTableGen: AsyncGenerator<
    PromiseResult<DynamoDB.ScanOutput, AWSError>,
    void,
    unknown
  >;
  try {
    scanTableGen = await dynamodbScanTable(
      tableName,
      Number(pageLimit),
      lastEvaluatedKey
    );
  } catch (e) {
    return {
      statusCode: 500,
      headers: {
        'content-type': 'text/plain; charset=utf-8',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
      },
      body:
        e instanceof Error
          ? e.message
          : 'dynamoDbScanTable returned an unknown error',
    };
  }

  const iterator = await scanTableGen?.next();

  if (iterator.value) {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
      },
      body: JSON.stringify({
        Items: iterator.value.Items,
        count: iterator.value.Count,
        lastEvaluatedKey: iterator.value.LastEvaluatedKey
          ? unmarshall(iterator.value.LastEvaluatedKey)
          : null,
      }),
    };
  }

  return {
    statusCode: 200,
    headers: {
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
    },
    body: JSON.stringify({
      Items: [],
      count: 0,
      lastEvaluatedKey: null,
    }),
  };
};
