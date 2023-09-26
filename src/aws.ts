import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import AWS from 'aws-sdk';
import dotenv from 'dotenv';

dotenv.config();

AWS.config.update({ region: process.env.AWS_REGION });

const { DynamoDB, SQS } = AWS;

const dynamodb = new DynamoDB();
const sqs = new SQS();

// describe a table
export const dynamodbDescribeTable = async (tableName: string) => {
  try {
    const table = await dynamodb
      .describeTable({
        TableName: tableName,
      })
      .promise();
    console.log('Table retrieved', table);
    return table;
  } catch (e) {
    if (e instanceof Error) {
      throw e;
    }
    throw new Error(
      `dynamodbDescribeTable error object unknown type`
    );
  }
};

// scan a table
export const dynamodbScanTable = async function* (
  tableName: string,
  limit: number = 25,
  lastEvaluatedKey?: AWS.DynamoDB.Key
) {
  while (true) {
    const params: AWS.DynamoDB.ScanInput = {
      TableName: tableName,
      Limit: limit,
    };

    if (lastEvaluatedKey) {
      params.ExclusiveStartKey = lastEvaluatedKey;
    }

    try {
      const result = await dynamodb.scan(params).promise();
      if (!result.Count) {
        return;
      }

      lastEvaluatedKey = (result as AWS.DynamoDB.ScanOutput)
        .LastEvaluatedKey;
      result.Items = result.Items?.map((item) => unmarshall(item));
      yield result;
    } catch (e) {
      if (e instanceof Error) {
        throw e;
      }
      throw new Error('dynamodbScanTable unexpected error');
    }
  }
};

// scan all results
export const getAllScanResults = async <T>(
  tableName: string,
  limit: number = 25
) => {
  try {
    await dynamodbDescribeTable(tableName);

    const scanTableGen = await dynamodbScanTable(tableName, limit);

    const results: T[] = [];
    let isDone = false;

    while (!isDone) {
      const iterator = await scanTableGen.next();

      if (!iterator) {
        throw new Error('No iterator returned');
      }

      if (iterator.done || !iterator.value.LastEvaluatedKey) {
        isDone = true;
      }

      if (iterator.value) {
        iterator.value.Items!.forEach((result: any) =>
          results.push(result)
        );
      }
    }

    return results;
  } catch (e) {
    if (e instanceof Error) {
      throw e;
    }

    throw new Error(`getAllScanResults unexpected error`);
  }
};

// Add a connection
export const dynamoDbAddConnection = async (
  tableName: string,
  connectionId: string
) => {
  try {
    const params: AWS.DynamoDB.PutItemInput = {
      TableName: tableName,
      Item: marshall({ connectionId }),
    };

    const res = await dynamodb.putItem(params).promise();

    return res;
  } catch (e) {
    if (e instanceof Error) {
      return e;
    }
    return new Error(
      'dynamoDbAddConnection error object unknown type'
    );
  }
};

// const execAddConnection = async () => {
//   const res = await dynamoDbAddConnection(
//     'websocket-connections',
//     '123'
//   );
//   console.log(res);
// };

// execAddConnection();

// Remove a connection
export const dynamoDbRemoveConnection = async (
  tableName: string,
  connectionId: string
) => {
  try {
    const params: AWS.DynamoDB.DeleteItemInput = {
      TableName: tableName,
      Key: {
        connectionId: marshall(connectionId),
      },
    };

    const res = await dynamodb.deleteItem(params).promise();

    return res;
  } catch (e) {
    if (e instanceof Error) {
      return e;
    }
    return new Error(
      'dynamoDbRemoveConnection error object unkown type'
    );
  }
};

// const execRemoveConnection = async () => {
//   const res = await dynamoDbRemoveConnection(
//     'websocket-connections',
//     '123'
//   );
//   console.log(res);
// };

// execRemoveConnection();

export const sqsDeleteMessage = async (
  queueUrl: string,
  receiptHandle: string
) => {
  try {
    const params: AWS.SQS.DeleteMessageRequest = {
      ReceiptHandle: receiptHandle,
      QueueUrl: queueUrl,
    };

    const res = await sqs.deleteMessage(params).promise();
    console.log('Message deleted!');
    return res;
  } catch (e) {
    if (e instanceof Error) {
      return e;
    }

    return new Error(`sqsDeleteMessage error object unknown type`);
  }
};

const execSqsDeleteMessage = async () => {
  const receiptHandle =
    'AQEBo8WLWoDJdh0kFyg6bzezPWH7ND1c6ND1BBbGwObaQXsbj9u2f989XZc4lvn3NAu3YylIaTBmdV3mDRMsc+nJ4LKg4Rq67/zsiwFbhTcnKlpaN47EUE7+Jk44F1tnlFvotgWRPkXiAHf6rMqP+g+fQK4GWyUktOkydkEj6etqJcQajDhsiUtMTTINF/XgYLbN8NjGMtFE9O+C3SLFqejadMlhOI/NnlPLES0VVDAQ+dQoCpbgnQIqMsef7GTBDZbJ8nF16uzDWrh67YQVRgltEB5+WWCLDZVgcFTqHr59a16gwzD8UV4sOEha7R7YDVNRt+IqxZ5f6z4CCDt8TsCP8D8i8oOctgcjHI++PyQ8Fsh6yiQ6KI8SJUtvuxSlGzXr6XVmCSO49yBzw8LCnUdZCQ==';
  const res = await sqsDeleteMessage(
    'https://sqs.us-east-1.amazonaws.com/525480118775/vender-twitter-queue',
    receiptHandle
  );
  console.log(res);
};

execSqsDeleteMessage();
