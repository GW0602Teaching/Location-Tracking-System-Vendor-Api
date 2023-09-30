# role => a task execution role
data "aws_iam_policy_document" "assume_role_policy" {
    statement {
      actions = ["sts:AssumeRole"]
      effect="Allow"
      principals {
        type = "Service"
        identifiers = ["lambda.amazonaws.com"]
      }
    }
}

resource "aws_iam_role" "lambda_main" {
    name = "${var.app_name}-lambda"
    assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
}

resource "aws_iam_role_policy_attachment" "attach_exec_role" {
    role = aws_iam_role.lambda_main.name
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_ws" {
    statement {
      effect = "Allow"
      actions = [
        "execute-api:ManageConnections",
        "dynamodb:PutItem",
        "dynamodb:DeleteItem",
        "dynamodb:GetItem",
        "dynamodb:Scan",
        "dynamodb:DescribeTable",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ]
      resources = [
        "arn:aws:sqs:${var.aws_region}:${local.account_id}:${var.sqs_queue_name}",
        "arn:aws:dynamodb:${var.aws_region}:${local.account_id}:table/${var.websocket_table_name}",
        "arn:aws:dynamodb:${var.aws_region}:${local.account_id}:table/${var.vendor_table_name}",
        # api gateway, will add it after we create out api gateway through terraform
      ]
    }
}

resource "aws_iam_policy" "lambda_ws" {
    name = "${var.app_name}-lambda-ws"
    policy = data.aws_iam_policy_document.lambda_ws.json
}

resource "aws_iam_role_policy_attachment" "lambda_ws" {
    policy_arn = aws_iam_policy.lambda_ws.arn
    role = aws_iam_role.lambda_main.name   
}