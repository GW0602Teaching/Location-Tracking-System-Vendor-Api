variable "app_name" {
  type        = string
  description = "Application Name"
  default = "Vendor-Api"
}

variable "websocket_table_name" {
  type        = string
  description = "Name of the web socket connection table in dynamo db"
  default     = "websocket-connections"
}

variable "sqs_queue_name" {
  type        = string
  description = "Queue name"
  default     = "vendor-twitter-queue"
}

variable "api_gateway_stage_name" {
    type        = string
    default     = "primary"
}

variable "vendor_table_name" {
  description = "Table name for dynamodb vendors"
  default = "vendors"
}

variable "image_tag" {}

variable "aws_region" {}