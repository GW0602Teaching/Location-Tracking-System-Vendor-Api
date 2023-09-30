# Connect
FROM amazon/aws-lambda-nodejs:18 AS connect

ARG FUNCTION_DIR="/var/task"

COPY package.json .

RUN npm install && npm install typescript -g

COPY . .

RUN tsc

RUN mkdir -p ${FUNCTION_DIR}

CMD ["build/connect.handler"]

# Disconnect
FROM amazon/aws-lambda-nodejs:18 AS disconnect

ARG FUNCTION_DIR="/var/task"

COPY package.json .

RUN npm install && npm install typescript -g

COPY . .

RUN tsc

RUN mkdir -p ${FUNCTION_DIR}

CMD ["build/disconnect.handler"]