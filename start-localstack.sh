#!/bin/sh

CURRENT_DIR="$(pwd)"
LOCALSTACK_STARTED='false'
S3_BUCKET_EXISTS='false'

LOCALSTACK_ENDPOINT="http://localhost:4566"
S3_BUCKET_NAME="report-store"

if [[ ! -d "$CURRENT_DIR/.localstack" ]]; then
    mkdir -p "$CURRENT_DIR/.localstack"
    echo "$CURRENT_DIR/.localstack is now created!"
else
    echo "$CURRENT_DIR/.localstack already exists!"
fi

# Check if localstack has already started
results=`docker container ls`

for line in $results
do
    for item in $line
    do
        if [[ "$item" == 'localstack_compliance_tech_recruitment_assignment' ]]; then
            LOCALSTACK_STARTED='true'
        fi
    done
done

if [[ $LOCALSTACK_STARTED == 'false' ]]; then
    echo 'Starting up localstack....'
    docker-compose up -d
    sleep 30
    echo 'localstack started!'
else
    echo 'localstack already started!'
fi

# Test if bucket already exists
results=`aws --endpoint-url=$LOCALSTACK_ENDPOINT s3 ls`

for line in $results
do
    for item in $line
    do
        if [[ "$item" == $S3_BUCKET_NAME ]]; then
            S3_BUCKET_EXISTS='true'
        fi
    done
done

if [[ $S3_BUCKET_EXISTS == 'false' ]]; then
    echo "Creating S3 bucket $S3_BUCKET_NAME"
    # create a bucket 
    aws --endpoint-url=$LOCALSTACK_ENDPOINT s3 mb s3://$S3_BUCKET_NAME
    sleep 10
    aws --endpoint-url=$LOCALSTACK_ENDPOINT s3api put-bucket-acl --bucket $S3_BUCKET_NAME --acl public-read
    echo "S3 bucket $S3_BUCKET_NAME created"
else
    echo "S3 bucket $S3_BUCKET_NAME already exists"
fi

aws --endpoint-url=$LOCALSTACK_ENDPOINT s3api put-object --bucket "report-store" --key  "rud/monthly/2024/11/202411-123456789-RUD.json" --body ./data/mock-rud.json

runtime=nodejs20.x
region=eu-west-1


# Create sender lambda
aws --endpoint-url=$LOCALSTACK_ENDPOINT lambda create-function \
    --function-name sender \
    --runtime $runtime \
    --role arn:aws:iam::000000000000:role/unsafe \
    --handler index.handler \
    --zip-file fileb://build/sender_function.zip \
    --environment "Variables={BUCKET_NAME=$S3_BUCKET_NAME}"

# Wait for sender function to be provisioned
aws --endpoint-url=$LOCALSTACK_ENDPOINT lambda wait function-active --function-name sender

# Create splitter lambda
aws --endpoint-url=$LOCALSTACK_ENDPOINT lambda create-function \
    --function-name splitter \
    --runtime $runtime \
    --role arn:aws:iam::000000000000:role/unsafe \
    --handler index.handler \
    --zip-file fileb://build/splitter_function.zip

# Wait for splitter function to be provisioned
aws --endpoint-url=$LOCALSTACK_ENDPOINT lambda wait function-active --function-name splitter

# Execute splitter Lambda function
aws --endpoint-url=$LOCALSTACK_ENDPOINT lambda invoke \
    --function-name splitter \
    --payload '{ }' \
    output.txt

# Create Step Function
aws --endpoint-url=$LOCALSTACK_ENDPOINT stepfunctions create-state-machine \
    --name "SplitAndSendReportByChunks" \
    --definition '{
        "Comment": "Split and send report by chunks",
        "StartAt": "SplitLargeReport",
            "States": {
            "SplitLargeReport": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:eu-west-1:000000000000:function:splitter",
                "Next": "SendReportByChunks"
            },
            "SendReportByChunks": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:eu-west-1:000000000000:function:sender",
                "End": true
            }
        }
    }' \
    --role-arn "arn:aws:iam::000000000000:role/stepfunctions-role"
