export AWS_REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="datalake-spotify-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)

# tomar el lambda_handler y añadirlo al zip para crear la lambda, yo lo comprimi directamente en zip porque no tengo el paquete para el comando
# zip firehose.zip ./firehose/firehose.py

aws lambda create-function \
    --function-name spotify-firehose-lambda \
    --runtime python3.12 \
    --role $ROLE_ARN \
    --handler firehose.lambda_handler \
    --zip-file fileb://firehose.zip \
    --timeout 60 \
    --memory-size 128

export LAMBDA_ARN=$(aws lambda get-function --function-name spotify-firehose-lambda --query 'Configuration.FunctionArn' --output text)

## para actualizar la lambda si ya existe:
#aws lambda update-function-code \
#    --function-name spotify-firehose-lambda \
#    --zip-file fileb://firehose.zip

# partitionKeyFromLambda:year -> primera parte de la particion
# partitionKeyFromLambda:month -> segunda parte de la particion
# buffer para entrar y otro para salir de la lambd
# el buffer de salida es el que esta en Processors
# no se pueden ejecutar mas de 7 lambdas en paralelo -> cuidado con cuantos datos de usan -> o usar delay
# aqui la clave de particion es para la tabla 
aws firehose create-delivery-stream \
    --delivery-stream-name spotify-delivery-stream \
    --delivery-stream-type KinesisStreamAsSource \
    --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:$AWS_REGION:$ACCOUNT_ID:stream/spotify-stream,RoleARN=$ROLE_ARN" \
    --extended-s3-destination-configuration '{
        "BucketARN": "arn:aws:s3:::'"$BUCKET_NAME"'",
        "RoleARN": "'"$ROLE_ARN"'",
        "Prefix": "raw/spotify_by_album_release_date/year=!{partitionKeyFromLambda:year}/month=!{partitionKeyFromLambda:month}/",
        "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
        "BufferingHints": {
            "SizeInMBs": 64,
            "IntervalInSeconds": 60
        },
        "DynamicPartitioningConfiguration": {
            "Enabled": true,
            "RetryOptions": {
                "DurationInSeconds": 300
            }
        },
        "ProcessingConfiguration": {
            "Enabled": true,
            "Processors": [
                {
                    "Type": "Lambda",
                    "Parameters": [
                        {
                            "ParameterName": "LambdaArn",
                            "ParameterValue": "'"$LAMBDA_ARN"'"
                        },
                        {
                            "ParameterName": "BufferSizeInMBs",
                            "ParameterValue": "1"
                        },
                        {
                            "ParameterName": "BufferIntervalInSeconds",
                            "ParameterValue": "60"
                        }
                    ]
                }
            ]
        }
    }'
