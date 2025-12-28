export AWS_REGION="us-east-1"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="datalake-spotify-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)

echo "Usando Bucket: $BUCKET_NAME y Role: $ROLE_ARN"

# Crear el bucket
aws s3 mb s3://$BUCKET_NAME

# Crear carpetas (objetos vacíos con / al final)
aws s3api put-object --bucket $BUCKET_NAME --key raw/
aws s3api put-object --bucket $BUCKET_NAME --key raw/spotify_by_album_release_date/
aws s3api put-object --bucket $BUCKET_NAME --key processed/
aws s3api put-object --bucket $BUCKET_NAME --key config/
aws s3api put-object --bucket $BUCKET_NAME --key scripts/
aws s3api put-object --bucket $BUCKET_NAME --key queries/
aws s3api put-object --bucket $BUCKET_NAME --key scripts/
aws s3api put-object --bucket $BUCKET_NAME --key errors/
aws s3api put-object --bucket $BUCKET_NAME --key logs/


aws kinesis create-stream --stream-name spotify-stream --shard-count 1
