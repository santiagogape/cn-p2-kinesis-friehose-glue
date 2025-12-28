export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="datalake-spotify-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)

# GLUE

aws glue create-database --database-input "{\"Name\":\"spotify_db\"}"

aws glue create-crawler \
    --name spotify-raw-crawler \
    --role $ROLE_ARN \
    --database-name spotify_db \
    --targets "{\"S3Targets\": [{\"Path\": \"s3://$BUCKET_NAME/raw/spotify_by_album_release_date\"}]}"

aws glue start-crawler --name spotify-raw-crawler

