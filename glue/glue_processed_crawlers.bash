export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="datalake-spotify-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)
export DAILY_OUTPUT="s3://$BUCKET_NAME/processed/daily_longest_songs/"
export MONTHLY_OUTPUT="s3://$BUCKET_NAME/processed/monthly_duration_time_aggregation/"

# GLUE CRAWLER PROCESSED

aws glue create-crawler \
    --name spotify-processed-daily-longest-songs-crawler \
    --role $ROLE_ARN \
    --database-name spotify_db \
    --targets "{\"S3Targets\": [{\"Path\": \"$DAILY_OUTPUT\"}]}"\
    --schema-change-policy '{"UpdateBehavior":"LOG","DeleteBehavior":"LOG"}' # para que no toque el esquema ya creado

aws glue start-crawler --name spotify-processed-daily-longest-songs-crawler


aws glue create-crawler \
    --name spotify-processed-monthly-duration-time-aggregation-crawler \
    --role $ROLE_ARN \
    --database-name spotify_db \
    --targets "{\"S3Targets\": [{\"Path\": \"$MONTHLY_OUTPUT\"}]}"\
    --schema-change-policy '{"UpdateBehavior":"LOG","DeleteBehavior":"LOG"}'

aws glue start-crawler --name spotify-processed-monthly-duration-time-aggregation-crawler