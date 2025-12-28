export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="datalake-spotify-${ACCOUNT_ID}"
export ROLE_ARN=$(aws iam get-role --role-name LabRole --query 'Role.Arn' --output text)

# GLUE ETL

aws s3 cp ./glue/daily_longest_songs.py s3://$BUCKET_NAME/scripts/
aws s3 cp ./glue/monthly_duration_time_aggregation.py s3://$BUCKET_NAME/scripts/

export DATABASE="spotify_db"
export TABLE="spotify_by_album_release_date"
export DAILY_OUTPUT="s3://$BUCKET_NAME/processed/daily_longest_songs/"
export MONTHLY_OUTPUT="s3://$BUCKET_NAME/processed/monthly_duration_time_aggregation/"

aws glue create-job \
    --name spotify-daily-longest-songs \
    --role $ROLE_ARN \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://'"$BUCKET_NAME"'/scripts/daily_longest_songs.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--database": "'"$DATABASE"'",
        "--table": "'"$TABLE"'",
        "--output_path": "'"$DAILY_OUTPUT"'",
        "--enable-continuous-cloudwatch-log": "true",
        "--spark-event-logs-path": "s3://'"$BUCKET_NAME"'/logs/"
    }' \
    --glue-version "4.0" \
    --number-of-workers 2 \
    --worker-type "G.1X"


aws glue create-job \
    --name spotify-monthly-duration-time-aggregation \
    --role $ROLE_ARN \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://'"$BUCKET_NAME"'/scripts/monthly_duration_time_aggregation.py",
        "PythonVersion": "3"
    }' \
    --default-arguments '{
        "--database": "'"$DATABASE"'",
        "--table": "'"$TABLE"'",
        "--output_path": "'"$MONTHLY_OUTPUT"'",
        "--enable-continuous-cloudwatch-log": "true",
        "--spark-event-logs-path": "s3://'"$BUCKET_NAME"'/logs/"
    }' \
    --glue-version "4.0" \
    --number-of-workers 2 \
    --worker-type "G.1X"


aws glue start-job-run --job-name spotify-daily-longest-songs

aws glue start-job-run --job-name spotify-monthly-duration-time-aggregation

# Ver estado
aws glue get-job-runs --job-name spotify-daily-longest-songs --max-items 1
aws glue get-job-runs --job-name spotify-monthly-duration-time-aggregation --max-items 1


