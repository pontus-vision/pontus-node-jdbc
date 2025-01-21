#!/bin/bash

TOKEN="6b6928de-1efc-4a67-aa76-2a7090ee0eb3"
BASE_URL="http://localhost:8888"
KERNEL_NAME="pyspark"  # Change this if your kernel name is different

# Create a session
SESSION=$(curl -s -X POST -H "Authorization:token $TOKEN" "$BASE_URL/api/sessions" \
    -H 'Content-Type: application/json' \
    --data-binary @- << EOF
{
  "kernel": {
    "name": "$KERNEL_NAME"
  }
}
EOF
)
echo $SESSION
SESSION_ID=$(echo $SESSION | jq -r '.id')
KERNEL_ID=$(echo $SESSION | jq -r '.kernel.id')

echo "Session ID: $SESSION_ID"
echo "Kernel ID: $KERNEL_ID"

# Define the Spark SQL code
CODE=$(cat <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()
df = spark.sql("SELECT * FROM delta.`/data/silver/split/delta-parquet/app-history`")
df.show()
EOF
)

# Execute the code
curl -s -X POST -H "Authorization: token $TOKEN" "$BASE_URL/api/kernels/$KERNEL_ID/execute" \
    -H 'Content-Type: application/json' \
    --data-binary @- << EOF
{
  "code": "$CODE"
}
EOF

# Cleanup: delete the session
curl -s -X DELETE -H "Authorization: token $TOKEN" "$BASE_URL/api/sessions/$SESSION_ID"

