# Scalable-and-Cloud-Programming-project

# Spark Co-Purchase Analysis Project

## Dataproc Execution
```bash
# Upload your JAR
gsutil cp data_jar-trabajo.jar gs://<YOUR_BUCKET>/jars/

# Submit job (replace CAPITALIZED values)
gcloud dataproc jobs submit spark \
  --cluster=CLUSTER_NAME \
  --region=REGION \
  --jar=gs://YOUR_BUCKET/jars/data_jar-trabajo.jar \
  -- gs://INPUT_BUCKET/orders.csv gs://OUTPUT_BUCKET/results/
```

## Local Testing
```bash
spark-submit \
  --class "OrderProductsCoPurchaseAnalysis" \
  data_jar-trabajo.jar \
  sample_orders.csv local_output/
```
