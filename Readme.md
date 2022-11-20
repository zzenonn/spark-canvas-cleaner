# DataFrames and SparkSQL

You may not need to use RDDs very often in Spark, as most of the developments are currently happening with DataFrames and DataSets. DataSets are statically typed constructs used in Scala and Java. DataSets aren't available in Python.

To compile, run `sbt assembly`. To run, run `spark-submit --files winemag-data-130k-v2.csv.gz target/scala-2.12/SparkSqlWine-assembly-2.0.jar`

```
gcloud dataproc clusters create canvas-etl2 --region asia-southeast1 --zone asia-southeast1-a --master-machine-type n2-standard-8 --master-boot-disk-size 500 --num-workers 5 --worker-machine-type n2-standard-8 --worker-boot-disk-size 500 --image-version 2.0-debian10 --scopes 'https://www.googleapis.com/auth/cloud-platform' --project canvasdata-304016 --max-idle 1h
```

## Acknowledgements

This data was taken from this [Kaggle dataset](https://www.kaggle.com/zynicide/wine-reviews#winemag-data-130k-v2.csv)
