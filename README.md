# scala.spark

This was a school project that runs on Spark/HDFS in cluster mode. The challenge was optimizing resource configuration and code to work with a shared cluster that could go down for a minute or a few hours without notice, sometimes after a job has been submitted. Lots of debugging. The key was maximizing threads/RAM per executor and minimizing persistence and rdd shuffles.
