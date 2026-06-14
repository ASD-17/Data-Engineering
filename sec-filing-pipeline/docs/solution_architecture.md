Filing submitted to EDGAR
         |
         | 0 to 30 seconds (Kafka poll interval)
         v
Event published to Kafka
         |
         | 0 to 30 seconds (Spark micro-batch interval)
         v
Written to Bronze
         |
         | Manual trigger in current implementation
         | Airflow schedule planned for production
         v
Written to Silver
         |
         v
Written to Gold with sentiment scores
         |
         v
Available in dashboard

Total end-to-end latency target: under 5 minutes filing to dashboard