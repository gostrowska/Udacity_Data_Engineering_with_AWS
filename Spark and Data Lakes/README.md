
# STEDI Human Balance Analytics

## Project Description

STEDI has created a step trainer designed to support users in their fitness routines. The device is equipped with sensors that collect data, which is then used to train a machine learning model. A companion mobile app also connects with the trainer, capturing additional customer data.

For this project, I built an ETL pipeline using an AWS-based stack—including S3, AWS Glue, Athena, and Apache Spark—to clean and process the data. The architecture follows a three-tiered structure:

* **Landing Zone**: Stores raw, unprocessed data collected from the STEDI device and the mobile app.
* **Trusted Zone**: Contains cleaned and sanitized data filtered from the Landing Zone, specifically including only data from customers who have consented to share their information for research purposes.
* **Curated Zone**: Hosts fully processed datasets ready for downstream use, including the training dataset for the machine learning model and the finalized customers table.
