#### A Python script that downloads workbooks from Tableau Server using the REST API and uploads them to AWS S3.

The script solves the problem of accidental workbook deletion. If you have only full TS backup , restoring a single deleted workbook requires deploying a full TS backup on new hardware, which is costly and time-consuming. The script automatically backups all workbooks from Tableau Server to an Amazon S3 Bucket.


Features:
- Uses threads for parallelism.
- Can sends data to Zabbix for monitoring.
- Logs errors to Sentry.
