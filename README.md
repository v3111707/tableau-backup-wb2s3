# Tableau Workbook Backup to Amazon S3

A Python script for automated backup of Tableau Server workbooks to Amazon S3.  
The project was created to solve two problems:
- long-term archival of selected Tableau projects 
- self-service access to backup files for authorized users — without requiring administrator involvement

## 🔧 Features

- Full-site backup of Tableau workbooks (`[backup.sites]`)
- Selective backup of workbooks by project (`[[backup.projects]]`)
- Multithreaded downloads and S3 uploads
- Retry mechanism for failed operations
- Error tracking via Sentry (optional)
- Zabbix monitoring integration (heartbeat, metrics, error codes)
