# 📑 Logs folder
When executed, the script will store a ***logs.log*** file in this folder.

The way it logs it can be configured through ***config_logs.conf*** file in project's root.

By default, logs will have the following structure:

```
[time] - [source] - [level] - [msg]
```

Where:
- ***time***: time in 'YYYY-MM-DDThh:mm' format.
- ***source***: script that emitted the log. It could be ETL, DB or Scraper.
- ***level***: severity level. It could be DEBUG, INFO, WARNING, ERROR or CRITICAL.
- ***msg***: message to log.

Log example:
```
'2023-04-04T13:45' - Scraper - INFO - All app details scraped successfully!
```