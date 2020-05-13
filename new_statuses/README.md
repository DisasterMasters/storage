# **Scrape tweets with Twitter API**

## Usage
```
python3 main.py confg.ini
```

## Specify keywords in a ini file
```
[power] // session name

// list of keywords
keywords = [
    "electric power",
    "electric outage",
    "electricity",
    "electricity out"
    ]
```
Running main.py with this sample ini file, will scrapes tweets that contain given keywords, then creates a collection called "statuses_a:power" in MongoDB and keep updating it. Note that the keys are top-level JSON objects (i.e. JSON objects or arrays). Multi-line keys are okay, but all lines after the first need to be indented, including the last line.

## confg.ini with multiple sessions
Creating multiple twitter collections. "statuses_a:power", "statuses_a:climate_change", "statuses_a:covid19"
```
[power]
keywords = [
    "electric power",
    "electric outage"
    ]

[climate_change]
keywords = [
    "climate change",
    "climatechange",
    "global warming"
    ]

[covid19]
keywords = [
    "covid 19",
    "covid19",
    "coronavirus"
    ]
```
