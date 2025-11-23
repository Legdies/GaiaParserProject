# Gaia Parser

Initially was my project that I did from boredom. 

But now this project can be used somewhere other than just my machine because why not. 

It uses Gaia Project files available on internet and couple libraries to build a single or multiple .parquet files with all stucture needed for the projects.

It requires high disk space and preferably high RAM space to use. 

Highly not recommended on low-end devices, laptops and most of the PC's as this project intended to be used on DEDICATED servers. 

## Help

While this project provides help command by itself, I will give you some basics

If you need just download whole sources:
```shell
python3 gaia.py download 
```

To start up pipeline processing:
- Default 6 thread
- At the end creates single .parquet file with full structure
- Stores cache

```shell
python3 gaia.py pipeline
```

To verify everything:
```shell
python3 gaia.py verify
```

I hope it was useful but you always can use --help arg inside.