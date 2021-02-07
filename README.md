# vearch-tools
some tools for [vearch](https://github.com/vearch/vearch)

## load vearch data
### requirements
```bash
conda install requests
conda install -c conda-forge python-rocksdb
```
### run
```bash
# Parameter:
# -f: the path of vearch data
# -t: the name of table
# -s: the path to save data
# --url: the format is `{router_ip}/{db_name}/{space_name}/`. Take Care: the last `/` is required.
# --int64: add this param if you set `-DTABLE_STR_INT64=ON` when building gamma.
# if you want save vearch data to local file, exec this cmd:
python load_vearch_data.py -f ./ -t 1 -s ./data/docs.txt
# if you want save vearch data to other vearch cluster exec this cmd:
python load_vearch_data.py -f ./ -t 1 --url 127.0.0.1:9001/test_db/test_space/
```
