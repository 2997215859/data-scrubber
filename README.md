## build

```shell
# 编译产物在 dest 目录下
bash scripts/build.sh 
```

## run

```
nohup ./data-scrubber --config_file=conf/config.test.json > test.out 2>&1 &
```


## 

mock data acquire

```bash
head -n 4555000 20231108_Transaction.csv | tail -n +4550000
```

```bash

# daily data-scrubber
00 23 * * 1-5 cd /home/leewind/mnt/workspace/ruiy/code/data-scrubber/dest; ./run.sh -d;
```

```bash
## 同步 23 到 nas
nohup rsync -avzP /mnt/local/clean_stock_data/ /mnt/share/clean_stock_data/  > rsync_sync.log 2>&1 &
```

```bash
bash rsync_remote.sh --config config.ini
```

```bash
bash copy_stock_data.sh \
    --local-base /mnt/local/clean_stock_data \
    --remote-hosts "192.168.31.97 192.168.31.107" \
    --start-date 20240101
    --end-date 20240101
```