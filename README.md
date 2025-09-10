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