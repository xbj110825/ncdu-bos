# ncdu-bos
本工具为 BOS(Baidu Object Storage) 的 bucket 生成 [ncdu](http://dev.yorhel.nl/ncdu)  格式的 JSON 数据文件。

# 使用
```bash
$ pip install -e .
$ export BOS_ACCESS_KEY_ID=******
$ export BOS_SECRET_ACCESS_KEY=******
$ ncdu-bos --endpoint http://bj.bcebos.com --bucket my-bucket --output my-bucket.json
$ ncdu -f my-bucket.json
```