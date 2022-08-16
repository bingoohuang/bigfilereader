# 标签系统

1. 构造千万数据：`gg-rand -t 手机 -n 10000000 > label1qw.txt`
2. 编译安装：`go install`
3. 启动：`PARTITIONS=100 labeldb`，分区数越大，启动会稍慢一些，但是加载文件数据会快很多

## HTTP API

1. `POST /load/:file/:label` 加载指定的文件 file 中的手机号码，关联标签 label
1. `GET /labels/:mobile` 查询指定手机 mobile 的标签列表

## 演示

加载数据，其标签为 label1

```sh
$ gurl POST :8080/load/label1qw.txt/label2 -ugly -pb
{"body":{"cost":"7.548113339s","lines":10000000},"status":"ok"}
```

查询手机的标签：

```sh
$ head -3 label1qw.txt
15586520421
18297123752
13443134285
$ gurl :8080/labels/18297123752  -ugly -pb
{"body":{"cost":"319.628µs","labels":["label2"]},"status":"ok"}
```

不同分取值，加载速度的区别:

```sh
$ PARTITIONS=100 labeldb
2022/08/11 07:28:28 Listening on 8080
2022/08/11 07:28:39 start to load file label1.txt
2022/08/11 07:28:46 load file label1.txt with label label1 complete, cost 6.553783206s
^C

$ PARTITIONS=10 labeldb
2022/08/11 07:29:39 Listening on 8080
2022/08/11 07:29:45 start to load file label1.txt
2022/08/11 07:30:02 load file label1.txt with label label1 complete, cost 16.577362323s
```

```sh
$ PARTITIONS=100 labeldb
2022/08/11 11:25:08 Listening on 8080
2022/08/11 11:25:26 start to load file label1y.txt
2022/08/11 11:27:46 load file label1y.txt with label label2 andl lines 100000000 complete, cost 2m19.115438496s

$ gurl -pb :8080/labels/17660679064
{
  "body": {
    "cost": "1.280604ms",
    "labels": ["label2"]
  },
  "status": "ok"
}
```

## 并发读取大文件的性能对比

一亿行手机数据，无并发读取要 3.2 秒，12 并发读取要 2 秒。

```sh
$ labeldb
2022/08/16 14:18:14 Listening on 8080
2022/08/16 14:18:21 start to load file label1y.txt
2022/08/16 14:18:25 load file: label1y.txt with label: label2, lines: 100000000, sync: true complete, cost 4.90299822s
2022/08/16 14:18:30 start to load file label1y.txt
2022/08/16 14:18:33 load file: label1y.txt with label: label2, lines: 100000000, sync: true complete, cost 3.200350146s
2022/08/16 14:18:39 start to load file label1y.txt
2022/08/16 14:18:41 load file: label1y.txt with label: label2, lines: 100000000, sync: false complete, cost 2.096686172s
2022/08/16 14:18:43 start to load file label1y.txt
2022/08/16 14:18:46 load file: label1y.txt with label: label2, lines: 100000000, sync: false complete, cost 2.139934186s
```

```sh
$ gurl POST :8080/load/label1y.txt/label2 noop==y sync==y  -ugly -pb
{"body":{"cost":"4.90299822s","lines":100000000},"status":"ok"}
$ gurl POST :8080/load/label1y.txt/label2 noop==y sync==y  -ugly -pb
{"body":{"cost":"3.200350146s","lines":100000000},"status":"ok"}
$ gurl POST :8080/load/label1y.txt/label2 noop==y sync==n  -ugly -pb
{"body":{"cost":"2.096686172s","lines":100000000},"status":"ok"}
$ gurl POST :8080/load/label1y.txt/label2 noop==y sync==n  -ugly -pb
{"body":{"cost":"2.139934186s","lines":100000000},"status":"ok"}
```
