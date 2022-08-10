# 标签系统

1. 构造1亿数据：`gg-rand -t 手机 -n 10000000 > label1y.txt`
1. 编译程序：`go install`
1. 启动程序：`labeldb`

## HTTP API

1. `POST /load/:file/:label` 加载指定的文件 file 中的手机号码，关联标签 label
1. `GET /labels/:mobile` 查询指定手机 mobile 的标签列表

## 演示

加载1亿手机数据，其标签为 label1

```sh
$ gurl POST :8080/load/label1y.txt/label1 -pb
{
    "body": {
        "cost": "15.813194653s",
        "lines": 10000000
    },
    "status": "ok"
}
```

查询手机的标签：

```sh
$ gurl :8080/labels/17638937669 -pb
{
    "body": {
        "cost": "180.784µs",
        "labels": ["label1"]
    },
    "status": "ok"
}
```
