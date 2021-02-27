# 准备工具

[vearch](https://github.com/vearch/vearch)， [Prometheus](https://prometheus.io/)， [grafana](https://grafana.com/)

## 安装vearch

vearch的安装, 可以参考[build.sh](https://github.com/vearch/vearch/blob/master/build/build.sh)

## 安装prometheus
```bash
wget https://github.com/prometheus/prometheus/releases/download/v2.25.0/prometheus-2.25.0.linux-amd64.tar.gz
tar xvfz prometheus-2.25.0.linux-amd64.tar.gz
cd prometheus-2.25.0.linux-amd64

# Start Prometheus.
# By default, Prometheus stores its database in ./data (flag --storage.tsdb.path).
./prometheus --config.file=prometheus.yml
```

## grafana安装
```bash
wget https://dl.grafana.com/oss/release/grafana-7.4.3.linux-amd64.tar.gz
tar -zxvf grafana-7.4.3.linux-amd64.tar.gz
cd grafana-7.4.3

# start grafana
./bin/grafana-server
```

## 配置grafana
通过浏览器打开localhost:3000
登录账号/默认密码： admin/admin
添加数据源：configuration --> datasources --> add data source --> 修改Name和URL(name: vearch, URL: localhost:9090)
添加dashboard：create --> import (dashboard.json)



**完成 ！！！**