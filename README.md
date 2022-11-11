# canalzk-exporter

## 背景 
```
Canal实例本身集成了prometheus指标输出，针对延迟，sink,dump线程利用率，内存ringbuffer等多维度指标监控，


在Canal利用zookeeper搭建起来的HA模式时，有某些情况监控不到
1. instance异常失效退出
2. instance 在HA模式下只运行在1个canal节点, 无法监控出来
3. zookeeper保存数据库Postion等信息里，如时间戳，也可以作为canal同步数据库延迟非常重要的指标,生产环境业务数据库数据更新几乎是实时，zookeeper内此时间如有延迟，告警早发现
```
