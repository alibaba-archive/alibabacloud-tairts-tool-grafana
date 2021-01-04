# TairTs_to_Grafana

### Running the App

首先进行安装:
````
npm install package.json
````

运行命令如下。注意需要传递redis的host与port，最后需要加入聚合操作的过滤条件。
````
node index.js 127.0.0.1 6379 unit=zhangbei
````

默认端口为3333。可通过访问http://localhost:3333 进行确认。