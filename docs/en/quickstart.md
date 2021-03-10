# Quick start

### dependencies
```
64bit OS, Linux/Unix/Mac is recommended;
64bit JDK 1.8+;
Gradle 3.x;
4g+ free disk for Broker server
```

### download and build

```
download from git
unzip eventbus-master.zip
cd eventbus-master
gradle clean dist tar -x test

You can get a tar.gz package in directory named 'build'
```

### Deployment

deploy EventBusNamesrv
```
tar -zxvf EventBus_1.0.0.tar.gz
cd bin
sh runnamesrv.sh
```

deploy EventBusBroker
```
tar -zxvf EventBus_1.0.0.tar.gz
cd bin
sh runbroker.sh
```
