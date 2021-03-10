#!/bin/sh

echo "Eliminando crontab....."
crontab -l | grep -v broker_watchdog | grep -v namesrv_watchdog > tmp_crontab.txt
crontab  tmp_crontab.txt
rm -f tmp_crontab.txt
echo "Termino...."

case $1 in
    broker)

    pid=`ps ax | grep -i 'com.gcote.eventbus.broker.EventBusBrokerStartup' |grep java | grep -v grep | awk '{print $1}'`
    if [ -z "$pid" ] ; then
            echo "EventBusBroker no esta corriendo."
            exit -1;
    fi

    if [ "$2" != "-f" ] ; then
            echo "leyendo de pid.file"
            pid=`cat pid.file`
    fi

    echo "El EventBusBroker(${pid}) esta corriendo..."
    kill ${pid}

    echo "Enviando apagado de EventBusBroker(${pid}) OK"

    # esperando que el broker termine
    while ps -p ${pid} > /dev/null 2>&1; do sleep 1; echo "esperando que el broker ${pid} termine para salir."; done;

    rm -rf pid.file

    echo "salida proceso broker"
    ;;
    namesrv)

    pid=`ps ax | grep -i 'com.gcote.eventbus.namesrv.EventBusNameSrvStartup' |grep java | grep -v grep | awk '{print $1}'`
    if [ -z "$pid" ] ; then
            echo "EventBusNameSrv no esta corriendo."
            exit -1;
    fi

      echo "El EventBusNameSrv(${pid}) esta corriendo..."

    kill ${pid}

    echo "Enviando apagado de EventBusNameSrv(${pid}) OK"
    ;;
    *)
    echo "Para usarlo indicar: `basename $0` (broker|namesrv)"
esac