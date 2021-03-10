#!/bin/sh

#===========================================================================================
# Java Environment Setting
#===========================================================================================

TMP_JAVA_HOME="/opt/java/jdk8"

function is_java8 {
        local _java="$1"
        [[ -x "$_java" ]] || return 1
        [[ "$("$_java" -version 2>&1)" =~ 'java version "1.8' ]] || return 2
        return 0
}

#0(no corriendo),  1(si corriendo)
function is_brokerRunning {
        local _pid="$1"
        local pid=`ps ax | grep -i 'com.gcote.eventbus.broker.EventBusBrokerStartup' |grep java | grep -v grep | awk '{print $1}'|grep $_pid`
        if [ -z "$pid" ] ; then
            return 0
        else
            return 1
        fi
}


if [[ -d "$TMP_JAVA_HOME" ]] && is_java8 "$TMP_JAVA_HOME/bin/java"; then
        JAVA="$TMP_JAVA_HOME/bin/java"
elif [[ -d "$JAVA_HOME" ]] && is_java8 "$JAVA_HOME/bin/java"; then
        JAVA="$JAVA_HOME/bin/java"
elif  is_java8 "/opt/java/jdk8/bin/java"; then
    JAVA="/opt/java/jdk8/bin/java";
elif  is_java8 "/opt/java/jdk1.8/bin/java"; then
    JAVA="/opt/java/jdk1.8/bin/java";
elif  is_java8 "/opt/java/jdk/bin/java"; then
    JAVA="/opt/java/jdk/bin/java";
elif is_java8 "$(which java)"; then
        JAVA="$(which java)"
else
        echo -e "ERROR\t java(1.8) no encontrada, abortando operacion."
        exit 9;
fi

echo "broker utiliza la java localizada en= "$JAVA

ROCKETMQ_HOME=`cd "./.." && pwd`

error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}

export ROCKETMQ_HOME
export JAVA_HOME
#export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}

#===========================================================================================
# JVM Configuration
#===========================================================================================
JAVA_OPT="${JAVA_OPT} -server -Xms2g -Xmx2g -Xmn1g"
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:G1ReservePercent=25 -XX:InitiatingHeapOccupancyPercent=30 -XX:SoftRefLRUPolicyMSPerMB=0 -XX:SurvivorRatio=8 -XX:MaxGCPauseMillis=50"
JAVA_OPT="${JAVA_OPT} -verbose:gc -Xloggc:/dev/shm/mq_gc_%p.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintAdaptiveSizePolicy"
JAVA_OPT="${JAVA_OPT} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=30m"
JAVA_OPT="${JAVA_OPT} -XX:-OmitStackTraceInFastThrow"
JAVA_OPT="${JAVA_OPT} -XX:+AlwaysPreTouch"
JAVA_OPT="${JAVA_OPT} -XX:MaxDirectMemorySize=1g"
JAVA_OPT="${JAVA_OPT} -XX:-UseLargePages -XX:-UseBiasedLocking"
JAVA_OPT="${JAVA_OPT} -Djava.ext.dirs=${BASE_DIR}/lib:${BASE_DIR}/apps"
#JAVA_OPT="${JAVA_OPT} -Xdebug -Xrunjdwp:transport=dt_socket,address=9555,server=y,suspend=n"
JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"
JAVA_OPT="${JAVA_OPT} -Djava.security.egd=file:/dev/./urandom"

if [ -f "pid.file" ]; then
        pid=`cat pid.file`
        if ! is_brokerRunning "$pid"; then
            echo "el broker ya esta corriendo"
            exit 9;
        else
	    echo "err pid$pid, rm pid.file"
            rm pid.file
        fi
fi


nohup $JAVA ${JAVA_OPT} com.gcote.eventbus.broker.EventBusBrokerStartup -c ../conf/broker.properties 2>&1 >/dev/null &
echo $!>pid.file
