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
        echo -e "ERROR\t java(1.8) no encontrada, abortando operacion.">>read.me
        exit 9;
fi

echo "nameSrv utiliza la java localizada en= "$JAVA


ROCKETMQ_HOME=`cd "./.." && pwd`

error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}


export ROCKETMQ_HOME
#export JAVA_HOME
#export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}

#===========================================================================================
# JVM Configuration
#===========================================================================================
JAVA_OPT="${JAVA_OPT} -server -Xms4g -Xmx4g -Xmn2g"
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:MaxGCPauseMillis=100 -XX:G1ReservePercent=25 -XX:InitiatingHeapOccupancyPercent=30 -XX:-UseBiasedLocking -XX:+AlwaysPreTouch -XX:SoftRefLRUPolicyMSPerMB=0"
JAVA_OPT="${JAVA_OPT} -verbose:gc -Xloggc:/dev/shm/mq_gc_%p.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintAdaptiveSizePolicy"
JAVA_OPT="${JAVA_OPT} -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=30m"
JAVA_OPT="${JAVA_OPT} -XX:-OmitStackTraceInFastThrow"
JAVA_OPT="${JAVA_OPT} -XX:+AlwaysPreTouch"
JAVA_OPT="${JAVA_OPT} -Djava.ext.dirs=${BASE_DIR}/lib:${BASE_DIR}/apps"
#JAVA_OPT="${JAVA_OPT} -Dio.netty.recycler.maxCapacity.default=0"
#JAVA_OPT="${JAVA_OPT} -Xdebug -Xrunjdwp:transport=dt_socket,address=9555,server=y,suspend=n"
JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"
JAVA_OPT="${JAVA_OPT} -Djava.security.egd=file:/dev/./urandom"

nohup $JAVA ${JAVA_OPT} com.gcote.eventbus.namesrv.EventBusNamesrvStartup -c ../conf/namesrv.properties 2>&1 >/dev/null &

echo "Now Add crontab...."
crontab -l | grep -v namesrv_watchdog > tmp_crontab.txt
dir=`pwd`
echo "*/1 * * * * cd $dir; ./namesrv_watchdog.sh >/dev/null 2>&1" >> tmp_crontab.txt
crontab tmp_crontab.txt
rm tmp_crontab.txt