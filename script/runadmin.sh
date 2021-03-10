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
elif  is_java8 "/opt/java/jdk/bin/java"; then
    JAVA="/opt/java/jdk/bin/java";
elif is_java8 "$(which java)"; then
        JAVA="$(which java)"
else
        echo -e "ERROR\t java(1.8) no encontrada, abortando operacion."
        exit 9;
fi

echo "admin utiliza la java localizada en= "$JAVA

ROCKETMQ_HOME=`cd "./.." && pwd`
export ROCKETMQ_HOME


JAVA_OPTS="-server -Xms256m -Xmx256m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:gc.log -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+DisableExplicitGC"
JAVA_OPT="${JAVA_OPT} -Djava.security.egd=file:/dev/./urandom"


APP_HOME=../.
APP_MAIN=com.gcote.eventbus.tools.command.EventBusAdminStartup
CLASSPATH=$APP_HOME/lib:$APP_HOME/apps:$APP_HOME/conf
ARGS="$@"

for libJar in "$APP_HOME"/lib/*.jar;
do
   CLASSPATH="$CLASSPATH":"$libJar"
done

for appJar in "$APP_HOME"/apps/*.jar;
do
   CLASSPATH="$CLASSPATH":"$appJar"
done

for confFile in "$APP_HOME"/conf/*.*;
do
   CLASSPATH="$CLASSPATH":"$confFile"
done
export CLASSPATH
#echo $CLASSPATH
#echo $APP_HOME
#echo $APP_MAIN

startup(){
     $JAVA $JAVA_OPTS -classpath $CLASSPATH $APP_MAIN $ARGS
}

if [ ! -d "../logs" ]; then
  mkdir ../logs
fi
if [ ! -d "../logs/otherdays" ]; then
  mkdir ../logs/otherdays
fi
if [ -f "../logs/tools.log" ]; then
today=`date '+%Y-%m-%d'`
files="tools.log"
for file in $files
do
timestamp=`stat -c %Y ../logs/$file`
fileTime=`date -d @$timestamp '+%Y-%m-%d'`
if [[ $fileTime != $today ]]
then
  num=$(ls ../logs/otherdays|grep "tools-$fileTime"|wc -l)
  mv ../logs/$file ../logs/otherdays/tools-$fileTime-$num.log
else
filesize=`ls -l ../logs/$file | awk '{ print $5 }'`
maxsize=$((1024*1024*2))
if [[ $filesize -gt $maxsize ]]
then
  num=$(ls ../logs/otherdays|grep "tools-$fileTime"|wc -l)
  mv ../logs/$file ../logs/otherdays/tools-$fileTime-$num.log
fi
fi
done
fi
startup $ARGS |tee -a ../logs/tools.log