#!/bin/sh

Result=$(ps -ef|grep NamesrvStartup|grep -v grep | awk '{print $2}')
if [ "" == "$Result" ]
then
	export LANG=en_US.UTF-8
	export LC_ALL=en_US.UTF-8
	export LC_CTYPE=en_US.UTF-8
	./runnamesrv.sh
fi