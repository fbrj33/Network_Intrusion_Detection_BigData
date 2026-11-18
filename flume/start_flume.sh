#!/bin/bash
export HADOOP_HOME=/opt/hadoop
export HADOOP_COMMON_HOME=/opt/hadoop
export FLUME_CLASSPATH="$(/opt/hadoop/bin/hadoop classpath)"
export JAVA_OPTS="-Xms128m -Xmx400m"
exec /opt/flume/bin/flume-ng agent -n a1 -c /opt/flume/conf -f /flume/flows-agent.conf
