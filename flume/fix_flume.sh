#!/bin/bash
export JAVA_HOME=/opt/jdk
cat > /usr/local/bin/which <<'W'
#!/bin/bash
command -v "$@"
W
chmod 755 /usr/local/bin/which
CP="$(/opt/hadoop/bin/hadoop classpath)"
echo "export HADOOP_HOME=/opt/hadoop" > /opt/flume/conf/flume-env.sh
echo "export FLUME_CLASSPATH=\"$CP\"" >> /opt/flume/conf/flume-env.sh
echo "export JAVA_OPTS=\"-Xms128m -Xmx400m\"" >> /opt/flume/conf/flume-env.sh
echo "which hadoop -> $(which hadoop)"
echo "classpath length: ${#CP}"
