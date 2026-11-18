#!/bin/bash
# Allow user "hadoop" to impersonate, and turn off doAs in Hive if hive-site.xml exists
patch() {
  f="$1"; props="$2"
  [ -n "$f" ] && [ -f "$f" ] || return 0
  grep -q "$3" "$f" || sed -i "s#</configuration>#${props}</configuration>#" "$f"
  echo "patched: $f"
}
PROXY='<property><name>hadoop.proxyuser.hadoop.hosts</name><value>*</value></property><property><name>hadoop.proxyuser.hadoop.groups</name><value>*</value></property>'
DOAS='<property><name>hive.server2.enable.doAs</name><value>false</value></property>'
for f in $(find / -name core-site.xml -not -path '/proc/*' 2>/dev/null); do patch "$f" "$PROXY" "proxyuser.hadoop"; done
for f in $(find / -name hive-site.xml -not -path '/proc/*' 2>/dev/null); do patch "$f" "$DOAS" "enable.doAs"; done