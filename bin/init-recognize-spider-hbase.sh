cd /usr/lib/hbase/bin

for tablename in 'recognize_fact' 'recognize_spider' ; do
  if ! echo -e "exists '$tablename'" | hbase shell 2>&1 | grep -q "does exist" 2>/dev/null ; then
    echo -e "create '$tablename', 'info'" | hbase shell
  else
    echo -e "truncate '$tablename'" | hbase shell
  fi
done

