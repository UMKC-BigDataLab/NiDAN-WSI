rm *.JPEG
cat tempQueriesToRun.scala | ./initialize_spark_shell.sh > Log_7_1.log 2>&1	
cat Log_7_1.log | grep 'Time elapsed' | tail -4 | sed -e 's/Time\ elapsed:\ //' -e 's/\ seconds//'

rm *.JPEG
cat tempQueriesToRun.scala | ./initialize_spark_shell.sh > Log_7_2.log 2>&1	
cat Log_7_2.log | grep 'Time elapsed' | tail -4 | sed -e 's/Time\ elapsed:\ //' -e 's/\ seconds//'
rm *.JPEG
cat tempQueriesToRun.scala | ./initialize_spark_shell.sh > Log_7_3.log 2>&1	
cat Log_7_3.log | grep 'Time elapsed' | tail -4 | sed -e 's/Time\ elapsed:\ //' -e 's/\ seconds//'
