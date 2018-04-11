QUERIES="../experiments/queries/READY/"

for exp in 1 2 3
do
	for query in $(ls $QUERIES)
	do
		LOG="../logs/nidan.query.$query.EXP-$exp.log"
		echo "Running: $query Log: $LOG "
		cat $QUERIES/$query | ./init_spark_shell.sh > $LOG 2>&1
	done
done

