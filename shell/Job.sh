KDDC_LIB_HOME="/data/deploy/kddc/webapp/WEB-INF/lib"
KDDC_JOB="/data/deploy/kddc/shell/lib/kddc-1.0.0.jar"
HADOOP_HOME="/data/hadoop"
if [ ! -n "$KDDC_JOB" ];then
    echo 'Usage: ./Job.sh JobName ClassName Params  '
    exit 0
else
    echo "JobName: $KDDC_JOB"
fi

KDDC_CLASSPATH=""
for f in $KDDC_LIB_HOME/*.jar; do
	echo "Add Lib to ClassPath : $f"
	if [ "$KDDC_CLASSPATH" == "" ] ; then
		KDDC_CLASSPATH=$f
	else 
		KDDC_CLASSPATH=${KDDC_CLASSPATH}:$f;
	fi
done

echo -e "KDDC_CLASSPATH:$KDDC_CLASSPATH"

if [ "$HADOOP_CLASSPATH" != "" ] ; then
	HADOOP_CLASSPATH=${KDDC_CLASSPATH}:${HADOOP_CLASSPATH}
else 
	HADOOP_CLASSPATH=$KDDC_CLASSPATH
fi

echo -e "HADOOP_CLASSPATH:$HADOOP_CLASSPATH"

export HADOOP_CLASSPATH
echo -e "Job Class Name: $1"

echo -e "Params : ${'$@'#'$1'}"

exec "$HADOOP_HOME/bin/hadoop" jar $KDDC_JOB "$1" "$@#$1"

