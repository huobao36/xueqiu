KDDC="/data/deploy/recommender/shell/lib/kddc-1.0.0.jar"


$HADOOP jar $RECOMMENDER com.snowballfinance.recommender.job.JaccardIndexJob "input" "$JACCARDINDEX_OUTPUT_DIR/"

