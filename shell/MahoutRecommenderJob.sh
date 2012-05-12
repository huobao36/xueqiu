
KDDC="/data/deploy/recommender/shell/lib/kddc-1.0.0.jar"
JOB_NAME="/data/deploy/kddc/webapp/WEB-INF/lib/mahout-core-0.6.jar"
path="`dirname $0`"
INPUT_PATH=$1
OUTPUT_PATH=$2
if [ ! -n "$INPUT_PATH" -o ! -n "$OUTPUT_PATH" ];then
    echo 'Usage: ./MahoutRecommenderJob INPUT_PATH OUTPUT_PATH USERS_FILE ITEMS_FILE FILTER_FILE'
    exit 0
else
    echo "INPUT_PATH: $INPUT_PATH, OUTPUT_PATH:$OUTPUT_PATH"
fi

$path/Job.sh $JOB_NAME org.apache.mahout.cf.taste.hadoop.item.RecommenderJob --input "$1" --output "$2" --usersFile "$3" --itemsFile "$4" --filterFile "$5"  --similarityClassname SIMILARITY_COSINE --maxPrefsPerUser 10000 --maxPrefsPerUserInItemSimilarity 10000 --maxSimilaritiesPerItem 10000 --numRecommendations 3




