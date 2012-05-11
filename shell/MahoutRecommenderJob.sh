KDDC="/data/deploy/recommender/shell/lib/kddc-1.0.0.jar"
path="`dirname $0`"
INPUT_PATH=$1
OUTPUT_PATH=$2
if [ ! -n "$INPUT_PATH" -o ! -n "$OUTPUT_PATH" ];then
    echo 'Usage: ./UserActionSnsJoinJob INPUT_PATH OUTPUT_PATH USERS_FILE ITEMS_FILE MAX_PREFS_PER_USER'
    exit 0
else
    echo "INPUT_PATH: $INPUT_PATH, OUTPUT_PATH:$OUTPUT_PATH"
fi

$path/Job.sh com.snowballfinance.kddc.job.MahoutRecommenderJob --input "$1" --output "$2" --usersFile "$3" --itemsFile "$4" --maxPrefsPerUser "$5"  --similarityClassname SIMILARITY_COSINE




