KDDC="/data/deploy/recommender/shell/lib/kddc-1.0.0.jar"
path="`dirname $0`"
INPUT_PATH=$1
OUTPUT_PATH=$2
if [ ! -n "$INPUT_PATH" -o ! -n "$OUTPUT_PATH" ];then
    echo 'Usage: ./UserProfileJob.sh INPUT_PATH OUTPUT_PATH  '
    exit 0
else
    echo "INPUT_PATH: $INPUT_PATH, OUTPUT_PATH:$OUTPUT_PATH"
fi


$path/Job.sh com.snowballfinance.kddc.job.UserProfileJob "$1" "$2" 
