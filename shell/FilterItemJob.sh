KDDC="/data/deploy/recommender/shell/lib/kddc-1.0.0.jar"
path="`dirname $0`"
INPUT_PATH=$1
OUTPUT_PATH=$2
if [ ! -n "$INPUT_PATH" -o ! -n "$OUTPUT_PATH" ];then
    echo 'Usage: ./FilterItemJob INPUT_PATH OUTPUT_PATH  '
    exit 0
else
    echo "INPUT_PATH: $INPUT_PATH, OUTPUT_PATH:$OUTPUT_PATH"
fi


$path/Job.sh $KDDC com.snowballfinance.kddc.job.FilterItemJob "$1" "$2" 
