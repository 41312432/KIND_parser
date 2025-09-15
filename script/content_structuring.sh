# python main.py \
#     --steps content_structuring \
#     --file_list_path /data/output-data/test.txt \
#     --output_dir /data/output-data/test 
set -xe

cd $KIND_PARSER_PATH

python main.py \
    --steps content_structuring \
    --output_dir $OUTPUT_DIR