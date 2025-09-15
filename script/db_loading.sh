set -xe

cd $KIND_PARSER_PATH

python main.py \
    --steps db_loading \
    --output_dir $OUTPUT_DIR