set -xe

cd $KIND_PARSER_PATH

python main.py \
    --steps db_loading \
    --file_list_path $FILE_LIST_PATH \
    --output_dir $OUTPUT_DIR