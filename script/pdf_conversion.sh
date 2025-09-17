# python main.py \
#     --steps pdf_conversion \
#     --model_path /data/model/docling-models/model_artifacts \
#     --accelerator_thread 128 \
#     --image_resolution 4.0 \
#     --data_dir /data/local-repo/mcpdata/mts_ \
#     --file_list_path /data/output-data/test.txt \
#     --output_dir /data/output-data/test

set -xe

cd $KIND_PARSER_PATH

echo $MODEL_PATH
echo $ACCELERATOR_THREAD

python main.py \
    --steps pdf_conversion \
    --num_global_worker $NUM_GLOBAL_WORKER \
    --num_local_worker $NUM_LOCAL_WORKER \
    --root_global_worker_id $ROOT_GLOBAL_WORKER_ID \
    --model_path $MODEL_PATH \
    --accelerator_thread $ACCELERATOR_THREAD \
    --image_resolution $IMAGE_RESOLUTION \
    --pdf_parsing_num_workers $PDF_PARSING_NUM_WORKERS \
    --data_dir $DATA_DIR \
    --output_dir $OUTPUT_DIR

# export KIND_PARSER_PATH=/workspace/kind_parser/; source .env; ./pdf_conversion.sh