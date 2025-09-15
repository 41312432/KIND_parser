# vllm serve /data/model/Nanonets-OCR-s --served-model-name Nanonets-OCR-s --port 8000 &
# while true; do curl localhost:8000/health && break; sleep 1; done
# python main.py \
#     --steps vlm_processing \
#     --vlm_base_url http://localhost:8000/v1 \
#     --vlm_model_name Nanonets-OCR-s \
#     --vlm_concurrency_limit 300 \
#     --file_list_path /data/output-data/test.txt \
#     --output_dir /data/output-data/test 

set -xe

cd $KIND_PARSER_PATH

# vllm 별도 구성하는 것으로 변경
# vllm serve $VLM_MODEL_PATH --served-model-name $VLM_MODEL_NAME --port 8000 &
# while true; do curl localhost:8000/health && break; sleep 1; done

python main.py \
    --steps vlm_processing \
    --vlm_base_url $VLM_BASE_URL \
    --vlm_model_name $VLM_MODEL_NAME \
    --vlm_concurrency_limit $VLM_CONCURRENCY_LIMIT \
    --data_dir $DATA_DIR \
    --output_dir $OUTPUT_DIR