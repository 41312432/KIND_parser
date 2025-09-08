import argparse

def get_args():
    parser = argparse.ArgumentParser(description="MCP Document Processing Pipeline")

    parser.add_argument('--accelerator_thread', type=int, default=128)
    parser.add_argument('--model_path', type=str, default='/workspace/mnt/local-repo/hf_models/docling-models/model_artifacts')
    parser.add_argument('--data_dir', type=str, default='/workspace/data/mcpdata/pdf/mts_')
    parser.add_argument('--output_dir', type=str, default='/workspace/mnt/local-repo/mcpdata/parser_test')
    parser.add_argument('--file_list_path', type=str, default='/workspace/kind_parser/data/parser_test.txt')
    
    parser.add_argument('--pdf_parsing_num_workers', type=int, default=20)
    parser.add_argument('--image_resolution', type=float, default=4.0)
    
    parser.add_argument('--vlm_base_url', type=str, default="http://50.50.79.154:8000/v1")
    parser.add_argument('--vlm_model_name', type=str, default="Nanonets-OCR-s")
    parser.add_argument('--vlm_concurrency_limit', type=int, default=300)

    parser.add_argument('--steps', type=str, nargs='+', choices=['pdf_conversion', 'vlm_processing', 'content_structuring', 'db_loading'])

    return parser.parse_args()