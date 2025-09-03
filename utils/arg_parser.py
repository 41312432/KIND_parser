import argparse

def get_args():
    parser = argparse.ArgumentParser(description="MCP Document Processing Pipeline")

    parser.add_argument('--accelerator_thread', type=int, default=128)
    parser.add_argument('--model_path', type=str, default='/workspace/docling/docling-models/model_artifacts')
    parser.add_argument('--data_dir', type=str, default='/workspace/data/mcpdata/pdf/mts_')
    parser.add_argument('--output_dir', type=str, default='/workspace/mnt/local-repo/mcpdata/contents_final_resolution_4')
    parser.add_argument('--file_list_path', type=str, default='/workspace/mnt/local-repo/mcpdata/mcp_list/todo_file_0.txt')
    
    parser.add_argument('--image_resolution', type=float, default=4.0, 
                        help="Resolution for converting all PDF images (pages, tables, etc.).")
    
    parser.add_argument('--vlm_base_url', type=str, default="http://50.50.79.154:8000/v1")
    parser.add_argument('--vlm_model_name', type=str, default="Nanonets-OCR-s")
    parser.add_argument('--vlm_concurrency_limit', type=int, default=300)

    return parser.parse_args()