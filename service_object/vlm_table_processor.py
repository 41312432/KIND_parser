import os
import asyncio
import base64
import re
import logging
from collections import Counter
from pathlib import Path

from openai import AsyncOpenAI
from bs4 import BeautifulSoup, NavigableString
from tqdm.asyncio import tqdm_asyncio

class VLMTableProcessor:

    def __init__(self, base_url: str, model_name: str, concurrency_limit: int, logger: logging.Logger):
        self.client = AsyncOpenAI(api_key="1", base_url=base_url)
        self.model_name = model_name
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.logger = logger

    def _encode_image(self, image_path: str) -> str:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")

    # FIXME: Fit to Nanonet Model
    def _fix_html_table(self, html_string: str) -> str:
        if not html_string or '<table' not in html_string:
            return html_string
        
        soup = BeautifulSoup(html_string, 'html.parser')
        
        for br in soup.find_all("br"):
            br.replace_with(" ")
        for cell in soup.find_all(['td', 'th']):
            cell.string = ' '.join(cell.get_text(strip=True).split())

        table = soup.find('table')
        if not table: return str(soup)
        rows = table.find_all('tr')
        if not rows: return str(soup)

        grid_for_width_check = []
        row_widths = []
        max_cols_so_far = 0
        for r_idx, row in enumerate(rows):
            while len(grid_for_width_check) <= r_idx: grid_for_width_check.append([None] * max_cols_so_far)
            c_idx = 0
            current_row_width = sum(1 for cell in grid_for_width_check[r_idx] if cell is not None)
            for cell in row.find_all(['td', 'th']):
                while c_idx < len(grid_for_width_check[r_idx]) and grid_for_width_check[r_idx][c_idx] is not None: c_idx += 1
                colspan = int(cell.get('colspan', 1)); rowspan = int(cell.get('rowspan', 1))
                current_row_width += colspan
                for i in range(rowspan):
                    for j in range(colspan):
                        if len(grid_for_width_check) <= r_idx + i: grid_for_width_check.append([None] * max_cols_so_far)
                        while len(grid_for_width_check[r_idx + i]) < c_idx + j + 1: grid_for_width_check[r_idx + i].append(None)
                        grid_for_width_check[r_idx + i][c_idx + j] = cell
                max_cols_so_far = max(max_cols_so_far, c_idx + colspan)
            row_widths.append(current_row_width)
        
        if not row_widths: return table.prettify()
        true_width = Counter(row_widths).most_common(1)[0][0]
        
        grid = [([None] * true_width) for _ in range(len(rows))]
        for r_idx, row in enumerate(rows):
            c_idx = 0
            for cell in row.find_all(['td', 'th']):
                while c_idx < true_width and grid[r_idx][c_idx] is not None: c_idx += 1
                if c_idx >= true_width: continue
                rowspan = int(cell.get('rowspan', 1)); colspan = int(cell.get('colspan', 1))
                for i in range(rowspan):
                    for j in range(colspan):
                        if r_idx + i < len(grid) and c_idx + j < true_width:
                            grid[r_idx + i][c_idx + j] = cell
        
        new_soup = BeautifulSoup("<table></table>", 'html.parser')
        new_table = new_soup.find('table')
        processed_cells = set()
        for r_idx, row_data in enumerate(grid):
            new_row = new_soup.new_tag('tr')
            for c_idx, cell_obj in enumerate(row_data):
                if (r_idx, c_idx) in processed_cells or cell_obj is None: continue
                row_span, col_span = 1, 1
                while r_idx + row_span < len(grid) and c_idx < len(grid[r_idx + row_span]) and grid[r_idx + row_span][c_idx] is cell_obj: row_span += 1
                while c_idx + col_span < len(row_data) and row_data[c_idx + col_span] is cell_obj: col_span += 1
                new_cell = new_soup.new_tag(cell_obj.name)
                new_cell.string = cell_obj.get_text(strip=True)
                if row_span > 1: new_cell['rowspan'] = str(row_span)
                if col_span > 1: new_cell['colspan'] = str(col_span)
                new_row.append(new_cell)
                for i in range(row_span):
                    for j in range(col_span): processed_cells.add((r_idx + i, c_idx + j))
            if new_row.find(['td', 'th']): new_table.append(new_row)
        
        return new_table.prettify()

    async def _async_process_image(self, image_path: str):
        async with self.semaphore:
            try:
                parent_dir = Path(image_path).parent.parent
                output_dir = parent_dir / 'table_parsed'
                output_dir.mkdir(exist_ok=True)
                
                #NOTE: vllm serving
                img_base64 = self._encode_image(image_path)
                response = await self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[{"role": "user","content": [{"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_base64}"}},{"type": "text", "text": "Extract table in HTML format."}]}],
                    temperature=0.0, max_tokens=4096
                )
                html_output = response.choices[0].message.content
                if not html_output: return

                image_filename = os.path.basename(image_path)
                output_filename = os.path.splitext(image_filename)[0] + '.txt'
                output_filepath = output_dir / output_filename
                
                #FIXME:
                fixed_html = self._fix_html_table(html_output)
                with open(output_filepath, 'w', encoding='utf-8') as f:
                    f.write(fixed_html)
            except Exception as e:
                self.logger.error(f"Error processing image {image_path}: {e}")
    
    # TODO: Cannot replaced moment (Maybe attached)
    def _replace_markdown_tables(self, root_dir: Path):
        self.logger.info("Replacing markdown tables with parsed HTML tables (content2.md)...")
        md_table_pattern = re.compile(r"^(?:\|.*(?:\r?\n|$))+", re.MULTILINE)
        
        for dirpath, _, filenames in os.walk(root_dir):
            current_dir = Path(dirpath)
            content_md_path = current_dir / 'content.md'
            parsed_dir_path = current_dir / 'table_parsed'

            if content_md_path.exists() and parsed_dir_path.is_dir():
                with open(content_md_path, 'r', encoding='utf-8') as f:
                    md_content = f.read()

                md_tables_found = md_table_pattern.findall(md_content)
                parsed_files = sorted(
                    [f for f in os.listdir(parsed_dir_path) if f.endswith('.txt')],
                    key=lambda x: int(os.path.splitext(x)[0])
                )

                if not md_tables_found:
                    continue
                
                final_tables_to_insert = []
                if len(md_tables_found) == len(parsed_files):
                    for filename in parsed_files:
                        with open(parsed_dir_path / filename, 'r', encoding='utf-8') as f:
                            final_tables_to_insert.append(f.read())
                else:
                    self.logger.warning(f"Table count mismatch in {dirpath}. Skipping replacement.")
                    continue
                
                if final_tables_to_insert:
                    replacements_iter = iter(final_tables_to_insert)
                    def replacer(match):
                        try:
                            return next(replacements_iter)
                        except StopIteration:
                            return match.group(0)
                    
                    modified_md_content = md_table_pattern.sub(replacer, md_content)
                    output_path = current_dir / 'content2.md'
                    with open(output_path, 'w', encoding='utf-8') as f:
                        f.write(modified_md_content)

    # TODO: Cannot merged moment (Maybe attached)
    def _merge_adjacent_tables(self, root_dir: Path):
        self.logger.info("Merging adjacent HTML tables (content3.md)...")
        for dirpath, _, filenames in os.walk(root_dir):
            current_dir = Path(dirpath)
            content2_path = current_dir / 'content2.md'
            if content2_path.exists():
                output_path = current_dir / 'content3.md'
                with open(content2_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                soup = BeautifulSoup(content, 'html.parser')
                while True:
                    tables = soup.find_all('table')
                    if len(tables) < 2:
                        break
                    
                    merged_in_pass = False
                    for i in range(len(tables) - 1):
                        table1 = tables[i]
                        table2 = tables[i+1]
                        
                        if table1.next_sibling is table2 or (isinstance(table1.next_sibling, NavigableString) and not table1.next_sibling.strip()):
                            rows2 = table2.find_all('tr')
                            for row in rows2:
                                table1.append(row)
                            table2.decompose()
                            merged_in_pass = True
                            break 
                    
                    if not merged_in_pass:
                        break
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(str(soup))

    async def process_all_tables(self, root_dir: Path):
        image_paths = [str(p) for p in root_dir.glob('**/table_images/*.png')]
        
        if not image_paths:
            self.logger.info("No table images found to process.")
            return

        self.logger.info(f"Found {len(image_paths)} table images to process.")
        tasks = [self._async_process_image(path) for path in image_paths]
        await tqdm_asyncio.gather(*tasks, desc="Parsing Table Images with VLM")
        
        self._replace_markdown_tables(root_dir)
        self._merge_adjacent_tables(root_dir)