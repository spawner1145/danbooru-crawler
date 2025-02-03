# 相关参数在代码底部配置
import httpx
import asyncio
import aiofiles
import zipfile
import os
from pathlib import Path
import csv
from PIL import Image

async def download_image(session, url, filename, line, zipf, csv_writer, csvfile, error_flag, line_number):
    max_retries = 10
    retries = 0
    
    while retries < max_retries:
        try:
            response = await session.get(url)
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'video' in content_type:
                    print(f"跳过视频文件: {url}")
                    return False
                
                async with aiofiles.open(filename, 'wb') as f:
                    await f.write(response.content)
                
                if filename.suffix.lower() == '.webp':
                    new_filename = filename.with_suffix('.jpg')
                    with Image.open(filename) as img:
                        img.save(new_filename, 'JPEG')
                    os.remove(filename)
                    filename = new_filename
                    print(f"转换完成: {filename}")
                
                print(f"下载完成: {filename}")
                
                csv_row = {'filename': filename.name, 'tags': line}
                csv_writer.writerow(csv_row)
                print(f"写入CSV: {csv_row}")
                
                csvfile.flush()
                
                if filename.name in zipf.namelist():
                    del zipf.filelist[zipf.namelist().index(filename.name)]
                    del zipf.NameToInfo[filename.name]
                zipf.write(filename, arcname=filename.name)
                
                os.remove(filename)
                return True
            else:
                print(f"下载失败 (状态码 {response.status_code}): {url}")
                break
        except Exception as e:
            print(f"下载异常 (尝试 {retries + 1}/{max_retries}): {e} - {url}")
            retries += 1
    
    print(f"放弃下载: {url}")
    error_flag['value'] = True
    error_flag['lines'].append(line_number)
    return False

async def process_line(session, line, line_number, max_images=10, zipf=None, csv_writer=None, csvfile=None, existing_filenames=None, error_flag=None):
    processed_tags = line.replace(' ', '_')
    page = 1
    downloaded_count = 0
    
    while downloaded_count < max_images:
        url = f"https://kagamihara.donmai.us/posts.json?page={page}&tags={processed_tags}"
        try:
            response = await session.get(url)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    break
                for item in data:
                    media_asset = item.get('media_asset', {})
                    variants = media_asset.get('variants', [])
                    
                    size_720_variant = next((v for v in variants if v['type'] == '720x720'), None)
                    sample_variant = next((v for v in variants if v['type'] == 'sample'), None)
                    preview_variant = next((v for v in variants if v['type'] == 'preview'), None)
                    original_variant = next((v for v in variants if v['type'] == 'original'), None)
                    
                    variant = size_720_variant or sample_variant or preview_variant or original_variant
                    if variant:
                        image_url = variant['url']
                        image_name = os.path.basename(image_url)
                        
                        comparison_filename = image_name
                        if comparison_filename.endswith('.webp'):
                            comparison_filename = comparison_filename[:-5] + '.jpg'
                        
                        unique_filename = images_dir / f"{image_name}"
                        
                        if comparison_filename in existing_filenames:
                            print(f"文件已存在: {comparison_filename}, 跳过下载")
                            downloaded_count += 1
                            continue
                        
                        success = await download_image(session, image_url, unique_filename, line, zipf, csv_writer, csvfile, error_flag, line_number)
                        if success:
                            downloaded_count += 1
                            existing_filenames.add(comparison_filename)
                            if downloaded_count >= max_images:
                                break
                        elif error_flag['value']:
                            return
                    else:
                        print(f"未找到合适的变体: {item}")
            else:
                print(f"请求失败 (状态码 {response.status_code}): {url}")
                break
        except Exception as e:
            print(f"请求异常: {e} - {url}")
            error_flag['value'] = True
            error_flag['lines'].append(line_number)
            return
        page += 1

async def read_existing_filenames(csv_file):
    existing_filenames = set()
    if os.path.exists(csv_file):
        with open(csv_file, mode='r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            next(csv_reader, None)
            for row in csv_reader:
                existing_filenames.add(row['filename'])
    return existing_filenames

async def main(txt_path, output_zip, csv_file, timeout=1000, proxies=None, start_line=1, max_lines_per_batch=5, max_images=10):
    print("开始执行脚本...")
    print(f"当前工作目录: {os.getcwd()}")
    print(f"尝试打开文件: {txt_path}")

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout), proxies=proxies) as session:
        lines = []
        try:
            async with aiofiles.open(txt_path, mode='r') as file:
                line_number = 0
                while True:
                    line = await file.readline()
                    if not line:
                        break
                    line_number += 1
                    if line_number >= start_line:
                        lines.append(line.strip())
            print(f"成功读取文件: {txt_path}, 行数: {len(lines)}")
        except FileNotFoundError:
            print(f"错误: 文件 {txt_path} 不存在.")
            return None
        
        global images_dir
        images_dir = Path("images")
        images_dir.mkdir(parents=True, exist_ok=True)
        print(f"创建/检查图片目录: {images_dir}")

        with open(csv_file, mode='a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['filename', 'tags']
            csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if csvfile.tell() == 0:
                csv_writer.writeheader()
            
            existing_filenames = await read_existing_filenames(csv_file)
            
            with zipfile.ZipFile(output_zip, 'a', zipfile.ZIP_DEFLATED) as zipf:
                error_flag = {'value': False, 'lines': []}
                batch_start_line = start_line
                while lines:
                    batch_lines = lines[:max_lines_per_batch]
                    lines = lines[max_lines_per_batch:]
                    
                    for i, line in enumerate(batch_lines, start=1):
                        current_line_number = batch_start_line + i - 1
                        print(f"处理第 {current_line_number}/{batch_start_line + len(batch_lines) - 1} 行: {line}")
                        await process_line(session, line, current_line_number, max_images=max_images, zipf=zipf, csv_writer=csv_writer, csvfile=csvfile, existing_filenames=existing_filenames, error_flag=error_flag)
                        if error_flag['value']:
                            print(f"检测到下载异常，停止脚本。出错的行号: {', '.join(map(str, sorted(set(error_flag['lines']))))}")
                            return min(error_flag['lines'])
            
                    batch_start_line += max_lines_per_batch
    return None

if __name__ == "__main__":
    txt_path = "artist_full.txt" # 存放你的标签行的 txt 文件，注意要同一个目录下
    output_zip = "images.zip"
    csv_file = "train.csv"
    timeout = 5000  # 超时时间，毫秒
    proxies = {"http://": 'http://127.0.0.1:7890', "https://": 'http://127.0.0.1:7890'}
    # 全局变量，控制每次处理多少个标签行
    max_lines_per_batch = 5
    # 控制每个标签的最大下载图片数量
    max_images = 50
    # 全局变量，控制txt文件的启始标签行
    start_line = 763

    total_lines_processed = 0
    while True:
        result = asyncio.run(main(txt_path, output_zip, csv_file, timeout, proxies, start_line, max_lines_per_batch, max_images))
        if result is None:
            break
        else:
            start_line = result  # 将 start_line 设置为出错的行号并继续处理

        with open(txt_path, 'r') as file:
            total_lines_in_file = sum(1 for _ in file)
        
        if total_lines_processed >= total_lines_in_file:
            break