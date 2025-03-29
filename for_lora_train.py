import httpx
import asyncio
import aiofiles
import os
from pathlib import Path
from PIL import Image
import json

async def download_image(session, item, filename, save_dir, error_flag, line_number):
    max_retries = 10
    retries = 0
    
    # 获取原始图片URL
    media_asset = item.get('media_asset', {})
    variants = media_asset.get('variants', [])
    original_variant = next((v for v in variants if v['type'] == 'original'), None)
    url = original_variant['url'] if original_variant else None
    
    if not url:
        print(f"未找到原始图片URL: {item}")
        return False

    while retries < max_retries:
        try:
            response = await session.get(url)
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'video' in content_type:
                    print(f"跳过视频文件: {url}")
                    return False
                
                # 下载图片
                async with aiofiles.open(filename, 'wb') as f:
                    await f.write(response.content)
                
                # 处理WebP格式
                if filename.suffix.lower() == '.webp':
                    new_filename = filename.with_suffix('.jpg')
                    with Image.open(filename) as img:
                        img.save(new_filename, 'JPEG')
                    os.remove(filename)
                    filename = new_filename
                    print(f"转换完成: {filename}")
                
                print(f"下载完成: {filename}")
                
                # 处理tag_string并保存到txt文件
                tag_string = item.get('tag_string', '')
                processed_tags = tag_string.replace(' ', ',').replace('_', ' ')
                txt_filename = filename.with_suffix('.txt')
                async with aiofiles.open(txt_filename, 'w', encoding='utf-8') as txt_file:
                    await txt_file.write(processed_tags)
                print(f"写入TXT: {txt_filename}")
                
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

async def process_line(session, line, line_number, base_save_dir, max_images=5, existing_filenames=None, error_flag=None):
    # 为每行创建一个子文件夹，使用清理后的行内容作为文件夹名
    folder_name = line.strip().replace(' ', '_').replace('/', '_').replace('\\', '_')  # 避免非法字符
    save_dir = base_save_dir / folder_name
    save_dir.mkdir(parents=True, exist_ok=True)
    
    processed_tags = line.replace(' ', '_')
    page = 1
    processed_count = 0
    
    while processed_count < max_images:
        url = f"https://kagamihara.donmai.us/posts.json?page={page}&tags={processed_tags}"
        try:
            response = await session.get(url)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    break
                for item in data:
                    image_name = os.path.basename(item.get('file_url', ''))
                    
                    comparison_filename = image_name
                    if comparison_filename.endswith('.webp'):
                        comparison_filename = comparison_filename[:-5] + '.jpg'
                    
                    unique_filename = save_dir / comparison_filename
                    
                    if comparison_filename in existing_filenames:
                        print(f"文件已存在: {comparison_filename}, 跳过下载")
                        processed_count += 1
                        if processed_count >= max_images:
                            return
                    else:
                        success = await download_image(session, item, unique_filename, save_dir, error_flag, line_number)
                        if success:
                            processed_count += 1
                            existing_filenames.add(comparison_filename)
                            if processed_count >= max_images:
                                return
                        elif error_flag['value']:
                            return
            else:
                print(f"请求失败 (状态码 {response.status_code}): {url}")
                error_flag['value'] = True
                error_flag['lines'].append(line_number)
                return
        except Exception as e:
            print(f"请求异常: {e} - {url}")
            error_flag['value'] = True
            error_flag['lines'].append(line_number)
            return
        page += 1

async def read_existing_filenames(base_save_dir):
    existing_filenames = set()
    for sub_dir in base_save_dir.glob('*'):
        if sub_dir.is_dir():
            for file in sub_dir.glob('*.jpg'):
                existing_filenames.add(file.name)
    return existing_filenames

async def main(txt_path, save_dir="downloaded_images", timeout=1000, proxies=None, start_line=1, max_lines_per_batch=5, max_images=5):
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
        
        base_save_dir = Path(save_dir)
        base_save_dir.mkdir(parents=True, exist_ok=True)
        print(f"创建/检查基础保存目录: {base_save_dir}")

        existing_filenames = await read_existing_filenames(base_save_dir)
        
        error_flag = {'value': False, 'lines': []}
        batch_start_line = start_line
        while lines:
            batch_lines = lines[:max_lines_per_batch]
            lines = lines[max_lines_per_batch:]
            
            for i, line in enumerate(batch_lines, start=1):
                current_line_number = batch_start_line + i - 1
                print(f"处理第 {current_line_number}/{batch_start_line + len(batch_lines) - 1} 行: {line}")
                await process_line(session, line, current_line_number, base_save_dir, max_images=max_images, existing_filenames=existing_filenames, error_flag=error_flag)
                if error_flag['value']:
                    print(f"检测到下载异常，停止脚本。出错的行号: {', '.join(map(str, sorted(set(error_flag['lines']))))}")
                    return min(error_flag['lines'])
        
            batch_start_line += max_lines_per_batch
    return None

if __name__ == "__main__":
    txt_path = "artist_full.txt"
    save_dir = "downloaded_images"
    timeout = 5000
    proxies = {"http://": 'http://127.0.0.1:7890', "https://": 'http://127.0.0.1:7890'}
    max_lines_per_batch = 5
    max_images = 50
    start_line = 1

    total_lines_processed = 0
    while True:
        result = asyncio.run(main(txt_path, save_dir, timeout, proxies, start_line, max_lines_per_batch, max_images))
        if result is None:
            break
        else:
            start_line = result

        with open(txt_path, 'r') as file:
            total_lines_in_file = sum(1 for _ in file)
        
        if total_lines_processed >= total_lines_in_file:
            break
