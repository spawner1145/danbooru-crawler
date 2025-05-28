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
    
    url = item.get('file_url')
    
    if not url:
        print(f"未找到图片URL: {item}")
        return False

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
                    try:
                        with Image.open(filename) as img:
                            if img.mode in ('RGBA', 'P'):
                                img = img.convert('RGB')
                            img.save(new_filename, 'JPEG')
                        os.remove(filename)
                        filename = new_filename
                        print(f"转换完成: {filename}")
                    except Exception as e:
                        print(f"WebP转换失败: {e}, 文件: {filename}")
                        os.remove(filename)
                        return False

                print(f"下载完成: {filename}")
                
                tag_string = item.get('tags', '')
                processed_tags = tag_string.replace(' ', ',')
                
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
            await asyncio.sleep(2)
    
    print(f"放弃下载: {url}")
    error_flag['value'] = True
    error_flag['lines'].append(line_number)
    return False

async def process_line(session, line, line_number, base_save_dir, max_images=5, existing_filenames=None, error_flag=None):
    folder_name = line.strip().replace(' ', '_').replace('/', '_').replace('\\', '_')
    save_dir = base_save_dir / folder_name
    save_dir.mkdir(parents=True, exist_ok=True)
    
    processed_tags = line.strip().replace(' ', '_')
    page = 1
    processed_count = 0
    
    while processed_count < max_images:
        url = f"https://yande.re/post.json?page={page}&tags={processed_tags}"
        try:
            response = await session.get(url)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    print(f"Tag '{processed_tags}' 已无更多图片。")
                    break
                
                for item in data:
                    image_name = os.path.basename(item.get('file_url', ''))
                    if not image_name:
                        continue

                    comparison_filename = image_name
                    if comparison_filename.lower().endswith('.webp'):
                        comparison_filename = comparison_filename[:-5] + '.jpg'
                    
                    if comparison_filename in existing_filenames:
                        print(f"文件已存在: {comparison_filename}, 跳过下载")
                        processed_count += 1 
                        if processed_count >= max_images:
                            return
                        continue

                    original_filename_path = save_dir / image_name
                    
                    success = await download_image(session, item, original_filename_path, save_dir, error_flag, line_number)
                    if success:
                        processed_count += 1
                        existing_filenames.add(comparison_filename)
                        if processed_count >= max_images:
                            return
                    elif error_flag['value']:
                        print(f"因下载错误，终止处理Tag: '{line.strip()}'")
                        return

            else:
                print(f"请求失败 (状态码 {response.status_code}): {url}")
                error_flag['value'] = True
                error_flag['lines'].append(line_number)
                return
        except json.JSONDecodeError:
            print(f"JSON解析失败，可能是因为API返回了非JSON内容（如HTML错误页）: {url}")
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
    print("正在扫描已存在的文件...")
    for sub_dir in base_save_dir.glob('*'):
        if sub_dir.is_dir():
            for file in sub_dir.glob('*.jpg'):
                existing_filenames.add(file.name)
            for file in sub_dir.glob('*.png'):
                existing_filenames.add(file.name)
    print(f"扫描完成，找到 {len(existing_filenames)} 个已存在文件。")
    return existing_filenames

async def main(txt_path, save_dir="downloaded_images", timeout=1000, proxies=None, start_line=1, max_lines_per_batch=5, max_images=5):
    print("开始执行脚本...")
    print(f"当前工作目录: {os.getcwd()}")
    print(f"尝试打开文件: {txt_path}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout), proxies=proxies, headers=headers) as session:
        lines = []
        try:
            async with aiofiles.open(txt_path, mode='r', encoding='utf-8') as file:
                all_lines = await file.readlines()
                lines = [line.strip() for line in all_lines[start_line - 1:] if line.strip()]

            print(f"成功读取文件: {txt_path}, 从第 {start_line} 行开始，共 {len(lines)} 行待处理。")
        except FileNotFoundError:
            print(f"错误: 文件 {txt_path} 不存在.")
            return None
        
        base_save_dir = Path(save_dir)
        base_save_dir.mkdir(parents=True, exist_ok=True)
        print(f"创建/检查基础保存目录: {base_save_dir}")

        existing_filenames = await read_existing_filenames(base_save_dir)
        
        error_flag = {'value': False, 'lines': []}
        tasks_with_linenumbers = [(line, start_line + i) for i, line in enumerate(lines)]

        for i in range(0, len(tasks_with_linenumbers), max_lines_per_batch):
            batch = tasks_with_linenumbers[i:i + max_lines_per_batch]
            print(f"开始处理批次 {i // max_lines_per_batch + 1}")
            
            tasks = [
                process_line(session, line, line_num, base_save_dir, max_images, existing_filenames, error_flag)
                for line, line_num in batch
            ]
            
            await asyncio.gather(*tasks)

            if error_flag['value']:
                error_lines = sorted(set(error_flag['lines']))
                print(f"\n检测到下载异常，停止脚本。出错的行号: {', '.join(map(str, error_lines))}")
                return min(error_lines)
        
    return None

if __name__ == "__main__":
    txt_path = "artist_full.txt"  # 所有你需要爬的标签txt，每行一个tag，不同tag会保存到不同的文件夹里
    save_dir = "./yande" # 修改保存目录以区分
    timeout = 30                 # 超时时间（秒），不建议设置过高
    # 如果不需要代理，请设置为 None
    proxies = {"http://": 'http://127.0.0.1:7890', "https://": 'http://127.0.0.1:7890'}
    max_lines_per_batch = 5      # 同时处理的tag数量（并发数）
    max_images = 1500            # 每个tag最多爬的图片数
    start_line = 1               # 从txt文件的第几行开始爬取

    while True:
        try:
            result = asyncio.run(main(txt_path, save_dir, timeout, proxies, start_line, max_lines_per_batch, max_images))
            
            if result is None:
                print("\n所有任务已成功完成！")
                break
            else:
                print(f"\n脚本将在发生错误的行 {result} 处重新启动。")
                start_line = result

        except KeyboardInterrupt:
            print("\n用户手动中断脚本。")
            break
        except Exception as e:
            print(f"发生未捕获的严重错误: {e}")
            break
