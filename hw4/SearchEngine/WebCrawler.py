import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import os
import csv
import uuid
from datetime import datetime
import re
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
import logging

class WebCrawler:
    def __init__(self, start_url, max_pages, save_dir, max_workers=5):
        self.start_url = start_url
        self.max_pages = max_pages
        self.visited_urls = set()
        self.pending_urls_set = set()
        self.to_visit_queue = Queue()
        self.to_visit_queue.put(start_url)
        self.pending_urls_set.add(start_url)
        self.save_dir = os.path.normpath(save_dir)
        self.session = requests.Session()
        self.crawl_delay = 0.5
        self.crawled_count = 0
        self.max_workers = max_workers
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        
        # 创建存储目录
        os.makedirs(self.save_dir, exist_ok=True)
        
        # CSV文件路径
        self.csv_file = os.path.join(os.path.dirname(self.save_dir), "webpages.csv")
        
        # 初始化CSV文件
        self.init_csv()
        
        # 配置日志
        self.configure_logging()
        
        self.logger.info(f"爬虫初始化完成 - 起始URL: {start_url}")

    def configure_logging(self):
        """配置日志记录"""
        log_file = os.path.join(os.path.dirname(self.save_dir), "crawler.log")
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
        )
        self.logger = logging.getLogger("WebCrawler")

    def init_csv(self):
        """初始化CSV文件"""
        file_exists = os.path.exists(self.csv_file)
        
        with open(self.csv_file, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["URL", "Filename", "CrawlTime"])
    
    def crawl(self):
        """主爬取方法 - 使用多线程"""
        self.logger.info(f"开始多线程爬取，目标URL: {self.start_url}")
        self.logger.info(f"最大页面数: {self.max_pages}")
        self.logger.info(f"存储目录: {self.save_dir}")
        self.logger.info(f"工作线程数: {self.max_workers}")
        
        start_time = datetime.now()
        print(f"开始爬取 - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 使用线程池执行爬取任务
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交初始任务
                future_tasks = []
                for _ in range(self.max_workers):
                    future = executor.submit(self.worker)
                    future_tasks.append(future)
                
                # 监控爬取进度
                while not self.stop_event.is_set():
                    print(f"\r已爬取: {self.crawled_count} | 待爬取: {self.to_visit_queue.qsize()}", end="")
                    
                    # 检查是否达到终止条件
                    if self.crawled_count >= self.max_pages or (self.to_visit_queue.empty() and self.crawled_count > 0):
                        self.stop_event.set()
                        break
                    
                    time.sleep(1)
                
                # 等待所有任务完成
                for future in future_tasks:
                    future.result()  # 获取结果，处理异常
        
        except KeyboardInterrupt:
            print("\n用户中断爬取，正在停止...")
            self.stop_event.set()
        
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        
        self.logger.info(f"爬取完成! 总共爬取页面: {self.crawled_count}")
        self.logger.info(f"耗时: {elapsed_time:.2f}秒")
        self.logger.info(f"平均速度: {self.crawled_count/elapsed_time:.2f}页/秒")
        
        print(f"\n爬取完成! 总共爬取页面: {self.crawled_count}")
        print(f"耗时: {elapsed_time:.2f}秒")
        print(f"平均速度: {self.crawled_count/elapsed_time:.2f}页/秒")
        print(f"CSV文件位置: {self.csv_file}")

    def worker(self):
        """工作线程函数，处理单个URL"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
        
        while not self.stop_event.is_set():
            try:
                # 获取URL（设置超时避免永久阻塞）
                current_url = self.to_visit_queue.get(timeout=5)
                
                # 检查是否已访问
                with self.lock:
                    if current_url in self.visited_urls:
                        self.to_visit_queue.task_done()
                        continue
                
                # 域名检查
                if not self.is_valid_domain(current_url):
                    self.logger.info(f"跳过非目标域名URL: {current_url}")
                    self.to_visit_queue.task_done()
                    continue
                
                self.logger.info(f"处理URL: {current_url}")
                
                try:
                    # 添加请求延时
                    time.sleep(self.crawl_delay)
                    
                    response = self.session.get(
                        current_url, 
                        headers=headers, 
                        timeout=(3, 8),
                        allow_redirects=True
                    )
                    
                    if response.status_code == 200:
                        # 检测内容类型
                        content_type = response.headers.get('Content-Type', '')
                        if 'text/html' not in content_type:
                            self.logger.info(f"跳过非HTML内容: {content_type} - {current_url}")
                            self.to_visit_queue.task_done()
                            continue
                        
                        # 处理编码
                        encoding = response.encoding
                        if not encoding or encoding.lower() == 'iso-8859-1':
                            encoding = 'utf-8'
                        
                        try:
                            html_content = response.content.decode(encoding, errors="replace")
                        except (UnicodeDecodeError, LookupError):
                            html_content = response.content.decode('utf-8', errors="replace")
                        
                        # 添加到已访问集合
                        with self.lock:
                            if current_url not in self.visited_urls:
                                self.visited_urls.add(current_url)
                                self.crawled_count += 1
                        
                        # 保存页面
                        self.save_page(html_content, current_url)
                        
                        # 提取链接（移除总URL数检查）
                        new_links_count = self.extract_links(html_content, current_url)
                        self.logger.info(f"从 {current_url} 发现 {new_links_count} 个新链接")
                        if new_links_count == 0:
                            self.logger.warning(f"在 {current_url} 上未找到有效链接，可能是解析问题")
                    
                    else:
                        self.logger.warning(f"HTTP错误 {response.status_code}: {current_url}")
                    
                except requests.exceptions.Timeout:
                    self.logger.warning(f"请求超时: {current_url}")
                except requests.exceptions.TooManyRedirects:
                    self.logger.warning(f"重定向过多: {current_url}")
                except requests.exceptions.RequestException as e:
                    self.logger.error(f"请求异常: {current_url} - {str(e)}")
                except Exception as e:
                    self.logger.error(f"处理 {current_url} 时出错: {str(e)}")
                
                finally:
                    # 标记任务完成
                    self.to_visit_queue.task_done()
            
            except Empty:  # 正确捕获Empty异常
                # 队列为空，检查是否应该退出
                if self.stop_event.is_set() or self.to_visit_queue.empty():
                    break
                else:
                    time.sleep(1)  # 等待新的URL
            except Exception as e:
                self.logger.error(f"工作线程异常: {str(e)}")

    def is_valid_domain(self, url):
        """检查URL是否在目标域名内"""
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower()
            
            # 允许所有南开大学的子域名
            return domain.endswith("nankai.edu.cn") or domain == "nankai.edu.cn"
        except Exception:
            return False

    def extract_links(self, html, base_url):
        """从HTML中提取有效链接"""
        new_links_count = 0
        try:
            soup = BeautifulSoup(html, "lxml")
            base_parsed = urlparse(base_url)
            
            self.logger.debug(f"开始从 {base_url} 提取链接")
            
            for link in soup.find_all("a", href=True):
                # 检查是否已达到最大页面数
                if self.crawled_count >= self.max_pages or self.stop_event.is_set():
                    self.logger.info(f"已达到最大页面数 {self.max_pages}，停止提取链接")
                    break
                
                href = link.get("href", "").strip()
                
                # 过滤无效链接
                if not href or href.startswith(('javascript:', 'mailto:', 'tel:', '#', 'data:')):
                    continue
                
                # 处理相对路径
                try:
                    absolute_url = urljoin(base_url, href)
                    parsed_url = urlparse(absolute_url)
                    
                    # 规范化URL
                    normalized_url = parsed_url._replace(fragment="").geturl()
                    
                    # 域名检查
                    if not self.is_valid_domain(normalized_url):
                        continue
                    
                    # 线程安全的添加新URL
                    with self.lock:
                        if (normalized_url not in self.visited_urls and 
                            normalized_url not in self.pending_urls_set):
                            self.to_visit_queue.put(normalized_url)
                            self.pending_urls_set.add(normalized_url)
                            new_links_count += 1
                            self.logger.debug(f"发现新链接: {normalized_url}")
                        
                except ValueError:
                    continue
                    
            self.logger.info(f"成功从 {base_url} 提取 {new_links_count} 个链接")
            return new_links_count
            
        except Exception as e:
            self.logger.error(f"提取链接时出错: {str(e)}")
            return 0
    
    def generate_filename(self, url):
        """生成安全的文件名"""
        # 使用URL中的路径部分作为文件名基础
        parsed = urlparse(url)
        path = parsed.path.strip('/').replace('/', '_') or "index"
        
        # 限制文件名长度
        if len(path) > 50:
            path = path[:50]
        
        # 添加唯一标识符
        unique_id = uuid.uuid4().hex[:6]
        return f"{path}_{unique_id}.html"

    def save_page(self, html, url):
        """保存HTML页面并记录到CSV"""
        try:
            # 生成唯一文件名
            filename = self.generate_filename(url)
            filepath = os.path.join(self.save_dir, filename)
            
            # 写入文件
            with open(filepath, "w", encoding="utf-8") as file:
                file.write(html)
            
            # 记录到CSV
            with self.lock:
                self.write_to_csv(url, filepath)
            
            self.logger.info(f"已保存: {filename}")
            
        except OSError as e:
            self.logger.error(f"文件保存错误: {str(e)}")
        except Exception as e:
            self.logger.error(f"保存页面时出错: {str(e)}")

    def write_to_csv(self, url, filepath):
        """将记录写入CSV文件"""
        try:
            with open(self.csv_file, mode="a", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow([url, filepath, datetime.now().isoformat()])
        except Exception as e:
            self.logger.error(f"写入CSV时出错: {str(e)}")


if __name__ == "__main__":
    # 使用原始字符串避免转义问题
    start_url = "https://www.nankai.edu.cn/"
    max_pages = 101000  
    save_dir = "d:/SearchEngine/PagesData"  # 使用原始字符串
    max_workers = 10  # 设置工作线程数
    
    # 添加版权声明
    print("=" * 70)
    print("南开大学网站爬虫 - 仅用于学术研究")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"工作线程数: {max_workers}")
    print("=" * 70)
    
    crawler = WebCrawler(start_url, max_pages, save_dir, max_workers)
    
    try:
        crawler.crawl()
    except KeyboardInterrupt:
        print("\n用户中断爬取，正在保存进度...")
    finally:
        print(f"已爬取页面数: {crawler.crawled_count}")
        print(f"待爬取URL数: {crawler.to_visit_queue.qsize()}")
        print(f"CSV文件位置: {crawler.csv_file}")
        print("爬取结束!")