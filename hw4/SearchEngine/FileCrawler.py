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

# 支持的附件格式
ATTACHMENT_EXTENSIONS = ('.pdf', '.doc', '.docx', '.xls', '.xlsx')

class WebCrawler:
    def __init__(self, csv_path, save_dir, max_workers=5):
        self.csv_path = csv_path
        self.visited_urls = set()
        self.pending_urls_set = set()
        self.to_visit_queue = Queue()
        self.save_dir = os.path.normpath(save_dir)
        self.session = requests.Session()
        self.crawl_delay = 0.5
        self.max_workers = max_workers
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        # 创建存储目录
        os.makedirs(self.save_dir, exist_ok=True)

        # filepages.csv文件路径
        self.filepages_csv = os.path.join(self.save_dir, "filepages.csv")

        # 初始化filepages.csv文件
        self.init_filepages_csv()

        # 配置日志
        self.configure_logging()

        # 从webpages.csv中读取URL并添加到待访问队列
        self.load_urls_from_csv()

        self.logger.info(f"爬虫初始化完成 - 从 {csv_path} 加载URL")

    def configure_logging(self):
        """配置日志记录"""
        log_file = os.path.join(self.save_dir, "filecrawler.log")
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
        )
        self.logger = logging.getLogger("WebCrawler")

    def init_filepages_csv(self):
        """初始化filepages.csv文件"""
        file_exists = os.path.exists(self.filepages_csv)

        with open(self.filepages_csv, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["Source_URL", "Attachment_URL"])

    def load_urls_from_csv(self):
        """从webpages.csv中读取URL并添加到待访问队列"""
        try:
            with open(self.csv_path, mode="r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    url = row["URL"]
                    if url not in self.visited_urls and url not in self.pending_urls_set:
                        self.to_visit_queue.put(url)
                        self.pending_urls_set.add(url)
        except Exception as e:
            self.logger.error(f"从 {self.csv_path} 加载URL时出错: {str(e)}")

    def crawl(self):
        """主爬取方法 - 使用多线程"""
        self.logger.info(f"开始多线程爬取，从 {self.csv_path} 加载的URL")
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
                    print(f"\r待爬取: {self.to_visit_queue.qsize()}", end="")

                    # 检查是否达到终止条件
                    if self.to_visit_queue.empty():
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

        self.logger.info(f"爬取完成!")
        self.logger.info(f"耗时: {elapsed_time:.2f}秒")

        print(f"\n爬取完成!")
        print(f"耗时: {elapsed_time:.2f}秒")
        print(f"filepages.csv文件位置: {self.filepages_csv}")

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
                        if 'text/html' in content_type:
                            # 处理HTML页面
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

                            # 提取附件链接
                            self.extract_attachment_links(html_content, current_url)
                        else:
                            self.logger.info(f"跳过非HTML内容: {content_type} - {current_url}")

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

    def extract_attachment_links(self, html, base_url):
        """从HTML中提取附件链接并保存到filepages.csv"""
        try:
            soup = BeautifulSoup(html, "lxml")
            base_parsed = urlparse(base_url)

            self.logger.debug(f"开始从 {base_url} 提取附件链接")

            for link in soup.find_all("a", href=True):
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

                    # 检查是否为附件链接
                    if any(normalized_url.lower().endswith(ext) for ext in ATTACHMENT_EXTENSIONS):
                        # 保存附件链接到filepages.csv
                        with self.lock:
                            self.write_to_filepages_csv(base_url, normalized_url)
                        self.logger.debug(f"发现附件链接: {normalized_url}")

                except ValueError:
                    continue

            self.logger.info(f"成功从 {base_url} 提取附件链接")

        except Exception as e:
            self.logger.error(f"提取附件链接时出错: {str(e)}")

    def write_to_filepages_csv(self, source_url, attachment_url):
        """将附件链接记录写入filepages.csv文件"""
        try:
            with open(self.filepages_csv, mode="a", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow([source_url, attachment_url])
        except Exception as e:
            self.logger.error(f"写入filepages.csv时出错: {str(e)}")


if __name__ == "__main__":
    csv_path = "D:\\SearchEngine\\webpages.csv"
    save_dir = "D:\\SearchEngine"
    max_workers = 10  # 设置工作线程数

    # 添加版权声明
    print("=" * 70)
    print("南开大学网站附件链接爬虫 - 仅用于学术研究")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"工作线程数: {max_workers}")
    print("=" * 70)

    crawler = WebCrawler(csv_path, save_dir, max_workers)

    try:
        crawler.crawl()
    except KeyboardInterrupt:
        print("\n用户中断爬取，正在保存进度...")
    finally:
        print(f"filepages.csv文件位置: {crawler.filepages_csv}")
        print("爬取结束!")