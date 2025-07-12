# 搜索模块基本在这里实现

from elasticsearch import Elasticsearch

# Initialize Elasticsearch client
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

index_name = "web_pages"


def is_url(query):
    """判断输入是否为 URL（简单地通过检查是否以 http:// 或 https:// 开头）。"""
    return query.startswith("http://") or query.startswith("https://")


def search_url(query):
    """使用 'term' 查询进行 URL 精确匹配搜索。"""
    print("查询 URL 结果如下：")
    response = es.search(
        index=index_name,
        body={
            "query": {"term": {"url": query}},
            "size": 1000,
        },
    )
    return response


def search_exact(query, identity, college):
    """使用 'term' 查询进行精确匹配搜索，并将 identity 和 college 添加为加权因子"""
    response = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [
                        {"term": {"title": query}},
                        {"term": {"content": query}},
                        {"term": {"anchors.anchor_text": query}},
                    ],
                    "should": [
                        {
                            "multi_match": {
                                "query": identity,
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 0.5,
                            }
                        },
                        {
                            "multi_match": {
                                "query": college,
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 0.5,
                            }
                        }
                    ],
                    "minimum_should_match": 0,
                }
            },
            "size": 1000,
        },
    )
    return response


def search_phrase(query, identity, college):
    """使用 'multi_match' 查询，并将 identity 和 college 添加为加权因子"""
    response = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [  # 必须匹配 string1
                        {
                            "multi_match": {
                                "query": query,  # 字符串1
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 5.0,  # 字符串1的权重
                            }
                        }
                    ],
                    "should": [  # 可选匹配 string2，提高分数
                        {
                            "multi_match": {
                                "query": identity,  # 字符串2
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 0.5,  # 字符串2的权重
                            }
                        },
                        {
                            "multi_match": {
                                "query": college,  # 字符串3
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 0.5,  # 字符串3的权重
                            }
                        }
                    ],
                    "minimum_should_match": 0, 
                }
            },
            "size": 1000,
        },
    )

    return response



def search_wildcard(query_text, identity, college):
    """使用 'wildcard' 查询进行通配符匹配，并将 identity 和 college 添加为加权因子"""
    response = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "wildcard": {
                                "title": {
                                    "value": query_text,
                                    "boost": 5.0,  
                                }
                            }
                        }
                    ],
                    "should": [
                        {
                            "multi_match": {
                                "query": identity,
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 0.5,
                            }
                        },
                        {
                            "multi_match": {
                                "query": college,
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 0.5,
                            }
                        }
                    ],
                    "minimum_should_match": 0,
                },
            },
            "size": 1000,
        },
    )
    return response



def merge_results(results_list):
    """合并多个查询结果并按得分排序，同时进行去重"""
    unique_results = {}  
    for result in results_list:
        for hit in result["hits"]["hits"]:
            # 获取文档的 URL
            doc_url = hit["_source"]["url"]
            # 如果文档的 URL 不在 unique_results 中，或者当前得分更高，则更新
            if (
                doc_url not in unique_results
                or hit["_score"] > unique_results[doc_url]["_score"]
            ):
                unique_results[doc_url] = hit
    # 将去重后的文档按得分排序
    sorted_results = sorted(
        unique_results.values(), key=lambda x: x["_score"], reverse=True
    )
    return sorted_results


#附件搜索功能
import csv
from urllib.parse import unquote

# 加载附件元数据
ATTACHMENTS = []  # 存储附件信息: {'source_url': ..., 'attachment_url': ..., 'filename': ...}

def load_attachments(csv_path='D:\\SearchEngine\\filepages.csv'):
    """加载附件元数据"""
    try:
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                source_url = row['Source_URL']
                attachment_url = row['Attachment_URL']
                # 从attachment_url中提取文件名
                filename = unquote(attachment_url.split('/')[-1])  # 解码URL编码
                ATTACHMENTS.append({
                    'source_url': source_url,
                    'attachment_url': attachment_url,
                    'filename': filename
                })
        print(f"Loaded {len(ATTACHMENTS)} attachments metadata")
    except Exception as e:
        print(f"Error loading attachments: {str(e)}")

# 在模块初始化时加载附件数据
load_attachments()

def search_attachments(query, identity, college):
    """搜索附件元数据"""
    keywords = query.split()
    results = []
    
    for att in ATTACHMENTS:
        # 计算匹配度：查询词出现在文件名中的次数
        match_count = sum(1 for keyword in keywords if keyword in att['filename'])
        
        # 添加个性化权重
        weight = match_count
        if identity and identity in att['filename']:
            weight += 0.5
        if college and college in att['filename']:
            weight += 0.5
            
        if weight > 0:
            results.append({
                'url': att['attachment_url'],
                'title': f"[附件] {att['filename']}",
                'weight': weight
            })
    
    # 按权重排序
    results.sort(key=lambda x: x['weight'], reverse=True)
    return results

def all_search(query, identity, college):
    # 获取网页结果（需要返回内容摘要）
    webpage_results = search_and_rank(query, identity, college)  # 修改函数获取内容摘要
    
    combined_results = []
    # 处理网页结果（现在包含真实摘要）
    for url, title, content_snippet in webpage_results:
        # 如果没有内容摘要则使用标题
        snippet = content_snippet if content_snippet else title[:200] + "..."
        combined_results.append((url, title, snippet))
    
    # 附件结果处理（保持原逻辑）
    attachment_results = search_attachments(query, identity, college)
    for result in attachment_results:
        combined_results.append((
            result['url'],
            result['title'],
            "这是一个附件"  # 附件标识
        ))
    
    return combined_results


def search_and_rank(query, identity=None, college=None):
    """处理查询并按 Elasticsearch 得分排序的主搜索函数，返回(url, title, snippet)三元组"""
    print(f"Original query: {query}")
    
    # 如果是URL查询
    if is_url(query):
        url_response = search_url(query)
        if url_response["hits"]["hits"]:
            source = url_response["hits"]["hits"][0]["_source"]
            url = source.get("url", query)
            title = source.get("title", "无标题")
            content = source.get("content", title)  # 如果没有内容，使用标题
            snippet = generate_snippet(content, query)  # 从内容生成摘要
            return [(url, title, snippet)]
        else:
            return []
    
    # 分割查询词
    query_parts = query.split(" ")
    
    # 执行精确查询
    exact_results = []
    for part in query_parts:
        if "*" not in part and "?" not in part:
            exact_response = search_exact(part, identity, college)
            if exact_response["hits"]["hits"]:
                exact_results.append(exact_response)
    
    # 如果精确查询有结果，直接返回
    if exact_results:
        merged_exact_results = merge_results(exact_results)
        return [extract_result(hit) for hit in merged_exact_results]
    
    # 执行多种查询
    results_list = []
    for part in query_parts:
        if "*" in part or "?" in part:
            wildcard_response = search_wildcard(part, identity, college)
            if wildcard_response["hits"]["hits"]:
                results_list.append(wildcard_response)
        else:
            phrase_response = search_phrase(part, identity, college)
            if phrase_response["hits"]["hits"]:
                results_list.append(phrase_response)
    
    # 合并结果并按 ES 得分排序
    merged_results = merge_results(results_list) if results_list else []
    
    # 提取结果生成三元组
    return [extract_result(hit) for hit in merged_results]


def extract_result(hit):
    """从ES结果中提取URL、标题和摘要"""
    source = hit["_source"]
    url = source.get("url", "")
    title = source.get("title", "无标题")
    content = source.get("content", "") or title  # 如果没有内容，使用标题
    
    # 生成内容摘要（优先从内容中提取）
    snippet = generate_snippet(content, title)
    
    return (url, title, snippet)


def generate_snippet(content, query=""):
    """生成内容摘要，保留查询关键词上下文"""

    if len(content) > 400:

        end_pos = content.find(" ", 380, 420)
        end_pos = end_pos if end_pos > 0 else 400
        snippet = content[:end_pos] + "..."
    else:
        snippet = content
    return snippet

