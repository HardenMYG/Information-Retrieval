from elasticsearch import Elasticsearch
import json
import os
import datetime

# 初始化 Elasticsearch 客户端
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

# 创建输出目录
output_dir = "D:\\SearchEngine\\webpage_index_reports"
os.makedirs(output_dir, exist_ok=True)

# 生成带时间戳的文件名
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
report_file = os.path.join(output_dir, f"webpage_index_structure_{timestamp}.txt")

# 目标索引名称
index_name = "web_pages"

# 获取索引映射和设置
try:
    index_info = es.indices.get(index=index_name)[index_name]
    
    # 创建报告文件
    with open(report_file, "w", encoding="utf-8") as f:
        # 报告头部信息
        f.write("="*80 + "\n")
        f.write(f"Elasticsearch 网页索引结构报告\n")
        f.write(f"索引名称: {index_name}\n")
        f.write(f"生成时间: {datetime.datetime.now()}\n")
        f.write("="*80 + "\n\n")
        
        # 1. 索引整体结构
        f.write("1. 索引整体结构\n")
        f.write("="*60 + "\n")
        f.write(f"索引状态: {es.indices.stats(index=index_name)['indices'][index_name]['health']}\n")
        f.write(f"文档总数: {es.count(index=index_name)['count']}\n")
        f.write(f"主分片数: {index_info['settings']['index']['number_of_shards']}\n")
        f.write(f"副本分片数: {index_info['settings']['index']['number_of_replicas']}\n")
        f.write("\n")
        
        # 2. 文本索引域详情
        f.write("2. 文本索引域配置\n")
        f.write("="*60 + "\n")
        properties = index_info['mappings']['properties']
        
        # 2.1 URL字段
        url_field = properties['url']
        f.write("URL字段 (keyword类型):\n")
        f.write(f"  - 存储方式: {url_field['type']}\n")
        f.write(f"  - 索引方式: 精确值索引，不分词\n")
        f.write(f"  - 用途: 唯一标识网页，支持精确匹配和聚合\n\n")
        
        # 2.2 标题字段
        title_field = properties['title']
        f.write("标题字段 (text类型):\n")
        f.write(f"  - 存储方式: {title_field['type']}\n")
        if 'analyzer' in title_field:
            f.write(f"  - 分词器: {title_field['analyzer']}\n")
        else:
            f.write(f"  - 分词器: 标准分词器 (standard)\n")
        f.write(f"  - 索引方式: 全文索引，支持模糊搜索和相关性排序\n\n")
        
        # 2.3 内容字段
        content_field = properties['content']
        f.write("内容字段 (text类型):\n")
        f.write(f"  - 存储方式: {content_field['type']}\n")
        if 'analyzer' in content_field:
            f.write(f"  - 分词器: {content_field['analyzer']}\n")
        else:
            f.write(f"  - 分词器: 标准分词器 (standard)\n")
        f.write(f"  - 索引方式: 全文索引，支持复杂查询和相关性计算\n\n")
        
        # 2.4 锚文本字段
        anchors_field = properties['anchors']
        f.write("锚文本字段 (nested类型):\n")
        f.write(f"  - 存储方式: {anchors_field['type']}\n")
        f.write(f"  - 结构: 嵌套对象，包含以下子字段:\n")
        
        # 锚文本子字段
        anchor_props = anchors_field['properties']
        f.write(f"    * 锚文本 (anchor_text):\n")
        f.write(f"        - 类型: {anchor_props['anchor_text']['type']}\n")
        if 'analyzer' in anchor_props['anchor_text']:
            f.write(f"        - 分词器: {anchor_props['anchor_text']['analyzer']}\n")
        else:
            f.write(f"        - 分词器: 标准分词器 (standard)\n")
        
        f.write(f"    * 目标URL (target_url):\n")
        f.write(f"        - 类型: {anchor_props['target_url']['type']}\n")
        f.write(f"        - 索引方式: 精确值索引，不分词\n")
        f.write("\n")
        
        # 3. 文档示例展示
        f.write("3. 网页文档索引结构示例\n")
        f.write("="*60 + "\n")
        f.write("以下是索引中5个网页文档的实际存储结构示例:\n\n")
        
        # 获取5个文档样本
        search_results = es.search(
            index=index_name,
            body={"query": {"match_all": {}}, "size": 5}
        )
        
        for i, hit in enumerate(search_results['hits']['hits']):
            doc = hit['_source']
            f.write(f"文档 #{i+1}:\n")
            f.write(f"  - ID: {hit['_id']}\n")
            f.write(f"  - URL: {doc['url']}\n")
            f.write(f"  - 标题: {doc['title'][:50]}...\n")
            f.write(f"  - 内容摘要: {doc['content'][:100]}...\n")
            
            # 锚文本处理
            f.write("  - 锚文本:\n")
            for j, anchor in enumerate(doc['anchors'][:3]):  # 只显示前3个锚文本
                f.write(f"      {j+1}. '{anchor['anchor_text'][:30]}' -> {anchor['target_url']}\n")
            
            f.write("\n")
        
        
        # 报告尾部
        f.write("="*80 + "\n")
        f.write("报告结束\n")
        f.write("="*80 + "\n")
    
    print(f"网页索引结构报告已保存至: {report_file}")
    
except Exception as e:
    print(f"生成索引报告时出错: {str(e)}")
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(f"错误: 无法生成索引报告\n{str(e)}")