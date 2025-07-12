from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from Search import all_search
import os
import json
from collections import defaultdict
import itertools
import csv


app = Flask(__name__)
app.secret_key = 'your_secret_key'

# 预定义用户信息，添加身份和学院信息
users = {
    "G": {
        "password": "G",
        "identity": "学生",
        "college": "金融学院"
    },
    "MY": {
        "password": "MY",
        "identity": "老师",
        "college": "化学学院"
    },
}

# 存储查询历史的文件路径
QUERY_LOG_FILE = 'query_logs.json'

# 加载查询历史
def load_query_logs():
    if os.path.exists(QUERY_LOG_FILE):
        with open(QUERY_LOG_FILE, 'r', encoding='utf-8') as file:
            return json.load(file)
    return {user: [] for user in users.keys()}

# 保存查询历史
def save_query_logs(query_logs):
    with open(QUERY_LOG_FILE, 'w', encoding='utf-8') as file:
        json.dump(query_logs, file, ensure_ascii=False, indent=4)

# 加载查询日志
query_logs = load_query_logs()

# 标记应用是否首次启动
is_first_start = True

@app.before_request
def clear_session_on_start():
    global is_first_start
    if is_first_start:
        session.clear()
        is_first_start = False

import math
from collections import defaultdict

class CooccurrenceAnalyzer:
    def __init__(self, query_logs, max_distance=5):
        self.cooccurrence = defaultdict(lambda: defaultdict(float))
        self.max_distance = max_distance  # 最大考虑距离
        self.build_weighted_matrix(query_logs)
    
    def distance_decay(self, distance):
        """距离衰减函数：距离越近权重越高"""
        # 指数衰减：距离1权重1.0，距离2权重0.5，距离3权重0.25...
        return math.exp(-distance)
    
    def build_weighted_matrix(self, query_logs):
        """构建带距离权重的共现矩阵"""
        for user, logs in query_logs.items():
            queries = [log[0].lower() for log in logs]
            
            # 遍历查询序列中的每个位置
            for i, current_query in enumerate(queries):
                # 仅检查后续查询
                for j in range(i+1, min(i+1+self.max_distance, len(queries))):
                    related_query = queries[j]
                    
                    if current_query != related_query:
                        distance = j - i  # 计算距离
                        weight = self.distance_decay(distance)
                        
                        # 更新共现权重（累积加权值）
                        self.cooccurrence[current_query][related_query] += weight
                        self.cooccurrence[related_query][current_query] += weight
    
    def update_with_new_query(self, username, new_query, query_logs):
        """更新共现矩阵（考虑距离权重）"""
        new_query = new_query.lower()
        user_queries = [log[0].lower() for log in query_logs.get(username, [])]
        
        # 新查询作为当前查询
        current_query = new_query
        for i, related_query in enumerate(user_queries):
            distance = len(user_queries) - i  # 新查询在序列末尾
            weight = self.distance_decay(distance)
            
            self.cooccurrence[current_query][related_query] += weight
            self.cooccurrence[related_query][current_query] += weight
    
    def get_suggestions(self, query, top_n=5):
        """获取考虑距离权重的建议"""
        query = query.lower()
        if query not in self.cooccurrence:
            return []
        
        # 获取并排序建议
        suggestions = sorted(
            self.cooccurrence[query].items(),
            key=lambda x: x[1],  # 按加权值排序
            reverse=True
        )[:top_n]
        
        # 返回查询词和关联强度
        return [(suggestion[0], suggestion[1]) for suggestion in suggestions]
# 在应用初始化后创建分析器
cooccurrence_analyzer = CooccurrenceAnalyzer(query_logs)

@app.route('/register', methods=['GET', 'POST'])
def register():
    return "注册功能暂不开放，使用预定义用户登录。"

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        if username in users and users[username]["password"] == password:
            session['username'] = username
            session['identity'] = users[username]["identity"]
            session['college'] = users[username]["college"]
            return redirect(url_for('search'))
        else:
            return '用户名或密码错误，请重试。'
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('username', None)
    session.pop('identity', None)
    session.pop('college', None)
    return redirect(url_for('login'))

@app.route('/', methods=['GET'])
def index():
    """首页路由，未登录用户重定向到登录页面"""
    if 'username' not in session:
        return redirect(url_for('login'))
    return redirect(url_for('search'))

@app.route('/search', methods=['GET', 'POST'])
def search():
    """搜索页面路由，仅允许已登录用户访问"""
    if 'username' not in session:
        return redirect(url_for('login'))
    
    username = session['username']
    user_query_logs = query_logs.get(username, [])
    identity = session['identity']
    college = session['college']
    
    if request.method == 'POST':
        query = request.form.get('query')
        results = all_search(query, identity, college)

        # 记录查询日志
        new_log = (query, identity, college)
        user_query_logs.append(new_log)
        query_logs[username] = user_query_logs
        save_query_logs(query_logs)

        # 更新共现矩阵
        cooccurrence_analyzer.update_with_new_query(username, query, query_logs)

        # 简单的个性化排序示例：对包含用户身份、所在学院和查询历史关键词的结果给予更高的权重
        query_history = [q for q, _, _ in user_query_logs]
        personalized_results = []
        for url, title, snippet in results:  
            score = 1
            for keyword in query_history:
                if keyword in title or keyword in url:
                    score += 1
            if identity in title or identity in url:
                score += 2
            if college in title or college in url:
                score += 3
            personalized_results.append((url, title, snippet, score))

        # 按得分排序
        personalized_results.sort(key=lambda x: x[3], reverse=True)
        personalized_results = [(url, title, snippet) for url, title, snippet, _ in personalized_results]

        return render_template('search_results.html', query=query, results=personalized_results)
    return render_template('search_form.html', query_logs=user_query_logs)

@app.route('/suggest', methods=['GET'])
def suggest():
    if 'username' not in session:
        return jsonify([])
    
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify([])
    
    # 基于共现分析的全局建议
    suggestions = cooccurrence_analyzer.get_suggestions(query)
    
    # 基于个人历史的个性化建议
    username = session['username']
    user_queries = [log[0] for log in query_logs.get(username, [])]

    all_suggestions = list(set(suggestions + [
        q for q in user_queries 
        if query.lower() in q.lower() and q != query
    ]))[:5]
    
    return jsonify(all_suggestions)

# 处理网页快照请求
@app.route('/snapshot')
def snapshot():
    if 'username' not in session:
        return redirect(url_for('login'))
    url = request.args.get('url')
    # 根据URL查找对应的本地文件
    csv_file = 'D:\\SearchEngine\\webpages.csv'
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['URL'] == url:
                file_path = row['Filename']
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()
                    return html_content
    return "未找到网页快照。"

if __name__ == '__main__':
    app.run(debug=True)