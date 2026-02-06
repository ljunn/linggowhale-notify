import os
import json
import requests
import feedparser
from cozepy import Coze, TokenAuth, WebOAuthApp, COZE_CN_BASE_URL

# --- 环境变量 (GitHub Secrets) ---
CF_API_TOKEN = os.getenv("CF_API_TOKEN")
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
KV_NAMESPACE_ID = os.getenv("KV_NAMESPACE_ID")  # 你的 KV ID
D1_DB_ID = os.getenv("D1_DB_ID")  # 这里的 D1 仅保留用于文章去重

COZE_CLIENT_ID = os.getenv("COZE_CLIENT_ID")
COZE_CLIENT_SECRET = os.getenv("COZE_CLIENT_SECRET")
COZE_WORKFLOW_ID = os.getenv("COZE_WORKFLOW_ID")



# --- 1. Cloudflare KV 操作函数 (存取 Token) ---
def get_kv_value(key):

    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/storage/kv/namespaces/{KV_NAMESPACE_ID}/values/{key}"
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}"}
    res = requests.get(url, headers=headers)
    return res.text if res.status_code == 200 else None


def set_kv_value(key, value):
    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/storage/kv/namespaces/{KV_NAMESPACE_ID}/values/{key}"
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}"}
    requests.put(url, headers=headers, data=str(value))


# --- 2. Cloudflare D1 操作函数 (文章去重) ---
def d1_query(sql):
    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{D1_DB_ID}/query"
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}"}
    response = requests.post(url, headers=headers, json={"sql": sql})
    res_json = response.json()

    # 增加错误检查
    if not res_json.get('success'):
        print(f"D1 查询失败: {res_json.get('errors')}")
        return []

    results = res_json.get('result', [])
    if results and len(results) > 0:
        return results[0].get('results', [])
    return []


# --- 3. 获取并自动续期 Coze Token (核心逻辑) ---
def get_coze_auth():
    # 从 KV 读取种子
    old_refresh_token = get_kv_value("COZE_LINGGO_REFRESH_TOKEN")
    if not old_refresh_token:
        raise Exception("KV 中找不到 COZE_LINGGO_REFRESH_TOKEN，请先手动在 CF 后台添加。")

    oauth_app = WebOAuthApp(client_id=COZE_CLIENT_ID, client_secret=COZE_CLIENT_SECRET, base_url=COZE_CN_BASE_URL)

    # 自动续期：拿旧的换新的
    new_token = oauth_app.refresh_access_token(refresh_token=old_refresh_token)

    # 将新的 token 立刻存回 KV，保证下次脚本运行能拿到最新的“种子”
    # set_kv_value("ACCESS_TOKEN", new_token.access_token)
    set_kv_value("COZE_LINGGO_REFRESH_TOKEN", new_token.refresh_token)

    return new_token.access_token


# --- 5. 主程序 ---
def main():
    try:
        # 获取续期后的 Token
        access_token = get_coze_auth()

        with open('config.json', 'r') as f:
            configs = json.load(f)

        for cfg in configs:
            feed = feedparser.parse(cfg['rss_url'])
            for entry in feed.entries:
                # D1 去重检查 (只存 ID 即可)
                exists = d1_query(f"SELECT id FROM processed_articles WHERE id='{entry.id}'")
                if not exists:
                    print(f"发现新文章: {entry.title}")

                    article_body = ""
                    if 'content' in entry and len(entry.content) > 0:
                        article_body = entry.content[0].value
                    elif 'summary' in entry:
                        article_body = entry.summary

                    # 调 Coze 工作流获取正文
                    coze = Coze(auth=TokenAuth(access_token), base_url=COZE_CN_BASE_URL)
                    coze.workflows.runs.create(
                        workflow_id=COZE_WORKFLOW_ID,
                        parameters={
                            "content": article_body,
                            "title": entry.title,
                            "space_id": cfg.get('space_id'),
                            "parent_wiki_token": cfg.get('parent_wiki_token')
                        }
                    )

                    # print(f"工作流结果: {workflow_res}")

                    # 标记为已同步
                    d1_query(f"INSERT INTO processed_articles (id) VALUES ('{entry.id}')")

    except Exception as e:
        print(f"脚本运行出错: {str(e)}")


if __name__ == "__main__":
    main()