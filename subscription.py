import requests
import json
from datetime import datetime
import os

CF_API_TOKEN = os.getenv("CF_API_TOKEN")
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
KV_NAMESPACE_ID = os.getenv("KV_NAMESPACE_ID")  # 你的 KV ID
D1_DB_ID = os.getenv("D1_DB_ID")  # 这里的 D1 仅保留用于文章去重

COZE_CLIENT_ID = os.getenv("COZE_CLIENT_ID")
COZE_CLIENT_SECRET = os.getenv("COZE_CLIENT_SECRET")
COZE_WORKFLOW_ID = os.getenv("COZE_WORKFLOW_ID")


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

def fetch_feed_data(cursor:str = "", channel_ids=None):
    if channel_ids is None:
        channel_ids = []
    url = "https://api-public.lingowhale.com/api/feed/v2/feed/subscription"

    # 构建请求体
    payload = {
        "cursor": cursor,
        "sort_type": 2,
        "limit": 10,
        "filter_unread": False,
        "channel_ids": ["6813a8c550ec085890ddaf46"]
    }

    # 设置 Headers（模拟浏览器或正常客户端）
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # 发送 POST 请求
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # 检查请求是否成功

        result = response.json()

        if result.get("code") == 0:
            feed_list = result.get("data", {}).get("feed_list", [])
            cursor = result.get("data", {}).get("cursor", "")

            print(f"成功获取 {len(feed_list)} 条数据：\n")

            should_find_next_page = True

            for item in feed_list:
                title = item.get("title")
                # desc = item.get("description")
                entry_id = item.get("entry_id")
                entry_type = item.get("entry_type")

                exists = d1_query(f"SELECT id FROM processed_articles WHERE id='{entry_id}'")
                if not exists:
                    # 入库

                else:
                    should_find_next_page = False

                print(f"【标题】: {title}")

                if should_find_next_page:
                    fetch_feed_data(cursor=cursor, channel_ids=channel_ids)


        else:
            print(f"API 返回错误: {result.get('msg')}")

    except Exception as e:
        print(f"请求发生异常: {e}")


if __name__ == "__main__":
    fetch_feed_data()