import requests
import json
from datetime import datetime
import os
from cozepy import Coze, TokenAuth, WebOAuthApp, COZE_CN_BASE_URL

CF_API_TOKEN = os.getenv("CF_API_TOKEN")
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
KV_NAMESPACE_ID = os.getenv("KV_NAMESPACE_ID")
D1_DB_ID = os.getenv("D1_DB_ID")

COZE_CLIENT_ID = os.getenv("COZE_CLIENT_ID")
COZE_CLIENT_SECRET = os.getenv("COZE_CLIENT_SECRET")
COZE_WORKFLOW_ID = os.getenv("COZE_WORKFLOW_ID")

# 飞书 Webhook 地址
FEISHU_WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/5c348238-aee4-43f0-9720-68a7ceb5a244"


# --- Cloudflare KV 操作函数 ---
def get_kv_value(key):
    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/storage/kv/namespaces/{KV_NAMESPACE_ID}/values/{key}"
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}"}
    res = requests.get(url, headers=headers)
    return res.text if res.status_code == 200 else None


def set_kv_value(key, value):
    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/storage/kv/namespaces/{KV_NAMESPACE_ID}/values/{key}"
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}"}
    requests.put(url, headers=headers, data=str(value))


# --- Cloudflare D1 操作函数 ---
def d1_query(sql):
    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{D1_DB_ID}/query"
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}"}
    response = requests.post(url, headers=headers, json={"sql": sql})
    res_json = response.json()

    if not res_json.get('success'):
        print(f"D1 查询失败: {res_json.get('errors')}")
        return []

    results = res_json.get('result', [])
    if results and len(results) > 0:
        return results[0].get('results', [])
    return []


# --- 飞书 Webhook 通知 ---
def send_feishu_notification(title, content):
    """发送飞书通知"""
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": title
                },
                "template": "red"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": content
                    }
                },
                {
                    "tag": "div",
                    "text": {
                        "tag": "plain_text",
                        "content": f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    }
                }
            ]
        }
    }

    try:
        response = requests.post(FEISHU_WEBHOOK_URL, json=payload)
        if response.status_code == 200:
            print("飞书通知发送成功")
        else:
            print(f"飞书通知发送失败: {response.text}")
    except Exception as e:
        print(f"发送飞书通知异常: {e}")


# --- 获取 LingoWhale 认证 Token ---
def get_lingowhale_tokens():
    """从 KV 获取 LingoWhale API 所需的 token"""
    access_token = get_kv_value("LINGOWHALE_ACCESS_TOKEN")
    auth_token = get_kv_value("LINGOWHALE_AUTH_TOKEN")
    b_id = get_kv_value("LINGOWHALE_B_ID")
    guest_id = get_kv_value("LINGOWHALE_GUEST_ID")

    return {
        "access-token": access_token,
        "auth-token": auth_token,
        "b-id": b_id,
        "guest-id": guest_id
    }


# --- 获取并自动续期 Coze Token ---
def get_coze_auth():
    """从 KV 读取 refresh_token 并自动续期"""
    old_refresh_token = get_kv_value("COZE_LINGGO_REFRESH_TOKEN")
    if not old_refresh_token:
        raise Exception("KV 中找不到 COZE_LINGGO_REFRESH_TOKEN，请先手动在 CF 后台添加。")

    oauth_app = WebOAuthApp(client_id=COZE_CLIENT_ID, client_secret=COZE_CLIENT_SECRET, base_url=COZE_CN_BASE_URL)

    # 自动续期：拿旧的换新的
    new_token = oauth_app.refresh_access_token(refresh_token=old_refresh_token)

    # 将新的 refresh_token 存回 KV
    set_kv_value("COZE_LINGGO_REFRESH_TOKEN", new_token.refresh_token)

    return new_token.access_token


# --- 爬虫：获取文章详情 ---
def fetch_entry_detail(entry_id, entry_type=7):
    """获取文章详情"""
    url = f"https://api.lingowhale.com/api/entry/detail?entry_id={entry_id}&entry_type={entry_type}"

    # 从 KV 获取 token
    tokens = get_lingowhale_tokens()

    headers = {
        "access-token": tokens.get("access-token", ""),
        "auth-token": tokens.get("auth-token", ""),
        "b-id": tokens.get("b-id", ""),
        "guest-id": tokens.get("guest-id", ""),
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        result = response.json()

        # 检查是否 token 失效
        if result.get("code") == 22003:
            print(f"Token 失效: {result.get('msg')}")
            # 发送飞书通知
            send_feishu_notification(
                "LingoWhale Token 失效告警",
                f"**错误码**: {result.get('code')}\n**错误信息**: {result.get('msg')}\n**请及时更新 KV 中的 Token 信息**"
            )
            return None

        if result.get("code") == 0:
            return result.get("data", {})

        print(f"API 返回错误: code={result.get('code')}, msg={result.get('msg')}")
        return None

    except Exception as e:
        print(f"请求文章详情异常: {e}")
        return None


# --- 订阅 Feed 并获取详情 ---
def fetch_feed_data(cursor: str = "", channel_ids=None, space_id=None, parent_wiki_token=None, coze_access_token=None):
    if channel_ids is None:
        channel_ids = []

    url = "https://api-public.lingowhale.com/api/feed/v2/feed/subscription"

    payload = {
        "cursor": cursor,
        "sort_type": 2,
        "limit": 10,
        "filter_unread": False,
        "channel_ids": channel_ids if channel_ids else ["6813a8c550ec085890ddaf46"]
    }

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        result = response.json()

        if result.get("code") == 0:
            feed_list = result.get("data", {}).get("feed_list", [])
            next_cursor = result.get("data", {}).get("cursor", "")

            print(f"成功获取 {len(feed_list)} 条数据")

            should_find_next_page = True

            for item in feed_list:
                title = item.get("title")
                entry_id = item.get("entry_id")
                entry_type = item.get("entry_type")

                exists = d1_query(f"SELECT id FROM processed_articles WHERE id='{entry_id}'")
                if not exists:
                    print(f"【新文章】: {title}")

                    # 获取文章详情
                    detail = fetch_entry_detail(entry_id, entry_type)
                    if detail:
                        url_info = detail.get("url_info", {})
                        content = url_info.get("content", "")
                        html_content = url_info.get("html_content", "")
                        author = url_info.get("author", "")
                        publish_time = url_info.get("publish_time", "")

                        print(f"  作者: {author}, 发布时间: {publish_time}")
                        print(f"  内容长度: {len(content)} 字")

                        # 调用 Coze 工作流处理内容
                        if coze_access_token and COZE_WORKFLOW_ID:
                            try:
                                coze = Coze(auth=TokenAuth(coze_access_token), base_url=COZE_CN_BASE_URL)
                                coze.workflows.runs.create(
                                    workflow_id=COZE_WORKFLOW_ID,
                                    parameters={
                                        "content": html_content or content,
                                        "title": title,
                                        "space_id": space_id,
                                        "parent_wiki_token": parent_wiki_token
                                    }
                                )
                                print(f"  Coze 工作流调用成功")
                            except Exception as e:
                                print(f"  Coze 工作流调用失败: {e}")

                        # 入库标记已处理
                        d1_query(f"INSERT INTO processed_articles (id) VALUES ('{entry_id}')")
                else:
                    should_find_next_page = False
                    print(f"【已存在】: {title}")

            # 递归获取下一页
            if should_find_next_page and next_cursor:
                fetch_feed_data(
                    cursor=next_cursor,
                    channel_ids=channel_ids,
                    space_id=space_id,
                    parent_wiki_token=parent_wiki_token,
                    coze_access_token=coze_access_token
                )

        else:
            print(f"API 返回错误: {result.get('msg')}")

    except Exception as e:
        print(f"请求发生异常: {e}")


def main():
    try:
        # 获取 Coze access_token
        coze_access_token = get_coze_auth()

        # 读取配置
        with open('config.json', 'r') as f:
            configs = json.load(f)

        for cfg in configs:
            print(f"\n处理订阅源: {cfg.get('name')}")
            fetch_feed_data(
                channel_ids=cfg.get('channel_ids', []),
                space_id=cfg.get('space_id'),
                parent_wiki_token=cfg.get('parent_wiki_token'),
                coze_access_token=coze_access_token
            )

    except Exception as e:
        print(f"脚本运行出错: {str(e)}")


if __name__ == "__main__":
    main()
