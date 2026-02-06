import json
from flask import Flask, redirect, request
from cozepy import load_oauth_app_from_config

app = Flask(__name__)

# 配置信息
CONFIG_PATH = "coze_oauth_config.json"
REDIRECT_URI = "http://127.0.0.1:8080/callback"


def load_app():
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
    return load_oauth_app_from_config(config)


oauth_app = load_app()


@app.route("/")
def index():
    return '<a href="/login">点击这里开始授权</a>'


@app.route("/login")
def login():
    # 生成跳转到 Coze 的授权链接
    auth_url = oauth_app.get_oauth_url(redirect_uri=REDIRECT_URI)
    return redirect(auth_url)


@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return "授权失败，没拿到 code"

    # 用 code 换取 Token
    token = oauth_app.get_access_token(redirect_uri=REDIRECT_URI, code=code)

    # 打印到控制台和网页上
    print(f"\n{'=' * 20}\n你的 REFRESH_TOKEN 是:\n{token.refresh_token}\n{'=' * 20}\n")
    return f"授权成功！请去控制台看 refresh_token。<br>Refresh Token: {token.refresh_token}"


if __name__ == "__main__":
    print("服务已启动，请访问 http://127.0.0.1:8080")
    app.run(port=8080)