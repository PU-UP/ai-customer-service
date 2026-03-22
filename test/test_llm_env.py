"""
从项目根目录 .env 读取 LLM 相关配置，发起一次最小 chat 调用，验证大模型是否可用。

用法（在项目根目录执行）:
    python test/test_llm_env.py
"""

from __future__ import annotations

import os
import sys

# 保证无论从何处启动都能找到 app 包
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _mask_secret(s: str, head: int = 6, tail: int = 4) -> str:
    s = (s or "").strip()
    if len(s) <= head + tail:
        return "***" if s else "(empty)"
    return f"{s[:head]}...{s[-tail:]}"


def main() -> int:
    from app.config import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL, LLM_TIMEOUT_SECONDS, PROJECT_ROOT

    print("配置来源: .env（由 app.config 加载）")
    print(f"  PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"  LLM_BASE_URL: {LLM_BASE_URL or '(empty)'}")
    print(f"  LLM_MODEL:    {LLM_MODEL or '(empty)'}")
    print(f"  LLM_API_KEY:  {_mask_secret(LLM_API_KEY)}")
    print(f"  timeout_s:    {LLM_TIMEOUT_SECONDS}")

    try:
        from openai import OpenAI
    except Exception as e:
        print(f"\n失败: 未安装 openai 包 ({e!r})")
        return 1

    if not LLM_API_KEY or not LLM_BASE_URL or not LLM_MODEL:
        print("\n失败: LLM_API_KEY / LLM_BASE_URL / LLM_MODEL 至少有一项为空，请检查根目录 .env")
        return 1

    messages = [
        {"role": "user", "content": '只回复两个大写字母：OK。不要其它任何字符。'},
    ]

    try:
        client = OpenAI(
            api_key=LLM_API_KEY,
            base_url=LLM_BASE_URL,
            timeout=float(LLM_TIMEOUT_SECONDS),
        )
        resp = client.chat.completions.create(model=LLM_MODEL, messages=messages)
    except Exception as e:
        print(f"\n调用失败: {e!r}")
        return 1

    try:
        text = (resp.choices[0].message.content or "").strip()
    except Exception:
        text = ""

    usage = getattr(resp, "usage", None)
    if usage is not None:
        pt = int(getattr(usage, "prompt_tokens", 0) or 0)
        ct = int(getattr(usage, "completion_tokens", 0) or 0)
        tt = int(getattr(usage, "total_tokens", 0) or 0)
        print(f"\nusage: prompt={pt} completion={ct} total={tt}")
    else:
        print("\nusage: (未返回)")

    if not text:
        print("\n失败: 模型返回空内容")
        return 1

    print(f"\n模型回复:\n{text}\n")
    print("成功: 大模型接口可用。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
