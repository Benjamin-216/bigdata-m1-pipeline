import os
import httpx
from dotenv import load_dotenv
from openai import OpenAI

def test_llm_connection():
    try:
        load_dotenv()
        api_key = os.getenv("SILICONFLOW_API_KEY")
        
        if not api_key:
            raise ValueError("未找到SILICONFLOW_API_KEY，请检查.env文件")
        
        http_client = httpx.Client(verify=False)
        client = OpenAI(
            api_key=api_key,
            base_url="https://api.siliconflow.cn/v1",
            http_client=http_client
        )
        
        response = client.chat.completions.create(
            model="deepseek-ai/DeepSeek-V4-Flash",
            messages=[
                {"role": "user", "content": "请用一句话介绍你自己"}
            ]
        )
        
        reply = response.choices[0].message.content
        print("=" * 50)
        print("模型回复：")
        print(reply)
        print("=" * 50)
        print("✓ LLM API连通性测试成功！")
        
    except ValueError as e:
        print(f"✗ 配置错误：{str(e)}")
    except Exception as e:
        print(f"✗ 连接失败：{str(e)}")

if __name__ == "__main__":
    test_llm_connection()