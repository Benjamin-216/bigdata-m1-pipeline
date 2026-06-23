import os
import json
import httpx
from dotenv import load_dotenv
from openai import OpenAI

def extract_comment_info(comment):
    """
    分析电商评论，提取结构化信息
    :param comment: 评论文本
    :return: 包含sentiment、category、summary的字典
    """
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
        
        prompt = f"""你是电商评论分析师，对用户给出的商品评价，严格输出纯JSON，禁止多余文字、解释、markdown。
JSON固定3个字段：
- sentiment：只能三选一：正面/负面/中性
- category：只能五选一：物流/质量/价格/服务/综合
- summary：15字以内，一句话概括核心问题或感受

用户评论：{comment}
输出："""
        
        response = client.chat.completions.create(
            model="deepseek-ai/DeepSeek-V4-Flash",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        
        raw_output = response.choices[0].message.content.strip()
        
        try:
            result = json.loads(raw_output)
        except json.JSONDecodeError:
            raise ValueError(f"模型输出格式错误，无法解析JSON: {raw_output}")
        
        if not all(key in result for key in ['sentiment', 'category', 'summary']):
            raise ValueError(f"JSON缺少必要字段: {raw_output}")
        
        valid_sentiments = ['正面', '负面', '中性']
        valid_categories = ['物流', '质量', '价格', '服务', '综合']
        
        if result['sentiment'] not in valid_sentiments:
            raise ValueError(f"sentiment值无效: {result['sentiment']}")
        
        if result['category'] not in valid_categories:
            raise ValueError(f"category值无效: {result['category']}")
        
        if len(result['summary']) > 15:
            raise ValueError(f"summary超过15字限制: {result['summary']}")
        
        return result
        
    except ValueError as e:
        print(f"✗ 配置或格式错误: {str(e)}")
        return None
    except Exception as e:
        print(f"✗ 网络或API错误: {str(e)}")
        return None

if __name__ == "__main__":
    test_comment = "发货超级快，客服态度很好，非常满意"
    print(f"测试评论: {test_comment}")
    result = extract_comment_info(test_comment)
    if result:
        print(f"解析结果: {json.dumps(result, ensure_ascii=False)}")