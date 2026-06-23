import os
import json
import time
import httpx
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

def extract_comment_info(comment):
    """
    分析电商评论，提取结构化信息
    :param comment: 评论文本
    :return: 包含sentiment、category、summary的字典，失败返回None
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
            raw_output = raw_output.replace('"', '",').replace(',}', '}').replace(',]', ']')
            raw_output = raw_output[:-1] if raw_output.endswith(',') else raw_output
            try:
                result = json.loads(raw_output)
            except json.JSONDecodeError:
                raise ValueError(f"模型输出格式错误，无法解析JSON")
        
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
        
    except Exception:
        return None

def batch_extract_comments(comments):
    """
    批量处理评论提取，统计耗时
    :param comments: 评论列表
    :return: 处理结果列表，包含每条的耗时
    """
    results = []
    total_time = 0.0
    
    for idx, comment in enumerate(comments, 1):
        start_time = time.time()
        
        try:
            extracted = extract_comment_info(comment)
            if extracted:
                sentiment = extracted['sentiment']
                category = extracted['category']
                summary = extracted['summary']
            else:
                sentiment = "解析失败"
                category = "解析失败"
                summary = "解析失败"
        except Exception:
            sentiment = "解析失败"
            category = "解析失败"
            summary = "解析失败"
        
        elapsed = time.time() - start_time
        total_time += elapsed
        
        results.append({
            "评论原文": comment,
            "sentiment": sentiment,
            "category": category,
            "summary": summary,
            "单条耗时(秒)": round(elapsed, 2)
        })
        
        print(f"处理进度: {idx}/{len(comments)}")
    
    return results, total_time

if __name__ == "__main__":
    # 测试数据：5条真实电商评论，覆盖多类别和情感
    test_comments = [
        "发货太慢了，包装破损",
        "质量很好，性价比超高",
        "客服回复很慢，服务差",
        "价格实惠，整体满意",
        "物流很快，商品一般"
    ]
    
    print("开始批量处理评论...")
    print("=" * 60)
    
    results, total_time = batch_extract_comments(test_comments)
    
    df = pd.DataFrame(results)
    print("\n处理结果表格：")
    print(df.to_string(index=False))
    
    print("\n" + "=" * 60)
    print("耗时统计摘要：")
    print(f"评论总数: {len(test_comments)} 条")
    print(f"总耗时: {round(total_time, 2)} 秒")
    print(f"平均单条耗时: {round(total_time / len(test_comments), 2)} 秒")
    print("=" * 60)