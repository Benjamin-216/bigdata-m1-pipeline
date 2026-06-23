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
            raise ValueError("未找到SILICONFLOW_API_KEY")
        
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
                raise ValueError("模型输出格式错误")
        
        if not all(key in result for key in ['sentiment', 'category', 'summary']):
            raise ValueError("JSON缺少必要字段")
        
        valid_sentiments = ['正面', '负面', '中性']
        valid_categories = ['物流', '质量', '价格', '服务', '综合']
        
        if result['sentiment'] not in valid_sentiments:
            raise ValueError("sentiment值无效")
        
        if result['category'] not in valid_categories:
            raise ValueError("category值无效")
        
        if len(result['summary']) > 15:
            raise ValueError("summary超过15字限制")
        
        return result
        
    except Exception:
        return None

def batch_extract_and_export(comments, output_file="comment_llm_result.csv"):
    """
    批量处理评论并导出到CSV
    :param comments: 评论列表
    :param output_file: 输出CSV文件名
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
    
    df = pd.DataFrame(results)
    
    try:
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n✓ CSV文件保存成功: {os.path.abspath(output_file)}")
    except Exception as e:
        print(f"\n✗ CSV文件保存失败: {str(e)}")
        return None
    
    return df

if __name__ == "__main__":
    test_comments = [
        "发货太慢了，包装破损",
        "质量很好，性价比超高",
        "客服回复很慢，服务差",
        "价格实惠，整体满意",
        "物流很快，商品一般"
    ]
    
    print("开始批量处理评论并导出CSV...")
    print("=" * 60)
    
    result_df = batch_extract_and_export(test_comments)
    
    if result_df is not None:
        print("\n最终合并结果表格：")
        print(result_df.to_string(index=False))