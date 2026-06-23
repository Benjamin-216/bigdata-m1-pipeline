import os
import json
import time
import httpx
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

def extract_comment_info(comment, model_name):
    """
    分析电商评论，提取结构化信息
    :param comment: 评论文本
    :param model_name: 模型名称
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
            model=model_name,
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
                return None
        
        if not all(key in result for key in ['sentiment', 'category', 'summary']):
            return None
        
        valid_sentiments = ['正面', '负面', '中性']
        valid_categories = ['物流', '质量', '价格', '服务', '综合']
        
        if result['sentiment'] not in valid_sentiments:
            return None
        
        if result['category'] not in valid_categories:
            return None
        
        if len(result['summary']) > 15:
            return None
        
        return result
        
    except Exception:
        return None

def test_model(comments, model_name):
    """
    使用指定模型测试所有评论
    :param comments: 评论列表
    :param model_name: 模型名称
    :return: 测试结果列表, 总耗时, 成功数
    """
    results = []
    total_time = 0.0
    success_count = 0
    
    print(f"\n{'='*60}")
    print(f"正在测试模型: {model_name}")
    print(f"{'='*60}")
    
    for idx, comment in enumerate(comments, 1):
        start_time = time.time()
        
        extracted = extract_comment_info(comment, model_name)
        
        elapsed = time.time() - start_time
        total_time += elapsed
        
        if extracted:
            sentiment = extracted['sentiment']
            category = extracted['category']
            summary = extracted['summary']
            success_count += 1
        else:
            sentiment = "解析失败"
            category = "解析失败"
            summary = "解析失败"
        
        results.append({
            "评论原文": comment,
            "sentiment": sentiment,
            "category": category,
            "summary": summary,
            "耗时(秒)": round(elapsed, 2)
        })
        
        print(f"评论{idx}: {comment[:10]}... -> sentiment={sentiment}, category={category}, 耗时={elapsed:.2f}秒")
    
    return results, total_time, success_count

def main():
    # 测试数据
    test_comments = [
        "发货太慢了，包装破损",
        "质量很好，性价比超高",
        "客服回复很慢，服务差",
        "价格实惠，整体满意",
        "物流很快，商品一般"
    ]
    
    # 模型配置
    models = [
        {"name": "deepseek-ai/DeepSeek-V4-Flash", "alias": "DeepSeek-V4-Flash"},
        {"name": "Qwen/Qwen2.5-7B-Instruct", "alias": "Qwen2.5-7B"}
    ]
    
    print("大模型A/B对比测试开始")
    print(f"测试评论数: {len(test_comments)}")
    
    # 存储两个模型的测试结果
    model_results = {}
    
    for model in models:
        results, total_time, success_count = test_model(test_comments, model["name"])
        model_results[model["alias"]] = {
            "results": results,
            "total_time": total_time,
            "success_count": success_count,
            "avg_time": total_time / len(test_comments)
        }
    
    # 生成对比表格
    print("\n" + "="*60)
    print("模型对比结果表格")
    print("="*60)
    
    compare_data = []
    for i, comment in enumerate(test_comments):
        row = {"评论原文": comment}
        for model_alias in model_results.keys():
            result = model_results[model_alias]["results"][i]
            row[f"{model_alias}_sentiment"] = result["sentiment"]
            row[f"{model_alias}_category"] = result["category"]
            row[f"{model_alias}_summary"] = result["summary"]
        compare_data.append(row)
    
    compare_df = pd.DataFrame(compare_data)
    print(compare_df.to_string(index=False))
    
    # 输出统计总结
    print("\n" + "="*60)
    print("测试统计总结")
    print("="*60)
    
    for model_alias, stats in model_results.items():
        stability = (stats["success_count"] / len(test_comments)) * 100
        print(f"\n【{model_alias}】")
        print(f"  总耗时: {stats['total_time']:.2f} 秒")
        print(f"  平均耗时: {stats['avg_time']:.2f} 秒/条")
        print(f"  格式稳定性: {stability:.1f}% ({stats['success_count']}/{len(test_comments)})")
    
    # 对比结论
    print("\n" + "="*60)
    print("对比结论")
    print("="*60)
    
    model1, model2 = list(model_results.keys())
    time_diff = model_results[model1]["avg_time"] - model_results[model2]["avg_time"]
    
    if time_diff > 0:
        faster_model = model2
        slower_model = model1
        speed_ratio = model_results[model1]["avg_time"] / model_results[model2]["avg_time"]
        print(f"速度对比: {faster_model} 比 {slower_model} 快 {speed_ratio:.2f} 倍")
    else:
        faster_model = model1
        slower_model = model2
        speed_ratio = model_results[model2]["avg_time"] / model_results[model1]["avg_time"]
        print(f"速度对比: {faster_model} 比 {slower_model} 快 {speed_ratio:.2f} 倍")
    
    stability1 = model_results[model1]["success_count"] / len(test_comments)
    stability2 = model_results[model2]["success_count"] / len(test_comments)
    
    if stability1 > stability2:
        print(f"格式稳定性: {model1} 更稳定 ({stability1*100:.1f}% vs {stability2*100:.1f}%)")
    elif stability2 > stability1:
        print(f"格式稳定性: {model2} 更稳定 ({stability2*100:.1f}% vs {stability1*100:.1f}%)")
    else:
        print(f"格式稳定性: 两者相当 ({stability1*100:.1f}%)")

if __name__ == "__main__":
    main()