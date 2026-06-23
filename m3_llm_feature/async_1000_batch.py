"""
大数据实验10 任务4：1000条数据高速清洗与落盘（优化版）

优化内容：
1. 模型升级：Qwen/Qwen3.5-4B
2. 并发优化：Semaphore=30
3. 重试策略：stop_after_attempt(5) + 精准异常重试
4. 性能增强：httpx连接池 + 列表推导式优化
5. 速度监控：实时输出耗时统计
"""

import os
import json
import asyncio
import time
import httpx
import pandas as pd
from dotenv import load_dotenv
from openai import AsyncOpenAI
from openai import APIError, RateLimitError, APIConnectionError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm.asyncio import tqdm_asyncio

load_dotenv()
API_KEY = os.getenv("SILICONFLOW_API_KEY")
BASE_URL = "https://api.siliconflow.cn/v1"

if not API_KEY:
    raise ValueError("未找到SILICONFLOW_API_KEY，请检查.env文件")

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((RateLimitError, APIConnectionError)),
    reraise=False
)
async def extract_features(comment, semaphore):
    prompt = f"""你是电商评论分析师，对用户给出的商品评价，严格输出纯JSON，禁止多余文字、解释、markdown。
JSON固定3个字段：
- sentiment：只能三选一：正面/负面/中性
- category：只能五选一：物流/质量/价格/服务/综合
- summary：15字以内，一句话概括核心问题或感受

用户评论：{comment}
输出："""
    
    async with semaphore:
        try:
            async with httpx.AsyncClient(verify=False, timeout=httpx.Timeout(15.0, connect=5.0)) as http_client:
                client = AsyncOpenAI(
                    api_key=API_KEY,
                    base_url=BASE_URL,
                    http_client=http_client
                )
                
                response = await client.chat.completions.create(
                    model="Qwen/Qwen3.5-4B",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    timeout=15
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
                        return {"sentiment": "解析失败", "category": "解析失败", "summary": "格式错误"}
                
                if not all(key in result for key in ['sentiment', 'category', 'summary']):
                    return {"sentiment": "解析失败", "category": "解析失败", "summary": "字段缺失"}
                
                valid_sentiments = ['正面', '负面', '中性']
                valid_categories = ['物流', '质量', '价格', '服务', '综合']
                
                if result['sentiment'] not in valid_sentiments:
                    return {"sentiment": "解析失败", "category": "解析失败", "summary": "情感值无效"}
                
                if result['category'] not in valid_categories:
                    return {"sentiment": "解析失败", "category": "解析失败", "summary": "分类值无效"}
                
                if len(result['summary']) > 15:
                    result['summary'] = result['summary'][:15]
                
                return result
            
        except RateLimitError as e:
            raise e
        except APIConnectionError as e:
            raise e
        except APIError as e:
            return {"sentiment": "解析失败", "category": "解析失败", "summary": "API错误"}
        except Exception as e:
            return {"sentiment": "解析失败", "category": "解析失败", "summary": str(e)[:20]}

async def main():
    print("=" * 60)
    print("大数据实验10 任务4：1000条数据高速清洗与落盘（优化版）")
    print("=" * 60)
    print(f"模型: Qwen/Qwen3.5-4B")
    print(f"并发数: 30")
    print(f"重试策略: 最多5次，指数退避(2-30秒)")
    print("-" * 60)
    
    print("\n[1/4] 读取数据...")
    csv_path = "data/online_shopping_10_cats.csv"
    try:
        df = pd.read_csv(csv_path)
        print(f"      原始数据条数: {len(df)}")
        df = df.head(1000)
        comments = df['review'].tolist()
        print(f"      截取后数据条数: {len(comments)}")
    except Exception as e:
        print(f"      ✗ 读取数据失败: {str(e)}")
        return
    
    sem = asyncio.Semaphore(30)
    
    print("\n[2/4] 创建异步任务...")
    tasks = [extract_features(text, sem) for text in comments]
    
    print("\n[3/4] 开始异步批量处理...")
    start_time = time.time()
    
    results = await tqdm_asyncio.gather(
        *tasks,
        desc="处理进度",
        total=len(tasks),
        unit="条",
        leave=True
    )
    
    total_time = time.time() - start_time
    
    print("\n[4/4] 结果处理与落盘...")
    
    results_df = pd.DataFrame(results)
    final_df = pd.concat([df.reset_index(drop=True), results_df], axis=1)
    
    success_mask = final_df['sentiment'] != '解析失败'
    success_count = success_mask.sum()
    fail_count = len(final_df) - success_count
    
    output_file = "batch_1000_features.csv"
    final_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    
    avg_time = total_time / len(comments)
    throughput = len(comments) / total_time
    
    print("\n" + "=" * 60)
    print("📊 性能统计摘要：")
    print("-" * 40)
    print(f"总条数: {len(final_df)}")
    print(f"成功条数: {success_count}")
    print(f"失败条数: {fail_count}")
    print(f"成功率: {(success_count / len(final_df)) * 100:.2f}%")
    print("-" * 40)
    print(f"⏱️ 总耗时: {total_time:.2f}秒")
    print(f"⏱️ 平均单条耗时: {avg_time:.2f}秒")
    print(f"📈 吞吐量: {throughput:.2f}条/秒")
    print("-" * 40)
    print(f"💾 结果已保存到: {output_file}")
    print("=" * 60)
    print("🚀 批量处理完成！")
    
    print("\n📋 优化对比参考：")
    print("  优化前预估（20并发+DeepSeek-V4-Flash）: ~60-80秒")
    print(f"  优化后实际（30并发+Qwen3.5-4B）: {total_time:.2f}秒")
    print(f"  预估提升: {(1 - total_time / 70) * 100:.1f}%")

if __name__ == "__main__":
    asyncio.run(main())