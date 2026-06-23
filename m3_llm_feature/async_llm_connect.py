import os
import json
import asyncio
import httpx
from dotenv import load_dotenv
from openai import AsyncOpenAI
from openai import APIError, RateLimitError, APIConnectionError

async def async_extract_comment(comment, semaphore):
    """
    异步分析电商评论，提取结构化信息
    :param comment: 评论文本
    :param semaphore: asyncio.Semaphore信号量用于限流
    :return: 包含sentiment、category、summary的字典，失败返回None
    """
    async with semaphore:
        try:
            load_dotenv()
            api_key = os.getenv("SILICONFLOW_API_KEY")
            
            if not api_key:
                raise ValueError("未找到SILICONFLOW_API_KEY，请检查.env文件")
            
            async with httpx.AsyncClient(verify=False) as http_client:
                client = AsyncOpenAI(
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
                
                response = await client.chat.completions.create(
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
                        raise ValueError(f"模型输出格式错误，无法解析JSON: {raw_output}")
                
                if not all(key in result for key in ['sentiment', 'category', 'summary']):
                    raise ValueError(f"JSON缺少必要字段")
                
                valid_sentiments = ['正面', '负面', '中性']
                valid_categories = ['物流', '质量', '价格', '服务', '综合']
                
                if result['sentiment'] not in valid_sentiments:
                    raise ValueError(f"sentiment值无效: {result['sentiment']}")
                
                if result['category'] not in valid_categories:
                    raise ValueError(f"category值无效: {result['category']}")
                
                if len(result['summary']) > 15:
                    raise ValueError(f"summary超过15字限制")
                
                return result
                
        except ValueError as e:
            print(f"✗ 配置或格式错误 [{comment[:20]}...]: {str(e)}")
            return None
        except RateLimitError as e:
            print(f"✗ API限流 [{comment[:20]}...]: {str(e)}")
            return None
        except APIConnectionError as e:
            print(f"✗ 网络连接错误 [{comment[:20]}...]: {str(e)}")
            return None
        except APIError as e:
            print(f"✗ API调用错误 [{comment[:20]}...]: {str(e)}")
            return None
        except Exception as e:
            print(f"✗ 未知错误 [{comment[:20]}...]: {str(e)}")
            return None

async def main():
    """
    主函数：异步调用测试评论，验证异步调用正常
    """
    # 测试数据：3条电商评论
    test_comments = [
        "发货超级快，客服态度很好，非常满意",
        "质量太差了，用了两天就坏了，退货还很麻烦",
        "价格适中，物流一般，整体还行"
    ]
    
    print("=" * 60)
    print("异步大模型API测试")
    print("=" * 60)
    
    # 创建信号量，限制最大并发数为10
    semaphore = asyncio.Semaphore(10)
    
    # 创建异步任务列表
    tasks = [async_extract_comment(comment, semaphore) for comment in test_comments]
    
    # 并发执行所有任务
    results = await asyncio.gather(*tasks)
    
    # 打印结果
    print("\n测试结果：")
    for i, (comment, result) in enumerate(zip(test_comments, results), 1):
        print(f"\n[{i}] 评论: {comment}")
        if result:
            print(f"    解析结果: {json.dumps(result, ensure_ascii=False)}")
        else:
            print(f"    解析结果: 失败")
    
    print("\n" + "=" * 60)
    print("测试完成")

if __name__ == "__main__":
    asyncio.run(main())