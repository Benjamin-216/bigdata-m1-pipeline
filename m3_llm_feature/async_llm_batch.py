"""
大数据实验10 任务2：高并发异步批量提取+自动重试

依赖安装：
pip install tenacity tqdm pandas python-dotenv openai httpx
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

async def async_extract_comment(comment, semaphore):
    """
    异步分析电商评论，提取结构化信息（带自动重试）
    :param comment: 评论文本
    :param semaphore: asyncio.Semaphore信号量用于限流
    :return: 包含sentiment、category、summary的字典，失败返回None
    """
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((RateLimitError, APIConnectionError)),
        reraise=False
    )
    async def extract_with_retry():
        async with semaphore:
            try:
                load_dotenv()
                api_key = os.getenv("SILICONFLOW_API_KEY")
                
                if not api_key:
                    raise ValueError("未找到SILICONFLOW_API_KEY，请检查.env文件")
                
                async with httpx.AsyncClient(verify=False, timeout=30) as http_client:
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
                            raise ValueError(f"模型输出格式错误")
                    
                    if not all(key in result for key in ['sentiment', 'category', 'summary']):
                        raise ValueError(f"JSON缺少必要字段")
                    
                    valid_sentiments = ['正面', '负面', '中性']
                    valid_categories = ['物流', '质量', '价格', '服务', '综合']
                    
                    if result['sentiment'] not in valid_sentiments:
                        raise ValueError(f"sentiment值无效")
                    
                    if result['category'] not in valid_categories:
                        raise ValueError(f"category值无效")
                    
                    if len(result['summary']) > 15:
                        raise ValueError(f"summary超过15字限制")
                    
                    return result
                    
            except ValueError as e:
                raise ValueError(f"配置或格式错误: {str(e)}")
            except RateLimitError as e:
                raise e
            except APIConnectionError as e:
                raise e
            except APIError as e:
                raise ValueError(f"API调用错误: {str(e)}")
            except Exception as e:
                raise ValueError(f"未知错误: {str(e)}")
    
    try:
        return await extract_with_retry()
    except Exception as e:
        return {"error": str(e)}

def generate_test_comments():
    """
    生成100条模拟电商评论，覆盖多类别和情感
    :return: 评论列表
    """
    comments = []
    
    # 物流相关（20条）
    logistics_comments = [
        "发货速度很快，第二天就收到了",
        "物流超级慢，等了整整一周",
        "快递员态度很好，送货上门",
        "包裹收到时已经破损",
        "物流速度一般，还可以接受",
        "顺丰快递就是快，点赞",
        "物流信息更新不及时",
        "快递员电话都不打直接放驿站",
        "发货及时，包装完好",
        "物流太慢，影响使用体验",
        "京东物流真给力，当日达",
        "包裹被淋湿了，包装太差",
        "快递员很负责，当面签收",
        "物流速度超出预期",
        "等了好久才发货，差评",
        "物流跟踪信息清晰",
        "快递放在门口不安全",
        "发货快，物流也快",
        "包裹延迟送达，没有通知",
        "物流服务总体满意"
    ]
    comments.extend(logistics_comments)
    
    # 质量相关（20条）
    quality_comments = [
        "质量非常好，物超所值",
        "质量太差，用了几天就坏了",
        "材质很好，手感不错",
        "做工粗糙，有瑕疵",
        "质量一般，对得起价格",
        "品质优秀，值得购买",
        "商品有划痕，不满意",
        "用料扎实，很耐用",
        "质量问题，申请退货",
        "品质稳定，多次回购",
        "包装简陋，商品受损",
        "质量超出预期",
        "做工精细，细节到位",
        "商品与描述不符",
        "质量不错，性价比高",
        "有瑕疵但不影响使用",
        "质量堪忧，不推荐购买",
        "正品保证，质量可靠",
        "收到的是次品",
        "质量满意，好评"
    ]
    comments.extend(quality_comments)
    
    # 价格相关（20条）
    price_comments = [
        "价格实惠，性价比高",
        "太贵了，不值这个价",
        "价格合理，物有所值",
        "比其他店贵很多",
        "价格适中，可以接受",
        "活动价很划算",
        "原价太贵，打折才买",
        "价格透明，没有隐藏费用",
        "价格虚高，性价比低",
        "物超所值，价格美丽",
        "价格波动大，买贵了",
        "特价商品很划算",
        "价格公道，值得推荐",
        "价格偏高，但质量好",
        "性价比一般",
        "价格实惠，回购多次",
        "价格太贵，不会再买",
        "促销活动力度大",
        "价格合理，满意",
        "价格有点贵，质量一般"
    ]
    comments.extend(price_comments)
    
    # 服务相关（20条）
    service_comments = [
        "客服态度很好，回复及时",
        "客服不理人，服务差",
        "售后很耐心，解决问题快",
        "客服回复太慢，体验差",
        "服务态度一般",
        "客服很专业，解答详细",
        "售后推诿，不负责任",
        "售前咨询很耐心",
        "服务热情周到",
        "客服态度恶劣",
        "售后处理及时",
        "客服回复敷衍",
        "服务态度很好，点赞",
        "售后态度差，不解决问题",
        "客服响应快，服务满意",
        "服务有待提高",
        "客服很贴心，有问必答",
        "售后流程繁琐",
        "服务态度不错",
        "客服爱答不理"
    ]
    comments.extend(service_comments)
    
    # 综合评价（20条）
    general_comments = [
        "整体满意，下次还会回购",
        "非常失望，不会再来",
        "体验一般，没什么惊喜",
        "超出预期，非常满意",
        "总体来说还可以",
        "很不满意，差评",
        "购物体验很好",
        "一般般，不推荐",
        "非常棒，强烈推荐",
        "整体感觉一般",
        "各方面都很满意",
        "体验不佳，有待改进",
        "整体不错，性价比高",
        "不太满意，有瑕疵",
        "非常满意，好评",
        "总体还行，勉强接受",
        "购物愉快，推荐",
        "一般，不建议购买",
        "整体满意，物有所值",
        "体验一般，价格偏高"
    ]
    comments.extend(general_comments)
    
    return comments

async def async_batch_extract(comments):
    """
    异步批量提取评论信息
    :param comments: 评论列表
    :return: 处理结果列表，总耗时
    """
    start_time = time.time()
    
    # 创建信号量，限制最大并发数为10
    semaphore = asyncio.Semaphore(10)
    
    # 创建异步任务列表
    tasks = [async_extract_comment(comment, semaphore) for comment in comments]
    
    # 使用tqdm显示进度条
    results = await tqdm_asyncio.gather(
        *tasks,
        desc="处理进度",
        total=len(comments),
        unit="条"
    )
    
    total_time = time.time() - start_time
    
    return results, total_time

def analyze_results(comments, results, total_time):
    """
    分析处理结果，生成统计摘要
    :param comments: 原始评论列表
    :param results: 处理结果列表
    :param total_time: 总耗时
    :return: DataFrame结果，统计信息
    """
    success_count = 0
    fail_count = 0
    failed_indices = []
    
    processed_results = []
    
    for idx, (comment, result) in enumerate(zip(comments, results)):
        if result is None:
            processed_results.append({
                "评论原文": comment,
                "sentiment": "解析失败",
                "category": "解析失败",
                "summary": "解析失败",
                "失败原因": "未知错误"
            })
            fail_count += 1
            failed_indices.append(idx)
        elif "error" in result:
            processed_results.append({
                "评论原文": comment,
                "sentiment": "解析失败",
                "category": "解析失败",
                "summary": "解析失败",
                "失败原因": result["error"]
            })
            fail_count += 1
            failed_indices.append(idx)
        else:
            processed_results.append({
                "评论原文": comment,
                "sentiment": result["sentiment"],
                "category": result["category"],
                "summary": result["summary"],
                "失败原因": ""
            })
            success_count += 1
    
    df = pd.DataFrame(processed_results)
    
    # 统计信息
    stats = {
        "总条数": len(comments),
        "成功条数": success_count,
        "失败条数": fail_count,
        "成功率": f"{(success_count / len(comments)) * 100:.2f}%",
        "总耗时": f"{total_time:.2f}秒",
        "平均单条耗时": f"{total_time / len(comments):.2f}秒"
    }
    
    return df, stats

async def main():
    """
    主函数：异步批量处理100条评论，生成统计报告
    """
    print("=" * 60)
    print("大数据实验10 任务2：高并发异步批量提取")
    print("=" * 60)
    
    # 生成100条测试评论
    print("\n[1/4] 生成测试数据...")
    test_comments = generate_test_comments()
    print(f"      生成 {len(test_comments)} 条模拟电商评论")
    
    # 异步批量处理
    print("\n[2/4] 开始异步批量提取...")
    results, total_time = await async_batch_extract(test_comments)
    
    # 分析结果
    print("\n[3/4] 分析处理结果...")
    df, stats = analyze_results(test_comments, results, total_time)
    
    # 打印统计摘要
    print("\n[4/4] 统计摘要：")
    print("-" * 40)
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    # 计算加速比（假设实验9同步处理5条评论约需15-20秒）
    # 根据实验9的处理速度估算：同步处理5条约15秒，即单条约3秒
    sync_estimated_time = len(test_comments) * 3  # 假设同步单条3秒
    speedup_ratio = sync_estimated_time / total_time if total_time > 0 else 0
    print(f"\n加速比分析（对比同步处理）：")
    print(f"  同步处理估算时间: {sync_estimated_time:.2f}秒")
    print(f"  异步处理实际时间: {total_time:.2f}秒")
    print(f"  异步加速比: {speedup_ratio:.2f}x")
    
    # 保存结果到CSV
    output_file = "async_comments_result.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n结果已保存到: {output_file}")
    
    print("\n" + "=" * 60)
    print("批量处理完成！")

if __name__ == "__main__":
    asyncio.run(main())