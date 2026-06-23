# -*- coding: utf-8 -*-
"""
生成 100 万行模拟业务日志文件 large_data.csv
包含 6 个字段，模拟真实业务日志中的脏数据和混合格式
"""

import sys
import subprocess
import importlib

# 自动检查并安装缺失的依赖
def install_if_missing(package_name, import_name=None):
    """检查包是否已安装，若缺失则自动安装"""
    if import_name is None:
        import_name = package_name
    try:
        importlib.import_module(import_name)
    except ImportError:
        print(f"正在安装 {package_name}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])

# 检查并安装 faker
install_if_missing("faker", "faker")

import csv
import random
import json
import uuid
from datetime import datetime, timedelta
from faker import Faker

# 初始化 Faker（设置中文 locale 以支持中文内容）
fake = Faker('zh_CN')
Faker.seed(0)
random.seed(0)

# 配置参数
TOTAL_ROWS = 1_000_000
OUTPUT_FILE = "large_data.csv"
PROGRESS_INTERVAL = 100_000

# 枚举值定义
EVENT_TYPES = ["登录", "点击", "clik", "logut"]  # 包含错误拼写
OS_OPTIONS = ["Windows", "Android", "iOS", "MacOS"]
BRAND_OPTIONS = ["小米", "华为", "苹果", "三星"]
MODEL_OPTIONS = ["Model A", "Model B"]

# 特殊字符集（用于 metadata 字段）
SPECIAL_CHARS = "!@#$%^&*()_+-=[]{}|;:,.<>?"


def generate_event_id(row_index):
    """
    生成 event_id：
    - 全局唯一（基于 UUID）
    - 2% 概率为空字符串（模拟脏数据）
    """
    if random.random() < 0.02:
        return ""
    return str(uuid.uuid4())


def generate_user_id():
    """
    生成 user_id：
    - 5% 概率为 -1（异常值）
    - 5% 概率为 "guest"（字符串）
    - 90% 概率为正常 UUID 格式 ID
    """
    rand = random.random()
    if rand < 0.05:
        return -1
    elif rand < 0.10:
        return "guest"
    else:
        return str(uuid.uuid4())


def generate_action_time():
    """
    生成 action_time：
    - 50% 概率为 ISO 8601 格式（如 2026-03-17T10:00:00）
    - 50% 概率为 Unix 毫秒时间戳（如 1742392800000）
    """
    # 生成一个随机时间（2025-01-01 到 2026-12-31 之间）
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2026, 12, 31)
    random_days = random.randint(0, (end_date - start_date).days)
    random_seconds = random.randint(0, 86399)
    random_time = start_date + timedelta(days=random_days, seconds=random_seconds)
    
    if random.random() < 0.5:
        # ISO 8601 格式
        return random_time.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        # Unix 毫秒时间戳
        return int(random_time.timestamp() * 1000)


def generate_event_type():
    """
    生成 event_type：
    - 从 ["登录", "点击", "clik", "logut"] 中随机选择
    - 包含错误拼写（clik 应为 click，logut 应为 logout）
    """
    return random.choice(EVENT_TYPES)


def generate_device_info():
    """
    生成 device_info：
    - JSON 格式字符串
    - 包含 os、brand、model 三个字段
    """
    device = {
        "os": random.choice(OS_OPTIONS),
        "brand": random.choice(BRAND_OPTIONS),
        "model": random.choice(MODEL_OPTIONS)
    }
    return json.dumps(device, ensure_ascii=False)


def generate_metadata():
    """
    生成 metadata：
    - 包含换行符（\n）和特殊字符
    - 模拟真实日志的杂乱格式
    """
    # 生成一段随机文本
    text_parts = [
        fake.sentence(),
        fake.sentence(),
        f"代码：{random.randint(1000, 9999)}",
        f"状态：{random.choice(['OK', 'ERROR', 'WARN'])}"
    ]
    # 随机插入特殊字符
    special_insert = "".join(random.sample(SPECIAL_CHARS, random.randint(3, 8)))
    text_parts.append(f"标记：{special_insert}")
    
    # 用换行符连接，并随机添加额外换行
    metadata_text = "\n".join(text_parts)
    if random.random() < 0.3:
        metadata_text += "\n\n" + fake.word()
    
    return metadata_text


def generate_row(row_index):
    """生成单行数据"""
    return {
        "event_id": generate_event_id(row_index),
        "user_id": generate_user_id(),
        "action_time": generate_action_time(),
        "event_type": generate_event_type(),
        "device_info": generate_device_info(),
        "metadata": generate_metadata()
    }


def main():
    """主函数：生成 CSV 文件"""
    fieldnames = ["event_id", "user_id", "action_time", "event_type", "device_info", "metadata"]
    
    print(f"开始生成 {TOTAL_ROWS:,} 行模拟业务日志数据...")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for i in range(TOTAL_ROWS):
            row = generate_row(i)
            writer.writerow(row)
            
            # 显示进度
            if (i + 1) % PROGRESS_INTERVAL == 0:
                progress = (i + 1) / TOTAL_ROWS * 100
                print(f"进度：{i + 1:,} / {TOTAL_ROWS:,} ({progress:.1f}%)")
    
    print(f"\n数据生成完成！文件保存在当前目录的{OUTPUT_FILE}")


if __name__ == "__main__":
    main()
