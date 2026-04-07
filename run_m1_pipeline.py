"""
M1 数据处理管道 - 主程序入口
==============================

模块说明：
    本模块为 M1 数据处理管道的可执行入口脚本，负责初始化管道实例并触发
    全流程执行（extract → transform → load），同时提供完整的日志输出、
    异常处理与性能基准测试（Benchmark）。

使用方式：
    命令行执行：
        python run_m1_pipeline.py

    或作为模块导入：
        from run_m1_pipeline import run_pipeline
        result = run_pipeline()

配置说明：
    - INPUT_PATH:  输入 Parquet 文件路径（week1/data/UserBehavior.parquet）
    - OUTPUT_PATH: 输出 Parquet 文件路径（week1/data/m1_final_clean.parquet）
    - 可通过修改下方常量自定义路径

作者：AI 辅助生成
版本：v2.0（审计优化版）
日期：2026-04-07
"""

import sys
import logging
import time
from pathlib import Path

# 确保当前项目根目录在 sys.path 中（便于导入 m1_pipeline 模块）
_project_root = Path(__file__).parent.resolve()
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from m1_pipeline import M1DataPipeline

# ==================== 路径配置 ====================
# 输入路径：week1/data/UserBehavior.parquet
INPUT_PATH = str(_project_root / "data" / "UserBehavior.parquet")

# 输出路径：week1/data/m1_final_clean.parquet
OUTPUT_PATH = str(_project_root / "data" / "m1_final_clean.parquet")


def run_pipeline(input_path: str = INPUT_PATH, output_path: str = OUTPUT_PATH) -> dict:
    """执行 M1 数据处理管道全流程。

    本函数封装了管道初始化、执行、异常处理与结果返回的完整流程，
    可供外部模块直接调用。

    参数：
        input_path:  输入 Parquet 文件路径（默认使用模块级 INPUT_PATH）
        output_path: 输出 Parquet 文件路径（默认使用模块级 OUTPUT_PATH）

    返回：
        包含管道执行结果的字典，结构如下：
            {
                "extract": {...},        # extract() 阶段统计
                "transform": {...},      # transform() 阶段统计
                "load": {...},           # load() 阶段统计
                "total_elapsed_sec": 0.0 # 总耗时（秒）
            }

    异常：
        FileNotFoundError: 输入文件不存在
        ValueError:        路径配置错误
        RuntimeError:      管道执行失败
    """
    logger = logging.getLogger(__name__)

    # 打印启动信息
    logger.info("=" * 70)
    logger.info("  M1 数据处理管道 - 主程序启动")
    logger.info("=" * 70)
    logger.info("输入路径: %s", input_path)
    logger.info("输出路径: %s", output_path)
    logger.info("")

    # 初始化管道
    pipeline = M1DataPipeline(
        input_path=input_path,
        output_path=output_path,
    )

    # 执行全流程并返回结果
    return pipeline.run()


def main() -> None:
    """主函数：初始化管道、执行全流程、输出 Benchmark 报告。

    本函数作为脚本入口点，负责：
      1. 配置日志系统
      2. 调用 run_pipeline() 执行管道
      3. 打印最终汇总与性能基准测试报告
      4. 捕获并处理所有异常
    """
    # 获取模块日志记录器
    logger = logging.getLogger(__name__)

    # 记录整体开始时间（Benchmark）
    benchmark_start = time.perf_counter()

    try:
        # 执行管道全流程
        result = run_pipeline()

        # 计算总耗时
        total_elapsed = time.perf_counter() - benchmark_start

        # 打印最终汇总报告
        logger.info("")
        logger.info("=" * 70)
        logger.info("  执行结果汇总")
        logger.info("=" * 70)

        # 性能基准测试报告
        logger.info("--- 性能基准测试 (Benchmark) ---")
        logger.info("  总耗时: %.2f 秒 (%.2f 分钟)", total_elapsed, total_elapsed / 60)
        logger.info("  extract()   阶段: 详见上方日志")
        logger.info("  transform() 阶段: 详见上方日志")
        logger.info("  load()      阶段: 详见上方日志")

        # 数据结果汇总
        logger.info("--- 数据结果汇总 ---")
        logger.info("  输入文件: %s", result["load"]["output_path"])
        logger.info("  输出文件: %s", result["load"]["output_path"])
        logger.info("  输出大小: %.2f MB", result["load"]["file_size_mb"])
        logger.info("  最终行数: %s", f"{result['load']['after_funnel']:,}")

        # 数据压缩率
        before = result["load"]["before_dedup"]
        after = result["load"]["after_funnel"]
        compression_rate = (1 - after / before) * 100 if before > 0 else 0
        logger.info("  数据压缩率: %.2f%% (从 %s 行到 %s 行)",
                     compression_rate, f"{before:,}", f"{after:,}")

        logger.info("=" * 70)
        logger.info("✓ 管道执行成功！")
        logger.info("=" * 70)

    except FileNotFoundError as e:
        logger.error("文件未找到: %s", e)
        logger.error("请检查输入路径是否正确: %s", INPUT_PATH)
        sys.exit(1)

    except ValueError as e:
        logger.error("路径配置错误: %s", e)
        logger.error("请检查 INPUT_PATH 和 OUTPUT_PATH 配置")
        sys.exit(1)

    except RuntimeError as e:
        logger.error("管道运行时错误: %s", e)
        sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("用户中断执行 (Ctrl+C)")
        sys.exit(130)

    except Exception as e:
        logger.error("管道执行失败: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
