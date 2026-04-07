"""
M1 最终数据自检脚本
====================
用途：校验 m1_final_clean.parquet 的字段完整性、行数、业务指标
运行：python m1_self_check.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ==================== 路径配置 ====================
PROJECT_ROOT = Path(__file__).parent.resolve()
DATA_PATH = PROJECT_ROOT / "data" / "m1_final_clean.parquet"
REPORT_PATH = PROJECT_ROOT / "reports" / "Task5_M1_Final_Self_Check_Report.md"

# ==================== 预期业务指标 ====================
EXPECTED = {
    "total_rows": 71_818_219,
    "required_columns": {"user_id", "item_id", "behavior_type", "timestamp"},
    "valid_behaviors": {"pv", "cart", "fav", "buy"},
    "min_pv_count": 60_000_000,
    "min_buy_count": 1_000_000,
    "max_file_size_mb": 2000,
}


def check_file_exists() -> bool:
    """检查数据文件是否存在。"""
    if not DATA_PATH.exists():
        logger.error("❌ 数据文件不存在: %s", DATA_PATH)
        return False
    logger.info("✅ 数据文件存在: %s", DATA_PATH)
    return True


def check_schema(con: duckdb.DuckDBPyConnection) -> dict:
    """检查字段完整性。"""
    logger.info("--- [1/5] 字段完整性检查 ---")
    schema_rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{DATA_PATH}')"
    ).fetchall()
    actual_columns = {row[0] for row in schema_rows}

    missing = EXPECTED["required_columns"] - actual_columns
    extra = actual_columns - EXPECTED["required_columns"]

    if missing:
        logger.error("❌ 缺少字段: %s", missing)
    else:
        logger.info("✅ 必需字段完整: %s", sorted(EXPECTED["required_columns"]))

    if extra:
        logger.warning("⚠️  额外字段: %s", sorted(extra))

    return {
        "columns": sorted(actual_columns),
        "missing": sorted(missing),
        "extra": sorted(extra),
        "passed": len(missing) == 0,
    }


def check_row_count(con: duckdb.DuckDBPyConnection) -> dict:
    """检查数据行数。"""
    logger.info("--- [2/5] 数据行数检查 ---")
    count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{DATA_PATH}')"
    ).fetchone()[0]

    expected = EXPECTED["total_rows"]
    diff = abs(count - expected)
    diff_pct = (diff / expected * 100) if expected > 0 else 0

    if diff == 0:
        logger.info("✅ 行数完全匹配: %s（预期 %s）", f"{count:,}", f"{expected:,}")
    elif diff_pct < 0.01:
        logger.info("✅ 行数偏差 < 0.01%%: %s（预期 %s，偏差 %s 行）",
                     f"{count:,}", f"{expected:,}", f"{diff:,}")
    else:
        logger.warning("⚠️  行数偏差较大: %s（预期 %s，偏差 %.2f%%）",
                       f"{count:,}", f"{expected:,}", diff_pct)

    return {
        "actual": count,
        "expected": expected,
        "diff": diff,
        "diff_pct": round(diff_pct, 4),
        "passed": diff_pct < 0.01,
    }


def check_behavior_distribution(con: duckdb.DuckDBPyConnection) -> dict:
    """检查行为类型分布。"""
    logger.info("--- [3/5] 行为类型分布检查 ---")
    rows = con.execute(f"""
        SELECT behavior_type, COUNT(*) AS cnt
        FROM read_parquet('{DATA_PATH}')
        GROUP BY behavior_type
        ORDER BY cnt DESC
    """).fetchall()

    dist = {row[0]: row[1] for row in rows}
    all_behaviors = set(dist.keys())

    # 检查是否有非法行为类型
    invalid = all_behaviors - EXPECTED["valid_behaviors"]
    if invalid:
        logger.error("❌ 发现非法行为类型: %s", invalid)
    else:
        logger.info("✅ 所有行为类型均合法: %s", sorted(all_behaviors))

    # 检查 PV 和 Buy 数量
    pv_count = dist.get("pv", 0)
    buy_count = dist.get("buy", 0)

    pv_ok = pv_count >= EXPECTED["min_pv_count"]
    buy_ok = buy_count >= EXPECTED["min_buy_count"]

    if pv_ok:
        logger.info("✅ PV 数量合理: %s", f"{pv_count:,}")
    else:
        logger.warning("⚠️  PV 数量偏低: %s（预期 ≥ %s）",
                       f"{pv_count:,}", f"{EXPECTED['min_pv_count']:,}")

    if buy_ok:
        logger.info("✅ Buy 数量合理: %s", f"{buy_count:,}")
    else:
        logger.warning("⚠️  Buy 数量偏低: %s（预期 ≥ %s）",
                       f"{buy_count:,}", f"{EXPECTED['min_buy_count']:,}")

    # 漏斗逻辑检查
    funnel_ok = buy_count <= pv_count
    if funnel_ok:
        logger.info("✅ 漏斗逻辑合理: Buy(%s) ≤ PV(%s)",
                     f"{buy_count:,}", f"{pv_count:,}")
    else:
        logger.error("❌ 漏斗逻辑异常: Buy(%s) > PV(%s)",
                     f"{buy_count:,}", f"{pv_count:,}")

    # 转化率
    conversion_rate = (buy_count / pv_count * 100) if pv_count > 0 else 0
    logger.info("   浏览→购买转化率: %.2f%%", conversion_rate)

    return {
        "distribution": dist,
        "invalid_behaviors": sorted(invalid),
        "pv_count": pv_count,
        "buy_count": buy_count,
        "conversion_rate": round(conversion_rate, 2),
        "passed": len(invalid) == 0 and funnel_ok and pv_ok and buy_ok,
    }


def check_file_size() -> dict:
    """检查文件大小。"""
    logger.info("--- [4/5] 文件大小检查 ---")
    size_bytes = DATA_PATH.stat().st_size
    size_mb = size_bytes / (1024 * 1024)
    size_gb = size_mb / 1024

    max_mb = EXPECTED["max_file_size_mb"]
    if size_mb <= max_mb:
        logger.info("✅ 文件大小合理: %.2f MB (%.2f GB)", size_mb, size_gb)
    else:
        logger.warning("⚠️  文件偏大: %.2f MB（预期 ≤ %s MB）", size_mb, f"{max_mb:,}")

    return {
        "size_bytes": size_bytes,
        "size_mb": round(size_mb, 2),
        "size_gb": round(size_gb, 2),
        "passed": size_mb <= max_mb,
    }


def check_data_quality(con: duckdb.DuckDBPyConnection) -> dict:
    """检查数据质量（空值、时间戳范围）。"""
    logger.info("--- [5/5] 数据质量检查 ---")

    # 空值检查
    null_check = con.execute(f"""
        SELECT
            SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS user_id_null,
            SUM(CASE WHEN item_id IS NULL THEN 1 ELSE 0 END) AS item_id_null,
            SUM(CASE WHEN behavior_type IS NULL THEN 1 ELSE 0 END) AS behavior_null,
            SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS timestamp_null
        FROM read_parquet('{DATA_PATH}')
    """).fetchone()

    total_nulls = sum(null_check)
    if total_nulls == 0:
        logger.info("✅ 无空值：所有字段数据完整")
    else:
        logger.warning("⚠️  发现空值: user_id=%s, item_id=%s, behavior=%s, timestamp=%s",
                       null_check[0], null_check[1], null_check[2], null_check[3])

    # 时间戳范围
    ts_range = con.execute(f"""
        SELECT MIN(timestamp), MAX(timestamp)
        FROM read_parquet('{DATA_PATH}')
    """).fetchone()

    min_ts, max_ts = ts_range
    logger.info("   时间戳范围: %s ~ %s", min_ts, max_ts)

    ts_ok = min_ts is not None and max_ts is not None and max_ts > min_ts
    if ts_ok:
        logger.info("✅ 时间戳范围合理")
    else:
        logger.warning("⚠️  时间戳异常")

    return {
        "null_counts": {
            "user_id": null_check[0],
            "item_id": null_check[1],
            "behavior_type": null_check[2],
            "timestamp": null_check[3],
        },
        "total_nulls": total_nulls,
        "timestamp_min": min_ts,
        "timestamp_max": max_ts,
        "passed": total_nulls == 0 and ts_ok,
    }


def generate_report(results: dict) -> None:
    """生成自检报告 Markdown 文件。"""
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    all_passed = all(
        results[k].get("passed", False)
        for k in ["schema", "row_count", "behavior", "file_size", "data_quality"]
    )

    status_emoji = "✅ 全部通过" if all_passed else "⚠️  部分未通过"

    report = f"""# M1 最终数据自检报告

**自检日期**：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
**自检工具**：`m1_self_check.py`
**数据文件**：`{DATA_PATH}`
**自检状态**：{status_emoji}

---

## 一、自检结果汇总

| 检查项 | 状态 | 说明 |
|-------|------|------|
| 字段完整性 | {'✅ 通过' if results['schema']['passed'] else '❌ 未通过'} | 必需字段: {', '.join(results['schema']['columns'])} |
| 数据行数 | {'✅ 通过' if results['row_count']['passed'] else '⚠️ 偏差'} | 实际: {results['row_count']['actual']:,}（预期: {results['row_count']['expected']:,}） |
| 行为分布 | {'✅ 通过' if results['behavior']['passed'] else '❌ 未通过'} | PV: {results['behavior']['pv_count']:,}, Buy: {results['behavior']['buy_count']:,} |
| 文件大小 | {'✅ 通过' if results['file_size']['passed'] else '⚠️ 偏大'} | {results['file_size']['size_mb']:.2f} MB |
| 数据质量 | {'✅ 通过' if results['data_quality']['passed'] else '❌ 未通过'} | 空值: {results['data_quality']['total_nulls']:,} |

---

## 二、详细检查结果

### 2.1 字段完整性

- **必需字段**：{', '.join(sorted(EXPECTED['required_columns']))}
- **实际字段**：{', '.join(results['schema']['columns'])}
- **缺失字段**：{', '.join(results['schema']['missing']) if results['schema']['missing'] else '无'}
- **额外字段**：{', '.join(results['schema']['extra']) if results['schema']['extra'] else '无'}

### 2.2 数据行数

- **实际行数**：{results['row_count']['actual']:,}
- **预期行数**：{results['row_count']['expected']:,}
- **偏差行数**：{results['row_count']['diff']:,}
- **偏差比例**：{results['row_count']['diff_pct']:.4f}%

### 2.3 行为类型分布

| 行为类型 | 数量 | 占比 |
|---------|------|------|
"""
    total = results["row_count"]["actual"]
    for btype, count in sorted(
        results["behavior"]["distribution"].items(), key=lambda x: x[1], reverse=True
    ):
        pct = (count / total * 100) if total > 0 else 0
        report += f"| {btype} | {count:,} | {pct:.2f}% |\n"

    report += f"""
- **浏览→购买转化率**：{results['behavior']['conversion_rate']:.2f}%
- **非法行为类型**：{', '.join(results['behavior']['invalid_behaviors']) if results['behavior']['invalid_behaviors'] else '无'}

### 2.4 文件大小

- **文件大小**：{results['file_size']['size_mb']:.2f} MB（{results['file_size']['size_gb']:.2f} GB）
- **上限阈值**：{EXPECTED['max_file_size_mb']:,} MB

### 2.5 数据质量

- **空值总数**：{results['data_quality']['total_nulls']:,}
- **时间戳范围**：{results['data_quality']['timestamp_min']} ~ {results['data_quality']['timestamp_max']}

---

## 三、自检结论

{'✅ 所有检查项均通过，数据质量符合 M1 交付标准。' if all_passed else '⚠️  部分检查项未通过，请查看上方详情并修正。'}

---

**报告生成时间**：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
**自检脚本版本**：v1.0
"""

    REPORT_PATH.write_text(report, encoding="utf-8")
    logger.info("自检报告已保存: %s", REPORT_PATH)


def main() -> int:
    """主函数：执行全部自检检查。"""
    logger.info("=" * 60)
    logger.info("M1 最终数据自检开始")
    logger.info("=" * 60)

    start_time = time.perf_counter()

    # 检查文件存在
    if not check_file_exists():
        return 1

    con = duckdb.connect()
    results = {}

    try:
        results["schema"] = check_schema(con)
        results["row_count"] = check_row_count(con)
        results["behavior"] = check_behavior_distribution(con)
        results["file_size"] = check_file_size()
        results["data_quality"] = check_data_quality(con)
    finally:
        con.close()

    elapsed = time.perf_counter() - start_time
    logger.info("=" * 60)
    logger.info("自检完成，耗时 %.2f 秒", elapsed)
    logger.info("=" * 60)

    # 生成报告
    generate_report(results)

    # 返回码
    all_passed = all(r.get("passed", False) for r in results.values())
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
