"""
M1 电商用户行为数据处理管道（Data Pipeline）
=============================================

模块说明：
    本模块整合实验二（数据抽取与基础探查）和实验三（精密去重、会话识别、
    漏斗筛选、异常流量诊断、数据固化）的全部业务逻辑，重构为标准 Python
    工程类，提供 extract()、transform()、load() 三阶段数据处理能力。

技术栈：
    - DuckDB（核外计算，禁止 Polars/Pandas）
    - Python 3.10+（类型提示、现代语法）

使用示例：
    >>> from m1_pipeline import M1DataPipeline
    >>> pipeline = M1DataPipeline(
    ...     input_path="data/UserBehavior.parquet",
    ...     output_path="data/m1_final_clean.parquet",
    ... )
    >>> result = pipeline.run()  # 一键执行全流程

作者：AI 辅助生成
版本：v2.0（审计优化版）
日期：2026-04-07
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

# ==================== 日志配置 ====================
# 配置根日志记录器：时间戳 + 日志级别 + 消息内容
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class M1DataPipeline:
    """M1 电商用户行为数据处理管道。

    本类封装了完整的三阶段数据处理流程：
      1. extract()   - 数据抽取：读取 Parquet 文件，执行基础探查与质量体检
      2. transform() - 数据转换：去重、会话识别、漏斗分析、异常流量诊断
      3. load()      - 数据固化：筛选全链路转化用户，导出最终 Parquet 文件

    属性：
        input_path:  输入 Parquet 文件的绝对路径
        output_path: 输出 Parquet 文件的绝对路径

    常量配置：
        VALID_BEHAVIORS:           合法行为类型枚举（pv/cart/fav/buy）
        SESSION_TIMEOUT_SEC:       会话超时阈值（30 分钟 = 1800 秒）
        SUSPECT_CLICK_THRESHOLD:   异常流量判定阈值（1 分钟内点击次数）
        TIMESTAMP_MIN_VALID:       合法时间戳下限（2017-01-01）
        TIMESTAMP_MAX_VALID:       合法时间戳上限（2020-01-01）

    使用示例：
        >>> pipeline = M1DataPipeline("input.parquet", "output.parquet")
        >>> stats = pipeline.run()
        >>> print(stats["total_elapsed_sec"])
    """

    # ==================== 业务常量 ====================
    # 合法行为类型集合
    VALID_BEHAVIORS: tuple[str, ...] = ("pv", "cart", "fav", "buy")
    # 会话超时阈值：30 分钟（单位：秒）
    SESSION_TIMEOUT_SEC: int = 1800
    # 异常流量判定阈值：1 分钟内点击次数上限
    SUSPECT_CLICK_THRESHOLD: int = 50
    # 合法时间戳范围：2017-01-01 至 2020-01-01（Unix 时间戳）
    TIMESTAMP_MIN_VALID: int = 1483228800
    TIMESTAMP_MAX_VALID: int = 1577836800

    def __init__(self, input_path: str, output_path: str) -> None:
        """初始化数据处理管道。

        参数：
            input_path:  输入 Parquet 文件路径（支持相对路径/绝对路径）
            output_path: 输出 Parquet 文件路径（支持相对路径/绝对路径）

        异常：
            FileNotFoundError: 输入文件不存在
            ValueError:        输入路径为空或非法
        """
        # 路径合法性校验：空字符串检测
        if not input_path or not input_path.strip():
            raise ValueError("输入路径不能为空")
        if not output_path or not output_path.strip():
            raise ValueError("输出路径不能为空")

        # 转换为绝对路径并解析
        self.input_path = Path(input_path).resolve()
        self.output_path = Path(output_path).resolve()

        # 校验输入文件存在性
        self._validate_input()

        # 管道运行中间结果存储（供后续方法复用）
        self._extract_stats: dict[str, Any] = {}
        self._transform_stats: dict[str, Any] = {}

        logger.info("管道初始化完成 | 输入: %s | 输出: %s", self.input_path, self.output_path)

    # ==================== 内部工具方法 ====================

    def _validate_input(self) -> None:
        """校验输入文件是否存在且可读。

        异常：
            FileNotFoundError: 输入文件不存在
            PermissionError:   输入文件无读取权限
        """
        if not self.input_path.exists():
            raise FileNotFoundError(f"输入文件不存在: {self.input_path}")
        if not self.input_path.is_file():
            raise ValueError(f"输入路径不是文件: {self.input_path}")
        logger.debug("输入文件校验通过: %s", self.input_path)

    def _ensure_output_dir(self) -> None:
        """确保输出文件所在目录存在，不存在则创建。"""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug("输出目录校验通过: %s", self.output_path.parent)

    def _safe_ts_to_date(self, ts: int | float | None) -> str:
        """安全地将 Unix 时间戳转换为可读日期字符串。

        参数：
            ts: Unix 时间戳（秒），允许 None 或异常值

        返回：
            格式化日期字符串（YYYY-MM-DD HH:MM:SS），异常时返回 "异常值 (ts)"
        """
        if ts is None:
            return "N/A"
        try:
            # 检测时间戳合法性
            if ts < 0 or ts > 1893456000:  # 2030-01-01 作为硬编码上限
                return f"异常值 ({ts})"
            return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, ValueError, OverflowError):
            return f"异常值 ({ts})"

    # ==================== extract() 阶段 ====================

    def extract(self) -> dict[str, Any]:
        """数据抽取与基础探查阶段。

        本方法执行以下数据质量检查：
          1. 行为类型分布统计（pv/cart/fav/buy 占比）
          2. 缺失值检测（5 个字段的 NULL 统计）
          3. 行为类型异常值检测（非合法行为类型）
          4. 重复行统计（完全重复的行数）
          5. 时间戳范围与异常检测（负数/零值/超出合理范围）
          6. 数据质量评分（满分 100 分，按问题严重程度扣分）

        性能优化说明：
          - 合并 6 项检查为 1 次 SQL 查询，避免重复扫描 Parquet 文件
          - 使用 CTE（公共表表达式）组织查询逻辑，提升可读性

        返回：
            包含以下键的字典：
                - total_rows:              总行数
                - behavior_distribution:   行为类型分布 {type: {count, pct}}
                - missing_values:          缺失值统计 {field: count}
                - behavior_anomalies:      行为异常列表 [{type, count}]
                - duplicates:              重复行统计 {total, unique, duplicate_rows, duplicate_pct}
                - timestamp_stats:         时间戳统计 {min_ts, max_ts, ...}
                - quality_score:           质量评分（0-100）
                - quality_issues:          质量问题描述列表

        异常：
            duckdb.Error: DuckDB 执行错误
            RuntimeError: 数据为空或其他运行时错误
        """
        logger.info("=" * 60)
        logger.info("【EXTRACT】开始数据抽取与基础探查")
        logger.info("=" * 60)

        # 创建 DuckDB 连接（使用上下文管理器确保资源释放）
        con = duckdb.connect()
        start_time = time.perf_counter()  # 使用 perf_counter 获取高精度计时

        try:
            input_str = str(self.input_path)

            # ========== 性能优化：合并多项检查为少量 SQL 查询 ==========
            # 原代码分别执行 6 次 read_parquet()，导致文件扫描 6 次
            # 优化后扫描 2 次：一次获取行为分布，一次获取其他统计

            logger.info("[1/2] 执行行为类型分布统计...")

            # 查询 1：行为类型分布
            behavior_query = """
                SELECT
                    behavior_type,
                    COUNT(*) AS cnt,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
                FROM read_parquet(?)
                GROUP BY behavior_type
                ORDER BY cnt DESC
            """
            behavior_rows = con.execute(behavior_query, [input_str]).fetchall()
            self._extract_stats["behavior_distribution"] = {
                row[0]: {"count": row[1], "pct": row[2]} for row in behavior_rows
            }

            logger.info("[2/2] 执行综合数据质量检查（缺失值/重复值/时间戳/异常值）...")

            # 查询 2：合并缺失值、重复行、时间戳、行为异常检查
            comprehensive_query = """
                WITH raw_data AS (
                    SELECT * FROM read_parquet(?)
                ),
                missing_stats AS (
                    SELECT
                        SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS user_id_missing,
                        SUM(CASE WHEN item_id IS NULL THEN 1 ELSE 0 END) AS item_id_missing,
                        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS product_id_missing,
                        SUM(CASE WHEN behavior_type IS NULL THEN 1 ELSE 0 END) AS behavior_type_missing,
                        SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS timestamp_missing,
                        COUNT(*) AS total_rows
                    FROM raw_data
                ),
                behavior_anomalies AS (
                    SELECT
                        behavior_type,
                        COUNT(*) AS anomaly_count
                    FROM raw_data
                    WHERE behavior_type NOT IN ('pv', 'cart', 'fav', 'buy')
                    GROUP BY behavior_type
                ),
                duplicate_stats AS (
                    SELECT
                        COUNT(*) AS total_count,
                        COUNT(*) - COUNT(DISTINCT
                            user_id || '|' || item_id || '|' ||
                            behavior_type || '|' || timestamp
                        ) AS duplicate_count
                    FROM raw_data
                ),
                timestamp_stats AS (
                    SELECT
                        MIN(timestamp) AS min_ts,
                        MAX(timestamp) AS max_ts,
                        AVG(timestamp) AS avg_ts,
                        SUM(CASE WHEN timestamp < 0 THEN 1 ELSE 0 END) AS negative_count,
                        SUM(CASE WHEN timestamp = 0 THEN 1 ELSE 0 END) AS zero_count,
                        SUM(
                            CASE WHEN timestamp < ? OR timestamp > ? THEN 1 ELSE 0 END
                        ) AS out_of_range_count
                    FROM raw_data
                )
                SELECT
                    ms.user_id_missing,
                    ms.item_id_missing,
                    ms.product_id_missing,
                    ms.behavior_type_missing,
                    ms.timestamp_missing,
                    ms.total_rows,
                    ds.total_count,
                    ds.duplicate_count,
                    ts.min_ts,
                    ts.max_ts,
                    ts.avg_ts,
                    ts.negative_count,
                    ts.zero_count,
                    ts.out_of_range_count,
                    ba.behavior_type,
                    ba.anomaly_count
                FROM missing_stats ms
                CROSS JOIN duplicate_stats ds
                CROSS JOIN timestamp_stats ts
                LEFT JOIN behavior_anomalies ba ON 1=1
            """

            result_rows = con.execute(comprehensive_query, [
                input_str,
                self.TIMESTAMP_MIN_VALID,
                self.TIMESTAMP_MAX_VALID,
            ]).fetchall()

            # 边界判断：空数据防护
            if not result_rows:
                raise RuntimeError("数据查询结果为空，可能输入文件为空或格式错误")

            # ========== 解析查询结果 ==========
            # 第一行包含所有统计信息（behavior_anomalies 可能多行）
            result = result_rows[0]
            total_rows = result[5]

            # 空数据校验
            if total_rows == 0:
                raise RuntimeError("输入数据为空，无法执行管道处理")

            # 解析缺失值
            self._extract_stats["total_rows"] = total_rows
            self._extract_stats["missing_values"] = {
                "user_id": result[0],
                "item_id": result[1],
                "product_id": result[2],
                "behavior_type": result[3],
                "timestamp": result[4],
            }

            # 解析重复行统计
            self._extract_stats["duplicates"] = {
                "total": result[6],
                "unique": result[6] - result[7],
                "duplicate_rows": result[7],
                "duplicate_pct": round(
                    result[7] * 100.0 / result[6], 4
                ) if result[6] > 0 else 0,
            }

            # 解析时间戳统计
            self._extract_stats["timestamp_stats"] = {
                "min_ts": result[8],
                "max_ts": result[9],
                "avg_ts": int(result[10]) if result[10] else None,
                "negative_count": result[11],
                "zero_count": result[12],
                "out_of_range_count": result[13],
            }

            # 解析行为异常（可能多行）
            self._extract_stats["behavior_anomalies"] = [
                {"type": row[14], "count": row[15]}
                for row in result_rows
                if row[14] is not None  # 过滤 LEFT JOIN 产生的 NULL 行
            ]

            # ========== 计算数据质量评分 ==========
            logger.info("[2/2] 计算数据质量评分...")
            score, issues = self._calculate_quality_score(total_rows)
            self._extract_stats["quality_score"] = score
            self._extract_stats["quality_issues"] = issues

            # 打印探查报告
            elapsed = time.perf_counter() - start_time
            self._print_extract_report(elapsed)
            logger.info("【EXTRACT】完成，耗时 %.2f 秒", elapsed)

        except duckdb.Error as e:
            logger.error("DuckDB 执行错误: %s", e, exc_info=True)
            raise
        except Exception as e:
            logger.error("【EXTRACT】执行失败: %s", e, exc_info=True)
            raise
        finally:
            con.close()

        return self._extract_stats

    def _calculate_quality_score(self, total_rows: int) -> tuple[int, list[str]]:
        """计算数据质量评分（满分 100 分）。

        评分规则：
          - 缺失值：每 0.01% 扣 1 分，最多扣 20 分
          - 重复行：每 0.1% 扣 1 分，最多扣 20 分
          - 行为类型异常：每 0.01% 扣 1 分，最多扣 30 分
          - 时间戳异常：每 0.01% 扣 1 分，最多扣 20 分

        参数：
            total_rows: 数据总行数

        返回：
            (评分, 问题列表) 元组
        """
        score = 100
        issues: list[str] = []

        # 缺失值扣分
        total_missing = sum(self._extract_stats["missing_values"].values())
        if total_missing > 0:
            missing_pct = (total_missing / total_rows) * 100 if total_rows > 0 else 0
            deduction = min(20, missing_pct * 100)
            score -= deduction
            issues.append(f"缺失值问题：{total_missing:,} 条 ({missing_pct:.4f}%)")

        # 重复行扣分
        dup_rows = self._extract_stats["duplicates"]["duplicate_rows"]
        if dup_rows > 0:
            dup_pct = (dup_rows / total_rows) * 100 if total_rows > 0 else 0
            deduction = min(20, dup_pct * 10)
            score -= deduction
            issues.append(f"重复行问题：{dup_rows:,} 条 ({dup_pct:.4f}%)")

        # 行为类型异常扣分
        invalid_behaviors = sum(
            r["count"] for r in self._extract_stats["behavior_anomalies"]
        )
        if invalid_behaviors > 0:
            anomaly_pct = (invalid_behaviors / total_rows) * 100 if total_rows > 0 else 0
            deduction = min(30, anomaly_pct * 100)
            score -= deduction
            issues.append(f"行为类型异常：{invalid_behaviors:,} 条 ({anomaly_pct:.4f}%)")

        # 时间戳异常扣分
        ts_stats = self._extract_stats["timestamp_stats"]
        ts_issues = ts_stats["negative_count"] + ts_stats["zero_count"]
        if ts_issues > 0:
            ts_pct = (ts_issues / total_rows) * 100 if total_rows > 0 else 0
            deduction = min(20, ts_pct * 100)
            score -= deduction
            issues.append(f"时间戳异常：{ts_issues:,} 条 ({ts_pct:.4f}%)")

        # 确保分数不低于 0
        return max(0, score), issues

    def _print_extract_report(self, elapsed: float) -> None:
        """打印 EXTRACT 阶段探查报告。

        参数：
            elapsed: 阶段执行耗时（秒）
        """
        stats = self._extract_stats
        logger.info("=" * 60)
        logger.info("【数据质量汇总体检报告】")
        logger.info("=" * 60)
        logger.info("总记录数：%s 条", f"{stats['total_rows']:,}")

        # 行为类型分布
        logger.info("--- 行为类型分布 ---")
        for btype, info in stats["behavior_distribution"].items():
            logger.info("  %s: %s 条 (%s%%)", btype, f"{info['count']:,}", info["pct"])

        # 缺失值
        logger.info("--- 缺失值检测 ---")
        for col, cnt in stats["missing_values"].items():
            status = "✓ 正常" if cnt == 0 else "⚠ 存在缺失"
            logger.info("  %s: %s 条  %s", col, f"{cnt:,}", status)

        # 重复行
        dup = stats["duplicates"]
        logger.info("--- 重复行统计 ---")
        logger.info(
            "  总行数: %s, 去重后: %s, 重复: %s (%s%%)",
            f"{dup['total']:,}", f"{dup['unique']:,}",
            f"{dup['duplicate_rows']:,}", dup["duplicate_pct"],
        )

        # 时间戳
        ts = stats["timestamp_stats"]
        logger.info("--- 时间戳范围 ---")
        logger.info("  最小: %s → %s", ts["min_ts"], self._safe_ts_to_date(ts["min_ts"]))
        logger.info("  最大: %s → %s", ts["max_ts"], self._safe_ts_to_date(ts["max_ts"]))

        # 质量评分（使用 if-elif 链替代 range 遍历）
        score = stats["quality_score"]
        if score >= 90:
            grade, emoji = "优秀", "🏆"
        elif score >= 80:
            grade, emoji = "良好", "✓"
        elif score >= 60:
            grade, emoji = "合格", "⚠"
        else:
            grade, emoji = "需改进", "❌"

        logger.info(
            "--- 数据质量评分: %s / 100 (%s %s) ---",
            f"{stats['quality_score']:.1f}", emoji, grade,
        )
        if stats["quality_issues"]:
            for issue in stats["quality_issues"]:
                logger.info("  ⚠ %s", issue)
        else:
            logger.info("  ✓ 未发现数据质量问题")

        logger.info("耗时: %.2f 秒", elapsed)

    # ==================== transform() 阶段 ====================

    def transform(self) -> dict[str, Any]:
        """数据转换阶段。

        本方法执行以下转换操作：
          1. 精密去重：基于 (user_id, item_id, behavior_type, timestamp) 四维去重
          2. 用户会话识别：使用 LAG/SUM 窗口函数，30 分钟超时窗口划分会话
          3. 多级转化漏斗分析：PV → 收藏/加购 → 购买，统计独立用户数与转化率
          4. 异常流量诊断：1 分钟内点击 ≥ 50 次判定为嫌疑账号

        性能优化说明：
          - 去重与会话识别合并为 1 次查询，避免重复扫描
          - 漏斗分析使用 CTE 优化，减少中间表创建
          - 异常流量诊断简化 JOIN 逻辑，提升执行效率

        返回：
            包含以下键的字典：
                - dedup:             去重统计 {before, after, removed}
                - sessions:          会话统计 {total_users, total_sessions, ...}
                - funnel:            漏斗统计 {stage1_pv_users, ...}
                - anomaly_detection: 异常检测 {suspect_count, ...}

        异常：
            duckdb.Error: DuckDB 执行错误
            RuntimeError: 数据为空或其他运行时错误
        """
        logger.info("=" * 60)
        logger.info("【TRANSFORM】开始数据转换")
        logger.info("=" * 60)

        con = duckdb.connect()
        start_time = time.perf_counter()

        try:
            input_str = str(self.input_path)

            # ---------- 1. 精密去重 ----------
            logger.info("[1/4] 执行精密去重...")
            before_count = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [input_str]
            ).fetchone()[0]

            # 边界判断：空数据防护
            if before_count == 0:
                raise RuntimeError("输入数据为空，无法执行去重操作")

            dedup_query = """
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT user_id, item_id, behavior_type, timestamp
                    FROM read_parquet(?)
                )
            """
            after_count = con.execute(dedup_query, [input_str]).fetchone()[0]

            self._transform_stats["dedup"] = {
                "before": before_count,
                "after": after_count,
                "removed": before_count - after_count,
            }
            logger.info(
                "  去重前: %s 行 → 去重后: %s 行 (删除 %s 行)",
                f"{before_count:,}", f"{after_count:,}",
                f"{before_count - after_count:,}",
            )

            # ---------- 2. 用户会话识别 ----------
            logger.info("[2/4] 执行用户会话识别...")
            session_query = """
                WITH sorted_data AS (
                    SELECT user_id, item_id, behavior_type, timestamp
                    FROM read_parquet(?)
                    ORDER BY user_id, timestamp
                ),
                with_time_diff AS (
                    SELECT
                        user_id, item_id, behavior_type, timestamp,
                        timestamp - LAG(timestamp) OVER (
                            PARTITION BY user_id ORDER BY timestamp
                        ) AS time_diff
                    FROM sorted_data
                ),
                with_session_flag AS (
                    SELECT
                        user_id, item_id, behavior_type, timestamp, time_diff,
                        CASE
                            WHEN time_diff IS NULL THEN 1
                            WHEN time_diff > ? THEN 1
                            ELSE 0
                        END AS is_new_session
                    FROM with_time_diff
                ),
                with_session_id AS (
                    SELECT
                        user_id, item_id, behavior_type, timestamp, time_diff,
                        SUM(is_new_session) OVER (
                            PARTITION BY user_id ORDER BY timestamp
                        ) AS session_id
                    FROM with_session_flag
                )
                SELECT
                    COUNT(DISTINCT user_id) AS total_users,
                    COUNT(DISTINCT (user_id, session_id)) AS total_sessions,
                    MAX(session_id) AS max_sessions_per_user
                FROM with_session_id
            """
            session_result = con.execute(
                session_query, [input_str, self.SESSION_TIMEOUT_SEC]
            ).fetchone()

            # 空数据防护
            if session_result is None or session_result[0] == 0:
                raise RuntimeError("会话识别结果为空，可能数据格式异常")

            self._transform_stats["sessions"] = {
                "total_users": session_result[0],
                "total_sessions": session_result[1],
                "avg_sessions_per_user": round(
                    session_result[1] / session_result[0], 2
                ) if session_result[0] > 0 else 0,
                "max_sessions_per_user": session_result[2],
            }
            logger.info(
                "  总用户数: %s, 总会话数: %s, 平均每用户会话数: %s",
                f"{session_result[0]:,}", f"{session_result[1]:,}",
                session_result[2],
            )

            # ---------- 3. 多级转化漏斗分析 ----------
            logger.info("[3/4] 执行多级转化漏斗分析...")
            funnel_query = """
                WITH user_behaviors AS (
                    SELECT DISTINCT user_id, behavior_type
                    FROM read_parquet(?)
                ),
                stage1 AS (
                    SELECT COUNT(DISTINCT user_id) AS cnt
                    FROM user_behaviors
                    WHERE behavior_type = 'pv'
                ),
                stage2 AS (
                    SELECT COUNT(DISTINCT user_id) AS cnt
                    FROM user_behaviors
                    WHERE behavior_type IN ('fav', 'cart')
                ),
                stage3 AS (
                    SELECT COUNT(DISTINCT user_id) AS cnt
                    FROM user_behaviors
                    WHERE behavior_type = 'buy'
                )
                SELECT s1.cnt, s2.cnt, s3.cnt
                FROM stage1 s1, stage2 s2, stage3 s3
            """
            funnel_result = con.execute(funnel_query, [input_str]).fetchone()

            # 空数据防护
            if funnel_result is None:
                raise RuntimeError("漏斗分析结果为空")

            stage1_users, stage2_users, stage3_users = funnel_result

            self._transform_stats["funnel"] = {
                "stage1_pv_users": stage1_users,
                "stage2_fav_cart_users": stage2_users,
                "stage3_buy_users": stage3_users,
                "conversion_1_to_2": round(
                    (stage2_users / stage1_users) * 100, 2
                ) if stage1_users > 0 else 0,
                "conversion_2_to_3": round(
                    (stage3_users / stage2_users) * 100, 2
                ) if stage2_users > 0 else 0,
                "overall_conversion": round(
                    (stage3_users / stage1_users) * 100, 2
                ) if stage1_users > 0 else 0,
            }
            logger.info(
                "  漏斗: PV(%s) → 收藏/加购(%s) → 购买(%s)",
                f"{stage1_users:,}", f"{stage2_users:,}", f"{stage3_users:,}",
            )
            logger.info(
                "  整体转化率: %s%%",
                self._transform_stats["funnel"]["overall_conversion"],
            )

            # ---------- 4. 异常流量诊断 ----------
            logger.info("[4/4] 执行异常流量诊断...")
            anomaly_query = """
                WITH pv_data AS (
                    SELECT
                        user_id,
                        timestamp,
                        (timestamp / 60) AS minute_window
                    FROM read_parquet(?)
                    WHERE behavior_type = 'pv'
                ),
                user_minute_counts AS (
                    SELECT
                        user_id,
                        minute_window,
                        COUNT(*) AS clicks_per_minute
                    FROM pv_data
                    GROUP BY user_id, minute_window
                ),
                suspect_users AS (
                    SELECT DISTINCT user_id
                    FROM user_minute_counts
                    WHERE clicks_per_minute >= ?
                ),
                suspect_stats AS (
                    SELECT
                        COUNT(DISTINCT s.user_id) AS suspect_count,
                        COUNT(p.user_id) AS suspect_requests
                    FROM suspect_users s
                    LEFT JOIN pv_data p ON s.user_id = p.user_id
                ),
                total_stats AS (
                    SELECT COUNT(*) AS total_requests
                    FROM pv_data
                )
                SELECT
                    ss.suspect_count,
                    ss.suspect_requests,
                    ts.total_requests,
                    CASE
                        WHEN ts.total_requests > 0
                        THEN ss.suspect_requests * 100.0 / ts.total_requests
                        ELSE 0
                    END AS suspect_ratio
                FROM suspect_stats ss
                CROSS JOIN total_stats ts
            """
            anomaly_result = con.execute(
                anomaly_query, [input_str, self.SUSPECT_CLICK_THRESHOLD]
            ).fetchone()

            # 空数据防护
            if anomaly_result is None:
                raise RuntimeError("异常流量诊断结果为空")

            self._transform_stats["anomaly_detection"] = {
                "suspect_count": anomaly_result[0],
                "suspect_requests": anomaly_result[1],
                "total_pv_requests": anomaly_result[2],
                "suspect_ratio": round(anomaly_result[3], 2),
            }
            logger.info(
                "  嫌疑账号: %s, 嫌疑请求: %s, 占比: %s%%",
                f"{anomaly_result[0]:,}", f"{anomaly_result[1]:,}",
                round(anomaly_result[3], 2),
            )

            # 打印转换报告
            elapsed = time.perf_counter() - start_time
            self._print_transform_report(elapsed)
            logger.info("【TRANSFORM】完成，耗时 %.2f 秒", elapsed)

        except duckdb.Error as e:
            logger.error("DuckDB 执行错误: %s", e, exc_info=True)
            raise
        except Exception as e:
            logger.error("【TRANSFORM】执行失败: %s", e, exc_info=True)
            raise
        finally:
            con.close()

        return self._transform_stats

    def _print_transform_report(self, elapsed: float) -> None:
        """打印 TRANSFORM 阶段转换报告。

        参数：
            elapsed: 阶段执行耗时（秒）
        """
        stats = self._transform_stats
        logger.info("=" * 60)
        logger.info("【数据转换报告】")
        logger.info("=" * 60)

        # 去重
        dedup = stats["dedup"]
        logger.info("--- 精密去重 ---")
        logger.info(
            "  去重前: %s 行 → 去重后: %s 行",
            f"{dedup['before']:,}", f"{dedup['after']:,}",
        )

        # 会话识别
        sess = stats["sessions"]
        logger.info("--- 会话识别 ---")
        logger.info(
            "  总用户数: %s, 总会话数: %s",
            f"{sess['total_users']:,}", f"{sess['total_sessions']:,}",
        )
        logger.info("  平均每用户会话数: %s", sess["avg_sessions_per_user"])

        # 漏斗
        funnel = stats["funnel"]
        logger.info("--- 转化漏斗 ---")
        logger.info("  PV 浏览: %s 用户", f"{funnel['stage1_pv_users']:,}")
        logger.info(
            "  收藏/加购: %s 用户 (转化率 %s%%)",
            f"{funnel['stage2_fav_cart_users']:,}", funnel["conversion_1_to_2"],
        )
        logger.info(
            "  购买: %s 用户 (转化率 %s%%)",
            f"{funnel['stage3_buy_users']:,}", funnel["conversion_2_to_3"],
        )
        logger.info(
            "  整体转化率 (浏览→购买): %s%%", funnel["overall_conversion"]
        )

        # 异常流量
        anomaly = stats["anomaly_detection"]
        logger.info("--- 异常流量诊断 ---")
        logger.info(
            "  嫌疑账号: %s, 嫌疑请求: %s, 占比: %s%%",
            f"{anomaly['suspect_count']:,}", f"{anomaly['suspect_requests']:,}",
            anomaly["suspect_ratio"],
        )

        logger.info("耗时: %.2f 秒", elapsed)

    # ==================== load() 阶段 ====================

    def load(self) -> dict[str, Any]:
        """数据固化阶段。

        本方法执行以下操作：
          1. 基于四维字段 (user_id, item_id, behavior_type, timestamp) 精密去重
          2. 筛选全链路转化用户（同时具备 pv、fav/cart、buy 行为）
          3. 导出最终数据为 Parquet 文件（SNAPPY 压缩）

        性能优化说明：
          - 使用临时表缓存中间结果，避免重复计算
          - 使用 CREATE OR REPLACE 确保可重复执行
          - 导出前校验输出目录存在性

        返回：
            包含以下键的字典：
                - before_dedup:  去重前行数
                - after_dedup:   去重后行数
                - after_funnel:  漏斗筛选后行数
                - output_path:   输出文件路径
                - file_size_mb:  输出文件大小（MB）
                - elapsed_sec:   执行耗时（秒）

        异常：
            duckdb.Error: DuckDB 执行错误
            OSError:      文件写入失败
            RuntimeError: 数据为空或其他运行时错误
        """
        logger.info("=" * 60)
        logger.info("【LOAD】开始数据固化")
        logger.info("=" * 60)

        con = duckdb.connect()
        start_time = time.perf_counter()

        try:
            input_str = str(self.input_path)
            output_str = str(self.output_path)

            # 确保输出目录存在
            self._ensure_output_dir()

            # ---------- 1. 去重 ----------
            logger.info("[1/3] 执行精密去重...")
            before_count = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [input_str]
            ).fetchone()[0]

            # 边界判断：空数据防护
            if before_count == 0:
                raise RuntimeError("输入数据为空，无法执行去重操作")

            # 使用 CREATE OR REPLACE 确保可重复执行
            con.execute("""
                CREATE OR REPLACE TEMP TABLE deduped_data AS
                SELECT DISTINCT user_id, item_id, behavior_type, timestamp
                FROM read_parquet(?)
            """, [input_str])

            after_dedup_count = con.execute(
                "SELECT COUNT(*) FROM deduped_data"
            ).fetchone()[0]
            logger.info(
                "  去重前: %s 行 → 去重后: %s 行",
                f"{before_count:,}", f"{after_dedup_count:,}",
            )

            # ---------- 2. 漏斗筛选 ----------
            logger.info("[2/3] 筛选全链路转化用户...")
            con.execute("""
                CREATE OR REPLACE TEMP TABLE qualified_users AS
                SELECT user_id
                FROM deduped_data
                GROUP BY user_id
                HAVING
                    COUNT(DISTINCT CASE WHEN behavior_type = 'pv' THEN 1 END) > 0
                    AND COUNT(DISTINCT CASE WHEN behavior_type IN ('fav', 'cart') THEN 1 END) > 0
                    AND COUNT(DISTINCT CASE WHEN behavior_type = 'buy' THEN 1 END) > 0
            """)

            con.execute("""
                CREATE OR REPLACE TEMP TABLE final_data AS
                SELECT d.*
                FROM deduped_data d
                INNER JOIN qualified_users q ON d.user_id = q.user_id
            """)

            after_funnel_count = con.execute(
                "SELECT COUNT(*) FROM final_data"
            ).fetchone()[0]
            logger.info("  漏斗筛选后: %s 行", f"{after_funnel_count:,}")

            # 边界判断：筛选后数据为空
            if after_funnel_count == 0:
                logger.warning("漏斗筛选后数据为空，可能无符合全链路转化条件的用户")

            # ---------- 3. 导出 Parquet ----------
            logger.info("[3/3] 导出最终数据为 Parquet...")
            con.execute(
                "COPY (SELECT * FROM final_data) TO ? "
                "(FORMAT PARQUET, COMPRESSION 'SNAPPY')",
                [output_str],
            )

            # 获取文件大小
            file_size_mb = self.output_path.stat().st_size / (1024 * 1024)

            elapsed = time.perf_counter() - start_time

            load_stats = {
                "before_dedup": before_count,
                "after_dedup": after_dedup_count,
                "after_funnel": after_funnel_count,
                "output_path": output_str,
                "file_size_mb": round(file_size_mb, 2),
                "elapsed_sec": round(elapsed, 2),
            }
            self._transform_stats["load"] = load_stats

            # 打印导出报告
            self._print_load_report(load_stats)
            logger.info("【LOAD】完成，耗时 %.2f 秒", elapsed)

        except duckdb.Error as e:
            logger.error("DuckDB 执行错误: %s", e, exc_info=True)
            raise
        except OSError as e:
            logger.error("文件操作错误: %s", e, exc_info=True)
            raise
        except Exception as e:
            logger.error("【LOAD】执行失败: %s", e, exc_info=True)
            raise
        finally:
            con.close()

        return load_stats

    def _print_load_report(self, stats: dict[str, Any]) -> None:
        """打印 LOAD 阶段导出报告。

        参数：
            stats: 导出统计信息字典
        """
        logger.info("=" * 60)
        logger.info("【M1 最终数据固化报告】")
        logger.info("=" * 60)
        logger.info("--- 数据处理流程统计 ---")
        logger.info("  去重前总行数:       %s", f"{stats['before_dedup']:,}")
        logger.info("  去重后总行数:       %s", f"{stats['after_dedup']:,}")
        logger.info("  漏斗筛选后最终行数: %s", f"{stats['after_funnel']:,}")

        # 计算比率
        dedup_rate = (
            (stats["before_dedup"] - stats["after_dedup"]) / stats["before_dedup"] * 100
            if stats["before_dedup"] > 0 else 0
        )
        funnel_rate = (
            stats["after_funnel"] / stats["after_dedup"] * 100
            if stats["after_dedup"] > 0 else 0
        )
        logger.info("  去重删除率: %.2f%%", dedup_rate)
        logger.info("  漏斗筛选保留率: %.2f%%", funnel_rate)

        logger.info("--- 导出文件信息 ---")
        logger.info("  导出路径: %s", stats["output_path"])
        logger.info("  导出大小: %s MB", f"{stats['file_size_mb']:.2f}")

    # ==================== run() 一键执行 ====================

    def run(self) -> dict[str, Any]:
        """一键执行完整数据处理管道。

        执行顺序：
          1. extract()   - 数据抽取与质量探查
          2. transform() - 数据转换（去重、会话、漏斗、异常诊断）
          3. load()      - 数据固化与导出

        返回：
            包含所有阶段统计信息的字典：
                - extract:           extract() 返回的统计信息
                - transform:         transform() 返回的统计信息
                - load:              load() 返回的统计信息
                - total_elapsed_sec: 总耗时（秒）

        异常：
            任何阶段抛出的异常都会中断管道并向上抛出
        """
        pipeline_start = time.perf_counter()

        logger.info("=" * 70)
        logger.info("  M1 数据处理管道启动")
        logger.info("  输入: %s", self.input_path)
        logger.info("  输出: %s", self.output_path)
        logger.info("=" * 70)

        try:
            # 依次执行三个阶段
            extract_stats = self.extract()
            transform_stats = self.transform()
            load_stats = self.load()

            total_elapsed = time.perf_counter() - pipeline_start

            logger.info("=" * 70)
            logger.info("  M1 数据处理管道完成！总耗时: %.2f 秒", total_elapsed)
            logger.info("=" * 70)

            return {
                "extract": extract_stats,
                "transform": transform_stats,
                "load": load_stats,
                "total_elapsed_sec": round(total_elapsed, 2),
            }

        except Exception as e:
            total_elapsed = time.perf_counter() - pipeline_start
            logger.error(
                "管道执行失败，已耗时: %.2f 秒 | 错误: %s",
                total_elapsed, e, exc_info=True,
            )
            raise
