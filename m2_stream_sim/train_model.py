"""
实验七任务1：离线训练与模型资产序列化（优化版）
功能：数据读取、特征工程、模型训练、评估与序列化（解决样本不平衡问题）
"""

import pandas as pd
import numpy as np
import sys
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score
from sklearn.metrics import make_scorer, accuracy_score, roc_auc_score, precision_score, recall_score, f1_score
import joblib
import os

def main():
    print("=" * 60)
    print("实验七任务1：离线训练与模型资产序列化（优化版）")
    print("=" * 60)
    print("[STAR] 优化重点：解决样本不平衡问题，提升购买行为预测能力")

    # ==================== 1. 数据读取 ====================
    print("\n[1/7] 数据读取...")
    data_path = "../week1/data/m1_final_clean.parquet"

    # 检查并尝试安装必要的parquet支持库
    try:
        import pyarrow
        print("[OK] pyarrow 已安装")
    except ImportError:
        print("[WARNING] pyarrow 未安装，正在尝试自动安装...")
        try:
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pyarrow"])
            print("[OK] pyarrow 安装成功")
            print("[INFO] pyarrow 安装需要重启Python解释器，请重新运行脚本")
            return
        except Exception as install_error:
            print(f"[FAIL] 自动安装失败: {install_error}")
            print("[INFO] 请手动运行: pip install pyarrow")
            return

    try:
        df = pd.read_parquet(data_path)
        print(f"[OK] 数据读取成功，原始数据量: {len(df)} 条")
    except Exception as e:
        print(f"[FAIL] 数据读取失败: {e}")
        print("[INFO] 请检查:")
        print("  1. 数据文件路径是否正确: {}".format(data_path))
        print("  2. 是否安装了 pyarrow: pip install pyarrow")
        return

    # 随机采样 20000 条数据用于训练
    df = df.sample(n=20000, random_state=42).reset_index(drop=True)
    print(f"[OK] 随机采样 {len(df)} 条数据用于训练")

    # ==================== 2. 标签构造 ====================
    print("\n[2/7] 标签构造...")
    # 二分类任务：behavior_type == 'buy' 标记为正样本 1，其余为 0
    df['label'] = (df['behavior_type'] == 'buy').astype(int)

    positive_samples = df['label'].sum()
    negative_samples = len(df) - positive_samples
    imbalance_ratio = negative_samples / positive_samples

    print(f"[OK] 正样本(购买)数量: {positive_samples}")
    print(f"[OK] 负样本(非购买)数量: {negative_samples}")
    print(f"[OK] 正样本占比: {positive_samples/len(df):.2%}")
    print(f"[OK] 负/正样本比例: {imbalance_ratio:.1f}:1")

    if imbalance_ratio > 10:
        print("[WARNING] 样本严重不平衡，将使用 class_weight='balanced' 进行优化")

    # ==================== 3. 特征工程优化 ====================
    print("\n[3/7] 特征工程优化...")

    # 使用特征：user_id, item_id, timestamp
    # 注意：根据数据检查，原始数据没有category_id列
    feature_cols = ['user_id', 'item_id', 'timestamp']

    # 确保特征存在
    missing_features = [col for col in feature_cols if col not in df.columns]
    if missing_features:
        print(f"[FAIL] 缺少特征列: {missing_features}")
        return

    # 从 timestamp 提取 hour、dayofweek 两个时间特征
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['hour'] = df['timestamp'].dt.hour
    df['dayofweek'] = df['timestamp'].dt.dayofweek

    # 更新特征列
    feature_cols = ['user_id', 'item_id', 'hour', 'dayofweek']

    print(f"[OK] 特征列: {feature_cols}")
    print(f"[OK] 提取时间特征: hour (0-23), dayofweek (0-6)")
    print(f"[OK] 特征数量: {len(feature_cols)}")

    # 显示特征统计信息
    print("\n特征统计:")
    print(df[feature_cols].describe())

    # 检查缺失值
    missing_values = df[feature_cols].isnull().sum()
    if missing_values.sum() > 0:
        print("\n[OK] 发现缺失值数量:")
        print(missing_values)
    else:
        print("\n[OK] 无缺失值")

    # ==================== 4. 模型Pipeline优化 ====================
    print("\n[4/7] 模型Pipeline优化（解决样本不平衡）...")

    # 封装：SimpleImputer → StandardScaler → RandomForestClassifier
    # 核心优化：添加 class_weight='balanced' 处理样本不平衡
    pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),  # 缺失值填充
        ('scaler', StandardScaler()),  # 标准化
        ('classifier', RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1,
            class_weight='balanced'  # 核心优化：平衡正负样本权重
        ))
    ])

    print("[OK] Pipeline 构建完成（优化版）:")
    print("  - SimpleImputer (缺失值填充, strategy=median)")
    print("  - StandardScaler (标准化)")
    print("  - RandomForestClassifier (n_estimators=100, max_depth=10)")
    print("  - class_weight='balanced' ([STAR] 优化：平衡样本权重)")

    # 准备训练数据
    X = df[feature_cols].values
    y = df['label'].values

    print(f"[OK] 训练数据准备完成: X shape={X.shape}, y shape={y.shape}")

    # ==================== 5. 评估指标优化（适配不平衡数据） ====================
    print("\n[5/7] 评估指标优化（5折交叉验证，适配不平衡数据）...")

    # 定义评估指标
    scoring = {
        'accuracy': 'accuracy',
        'auc': 'roc_auc',
        'precision': 'precision',
        'recall': 'recall',
        'f1': 'f1'
    }

    # 5折交叉验证
    print("[OK] 开始5折交叉验证...")
    cv_results = {}

    for metric in scoring.keys():
        cv_results[metric] = cross_val_score(pipeline, X, y, cv=5, scoring=scoring[metric])

    # 计算各指标的平均值和标准差
    metrics_summary = {}
    for metric, scores in cv_results.items():
        metrics_summary[metric] = {
            'mean': scores.mean(),
            'std': scores.std(),
            'scores': scores
        }

    print("\n[OK] 5折交叉验证结果:")
    print("=" * 50)
    print(f"指标         | 平均值    | 标准差    | 各折分数")
    print("-" * 50)
    for metric, result in metrics_summary.items():
        metric_names = {
            'accuracy': '准确率',
            'auc': 'AUC',
            'precision': '精确率',
            'recall': '召回率',
            'f1': 'F1分数'
        }
        metric_name = metric_names[metric]
        print(f"{metric_name:<10} | {result['mean']:.4f}  | {result['std']:.4f}  | {list(result['scores'])}")

    # 打印针对少数类的评估重点
    print("\n[[FLASH]] 样本不平衡优化效果评估:")
    precision = metrics_summary['precision']['mean']
    recall = metrics_summary['recall']['mean']
    f1 = metrics_summary['f1']['mean']
    auc = metrics_summary['auc']['mean']

    print(f"- 模型识别购买行为的能力:")
    print(f"  - 精确率: {precision:.4f} (预测购买中，实际购买的比例)")
    print(f"  - 召回率: {recall:.4f} (实际购买中，被识别出的比例)")
    print(f"  - F1分数: {f1:.4f} (精确率和召回率的调和平均)")
    print(f"  - AUC: {auc:.4f} (整体区分能力)")

    if recall > 0.5:
        print("[OK] 模型能较好地识别出购买行为")
    else:
        print("[WARN] 模型识别购买行为的能力有待提升")

    # ==================== 6. 模型序列化 ====================
    print("\n[6/7] 模型序列化...")

    # 在流水线上训练（使用全部数据）
    print("正在训练模型（使用全部数据）...")
    pipeline.fit(X, y)
    print("[OK] 模型训练完成（已优化样本不平衡）")

    # 保存模型
    model_path = "model.pkl"
    joblib.dump(pipeline, model_path)

    file_size = os.path.getsize(model_path) / 1024  # KB
    print(f"[OK] 模型已保存为 {model_path} (大小: {file_size:.2f} KB)")

    # ==================== 7. 模型验证 ====================
    print("\n[7/7] 模型验证...")

    # 加载模型
    loaded_pipeline = joblib.load(model_path)
    print("[OK] 模型加载成功")

    # 测试数据准备：从原数据中选取正负样本
    positive_mask = df['label'] == 1
    negative_mask = df['label'] == 0

    # 确保有正负样本
    if positive_mask.sum() >= 2 and negative_mask.sum() >= 1:
        # 2个正样本，1个负样本
        test_positive = df[positive_mask].iloc[:2]
        test_negative = df[negative_mask].iloc[:1]
        test_df = pd.concat([test_positive, test_negative])
        test_df = test_df.sample(frac=1, random_state=42).reset_index(drop=True)
    else:
        # 如果样本不足，使用前3个
        test_df = df.iloc[:3]

    test_samples = test_df[feature_cols].values
    true_labels = test_df['label'].values

    print(f"\n[OK] 验证样本数量: {len(test_samples)}")
    print("[OK] 正负样本分布:")
    print(f"  正样本: {np.sum(true_labels == 1)} 个")
    print(f"  负样本: {np.sum(true_labels == 0)} 个")

    for i in range(len(test_samples)):
        label_type = "购买" if true_labels[i] == 1 else "非购买"
        print(f"\n样本 {i+1} (真实标签: {label_type}):")
        print(f"  特征值: user_id={test_samples[i][0]:.0f}, item_id={test_samples[i][1]:.0f}, "
              f"hour={test_samples[i][2]:.0f}, dayofweek={test_samples[i][3]:.0f}")

    # 预测
    predictions = loaded_pipeline.predict(test_samples)
    probabilities = loaded_pipeline.predict_proba(test_samples)

    print("\n预测结果:")
    correct_count = 0
    for i in range(len(test_samples)):
        pred_label = "购买" if predictions[i] == 1 else "非购买"
        true_label = "购买" if true_labels[i] == 1 else "非购买"
        prob_class1 = probabilities[i][1]  # 购买概率
        prob_class0 = probabilities[i][0]  # 非购买概率

        is_correct = pred_label == true_label
        if is_correct:
            result = "[OK] 正确"
            correct_count += 1
        else:
            result = "[FAIL] 错误"

        print(f"\n样本 {i+1}:")
        print(f"  真实标签: {true_label}")
        print(f"  预测标签: {pred_label} {result}")
        print(f"  预测概率: 购买={prob_class1:.4f}, 非购买={prob_class0:.4f}")

    print(f"\n验证准确率: {correct_count}/{len(test_samples)} = {correct_count/len(test_samples):.2%}")

    # ==================== 总结 ====================
    print("\n" + "=" * 60)
    print("实验七任务1完成！（优化版）")
    print("=" * 60)
    print("\n优化版训练总结:")
    print(f"  - 训练样本数: {len(df)} (正样本: {positive_samples}, 负样本: {negative_samples})")
    print(f"  - 特征数量: {len(feature_cols)}")
    print(f"  - 优化措施: class_weight='balanced'")
    print("\n模型性能指标:")
    print(f"  - 准确率 Accuracy: {metrics_summary['accuracy']['mean']:.4f}")
    print(f"  - AUC: {metrics_summary['auc']['mean']:.4f}")
    print(f"  - 精确率 Precision: {metrics_summary['precision']['mean']:.4f}")
    print(f"  - 召回率 Recall: {metrics_summary['recall']['mean']:.4f}")
    print(f"  - F1分数: {metrics_summary['f1']['mean']:.4f}")
    print(f"\n模型文件: {model_path}")
    print("\n[STAR] 优化效果:")
    print(f"  - 重点提升了模型识别少数类（购买行为）的能力")
    print(f"  - 通过 class_weight='balanced' 平衡样本权重")
    print(f"  - 增加了更适合不平衡数据的评估指标")
    print("=" * 60)

if __name__ == "__main__":
    main()