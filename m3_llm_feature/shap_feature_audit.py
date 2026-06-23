# -*- coding: utf-8 -*-
"""
大数据实验11 任务4：SHAP特征审计分析
依赖安装：pip install pandas numpy scikit-learn lightgbm shap matplotlib
"""

import warnings
warnings.filterwarnings('ignore')

import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from scipy.sparse import hstack
import lightgbm as lgb
import shap

def extract_structured_features(df, feature_cols=['cat']):
    """提取结构化特征"""
    encoder = OneHotEncoder(sparse_output=True, handle_unknown='ignore')
    features = encoder.fit_transform(df[feature_cols])
    return features, encoder, encoder.get_feature_names_out(feature_cols)

def get_sentiment_labels(df, label_col='label'):
    """获取情感标签"""
    from sklearn.preprocessing import LabelEncoder
    le = LabelEncoder()
    labels = le.fit_transform(df[label_col].fillna('').astype(str))
    return labels, le

def main():
    print("=" * 70)
    print("大数据实验11 任务4：SHAP特征审计分析")
    print("=" * 70)
    
    # 创建输出目录
    output_dir = 'shap_plots'
    os.makedirs(output_dir, exist_ok=True)
    print(f"\n输出目录: {output_dir}")
    
    # 1. 加载数据
    print("\n[1/5] 加载数据...")
    df = pd.read_csv('batch_1000_features.csv', encoding='utf-8')
    print(f"数据加载完成，共 {len(df)} 条记录")
    
    # 2. 提取特征
    print("\n[2/5] 提取特征...")
    
    # TF-IDF特征
    print("  - 提取TF-IDF文本特征...")
    tfidf = TfidfVectorizer(
        token_pattern=r'(?u)\b\w+\b',
        max_features=5000,
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.95
    )
    tfidf_features = tfidf.fit_transform(df['review'].fillna('').astype(str))
    tfidf_feature_names = tfidf.get_feature_names_out()
    print(f"    TF-IDF特征维度: {tfidf_features.shape}")
    
    # LLM结构化特征
    print("  - 提取LLM结构化特征...")
    llm_features, llm_encoder, llm_feature_names = extract_structured_features(df, feature_cols=['cat'])
    print(f"    LLM特征维度: {llm_features.shape}")
    
    # 融合特征
    print("  - 构建融合特征...")
    X = hstack([tfidf_features, llm_features])
    
    # 合并特征名称
    all_feature_names = np.concatenate([tfidf_feature_names, llm_feature_names])
    print(f"    融合特征维度: {X.shape}")
    
    # 获取标签
    y, le = get_sentiment_labels(df, label_col='label')
    print(f"  - 标签类别: {le.classes_}")
    
    # 3. 数据集划分（7:3）
    print("\n[3/5] 划分数据集...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )
    print(f"    训练集: {X_train.shape[0]} 条")
    print(f"    测试集: {X_test.shape[0]} 条")
    
    # 4. 训练LightGBM模型
    print("\n[4/5] 训练LightGBM模型...")
    lgb_train = lgb.Dataset(X_train, y_train)
    lgb_test = lgb.Dataset(X_test, y_test, reference=lgb_train)
    
    params = {
        'objective': 'multiclass',
        'num_class': len(le.classes_),
        'metric': 'multi_logloss',
        'seed': 42,
        'verbose': 0
    }
    
    callbacks = [lgb.early_stopping(10)]
    model = lgb.train(
        params,
        lgb_train,
        num_boost_round=100,
        valid_sets=[lgb_test],
        callbacks=callbacks
    )
    
    # 5. SHAP分析
    print("\n[5/5] SHAP特征贡献分析...")
    print("  - 初始化SHAP解释器...")
    
    # 使用TreeExplainer
    explainer = shap.TreeExplainer(model)
    
    # 计算测试集的SHAP值（采样一部分以提高速度）
    print("  - 计算SHAP值...")
    sample_size = min(100, X_test.shape[0])
    np.random.seed(42)
    sample_indices = np.random.choice(X_test.shape[0], sample_size, replace=False)
    X_sample = X_test[sample_indices].toarray()
    y_sample = y_test[sample_indices]
    
    shap_values = explainer.shap_values(X_sample)
    
    # 处理SHAP值的不同格式
    predictions = np.argmax(model.predict(X_sample), axis=1)
    
    if isinstance(shap_values, list):
        # 多分类任务：shap_values是每个类别的SHAP值列表
        # 每个类别数组形状是(样本数, 特征数)
        shap_values_pred_class = np.zeros((X_sample.shape[0], X_sample.shape[1]))
        for i, pred in enumerate(predictions):
            shap_values_pred_class[i] = shap_values[pred][i]
    elif shap_values.ndim == 3:
        # 多分类任务：shap_values是形状为(样本数, 特征数, 类别数)的数组
        shap_values_pred_class = np.zeros((X_sample.shape[0], X_sample.shape[1]))
        for i, pred in enumerate(predictions):
            shap_values_pred_class[i] = shap_values[i, :, pred]
    else:
        # 单分类或回归任务
        shap_values_pred_class = shap_values
    
    print(f"SHAP值形状: {shap_values_pred_class.shape}")
    print(f"特征名称数量: {len(all_feature_names)}")
    
    print("\nSHAP特征贡献分析结果:")
    print("=" * 70)
    
    # 计算各特征的平均绝对SHAP值（重要性）
    mean_abs_shap = np.mean(np.abs(shap_values_pred_class), axis=0)
    
    # 排序并获取前20个重要特征
    top_indices = np.argsort(mean_abs_shap)[::-1][:20]
    top_features = all_feature_names[top_indices]
    top_shap_values = mean_abs_shap[top_indices]
    
    # 输出前20个重要特征
    print("\n前20个最重要特征:")
    print("-" * 70)
    print(f"{'排名':<6} {'特征名称':<25} {'特征类型':<12} {'平均SHAP值':<12}")
    print("-" * 70)
    
    llm_feature_indices = set(range(len(tfidf_feature_names), len(all_feature_names)))
    
    for i, (idx, name, shap_val) in enumerate(zip(top_indices, top_features, top_shap_values), 1):
        feature_type = "LLM特征" if idx in llm_feature_indices else "TF-IDF特征"
        print(f"{i:<6} {name:<25} {feature_type:<12} {shap_val:<12.4f}")
    
    # 统计LLM特征整体贡献
    print("\nLLM特征贡献统计:")
    print("-" * 70)
    
    # 计算LLM特征和TF-IDF特征的总贡献
    llm_mask = np.array([i in llm_feature_indices for i in range(len(all_feature_names))])
    tfidf_mask = ~llm_mask
    
    llm_total_shap = np.sum(mean_abs_shap[llm_mask])
    tfidf_total_shap = np.sum(mean_abs_shap[tfidf_mask])
    total_shap = llm_total_shap + tfidf_total_shap
    
    llm_contribution_ratio = llm_total_shap / total_shap * 100
    tfidf_contribution_ratio = tfidf_total_shap / total_shap * 100
    
    print(f"LLM特征数量: {len(llm_feature_names)}")
    print(f"TF-IDF特征数量: {len(tfidf_feature_names)}")
    print(f"\n特征贡献占比:")
    print(f"  LLM特征贡献: {llm_contribution_ratio:.2f}%")
    print(f"  TF-IDF特征贡献: {tfidf_contribution_ratio:.2f}%")
    
    # 生成SHAP可视化图
    print("\n生成SHAP可视化图...")
    
    # 1. SHAP汇总图（Feature Importance）
    plt.figure(figsize=(12, 8))
    shap.summary_plot(shap_values_pred_class, X_sample, feature_names=all_feature_names, max_display=20)
    plt.title("SHAP特征重要性汇总图")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'shap_summary.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  - shap_summary.png 已保存")
    
    # 2. SHAP瀑布图（单个样本）
    sample_idx = 0
    plt.figure(figsize=(12, 6))
    # 创建Explanation对象用于瀑布图
    exp = shap.Explanation(
        values=shap_values_pred_class[sample_idx],
        base_values=explainer.expected_value[0],
        feature_names=all_feature_names
    )
    shap.plots.waterfall(exp, max_display=15)
    plt.title(f"SHAP瀑布图 - 样本{sample_idx}")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'shap_waterfall.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  - shap_waterfall.png 已保存")
    
    # 3. SHAP决策图
    plt.figure(figsize=(12, 8))
    shap.decision_plot(explainer.expected_value[0], shap_values_pred_class, 
                       feature_names=all_feature_names, highlight=0, show=False)
    plt.title("SHAP决策图")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'shap_decision.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  - shap_decision.png 已保存")
    
    # 6. LLM特征价值分析结论
    print("\n" + "=" * 70)
    print("LLM特征价值分析结论")
    print("=" * 70)
    
    print("\n[分析结果]")
    print(f"1. 总特征数: {len(all_feature_names)} (TF-IDF: {len(tfidf_feature_names)}, LLM: {len(llm_feature_names)})")
    print(f"2. LLM特征贡献占比: {llm_contribution_ratio:.2f}%")
    print(f"3. TF-IDF特征贡献占比: {tfidf_contribution_ratio:.2f}%")
    
    print("\n[LLM特征价值评估]")
    if llm_contribution_ratio > 10:
        print("✓ LLM特征贡献显著，表明LLM提取的结构化信息对模型预测有重要价值")
    elif llm_contribution_ratio > 5:
        print("✓ LLM特征有一定贡献，可以作为补充特征提升模型性能")
    else:
        print("✗ LLM特征贡献较小，可能需要优化特征提取策略或增加更多LLM特征")
    
    print("\n[关键发现]")
    llm_in_top20 = sum(1 for idx in top_indices if idx in llm_feature_indices)
    print(f"- 前20个重要特征中，LLM特征占 {llm_in_top20} 个")
    if llm_in_top20 > 0:
        print(f"- 重要LLM特征: {', '.join([all_feature_names[i] for i in top_indices if i in llm_feature_indices])}")
    
    print("\n[建议]")
    if llm_contribution_ratio < 10:
        print("1. 考虑从LLM提取更多结构化特征（如情感强度、实体信息等）")
        print("2. 优化LLM prompt设计以获取更丰富的信息")
        print("3. 尝试使用更强大的LLM模型进行特征提取")
    
    print("\n" + "=" * 70)
    print("SHAP特征审计完成！")
    print(f"可视化图片已保存至: {output_dir}/")
    print("=" * 70)

if __name__ == '__main__':
    main()