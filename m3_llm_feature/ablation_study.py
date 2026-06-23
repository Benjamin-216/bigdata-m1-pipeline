# -*- coding: utf-8 -*-
"""
大数据实验11 任务3：消融研究
对比不同特征组合的模型性能
依赖安装：pip install pandas numpy scikit-learn lightgbm
"""

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score
from scipy.sparse import hstack
import lightgbm as lgb

# 导入LLM特征提取模块
from llm_feature_extractor import extract_structured_features, get_sentiment_labels

def train_and_evaluate(X, y, params, random_state=42):
    """
    训练并评估LightGBM模型
    :param X: 特征矩阵
    :param y: 标签
    :param params: 模型参数
    :param random_state: 随机种子
    :return: 准确率, F1分数
    """
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=random_state, stratify=y
    )
    
    lgb_train = lgb.Dataset(X_train, y_train)
    lgb_test = lgb.Dataset(X_test, y_test, reference=lgb_train)
    
    callbacks = [lgb.early_stopping(10)]
    
    model = lgb.train(
        params,
        lgb_train,
        num_boost_round=100,
        valid_sets=[lgb_test],
        callbacks=callbacks
    )
    
    y_pred = np.argmax(model.predict(X_test, num_iteration=model.best_iteration), axis=1)
    
    accuracy = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, average='weighted')
    
    return accuracy, f1

def main():
    print("=" * 70)
    print("大数据实验11 任务3：消融研究 - 特征组合对比实验")
    print("=" * 70)
    
    # 1. 读取数据
    print("\n[1/4] 加载数据...")
    df = pd.read_csv('batch_1000_features.csv', encoding='utf-8')
    print(f"数据加载完成，共 {len(df)} 条记录")
    
    # 2. 提取特征
    print("\n[2/4] 提取特征...")
    
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
    print(f"    TF-IDF特征维度: {tfidf_features.shape}")
    
    # LLM结构化特征
    print("  - 提取LLM结构化特征...")
    llm_features, _ = extract_structured_features(df, feature_cols=['cat'])
    print(f"    LLM特征维度: {llm_features.shape}")
    
    # 融合特征
    print("  - 构建融合特征...")
    fusion_features = hstack([tfidf_features, llm_features])
    print(f"    融合特征维度: {fusion_features.shape}")
    
    # 获取标签
    y, le = get_sentiment_labels(df, label_col='label')
    print(f"  - 标签类别: {le.classes_}")
    
    # 3. 统一模型参数
    print("\n[3/4] 配置实验参数...")
    params = {
        'objective': 'multiclass',
        'num_class': len(le.classes_),
        'metric': 'multi_logloss',
        'seed': 42,
        'verbose': 0,
        'boosting_type': 'gbdt'
    }
    print("  - 模型参数已配置完成")
    
    # 4. 执行消融实验
    print("\n[4/4] 执行消融实验...")
    
    results = {}
    
    # 实验①：仅TF-IDF传统特征
    print("\n  实验①: 仅TF-IDF特征")
    acc1, f1_1 = train_and_evaluate(tfidf_features, y, params)
    results['TF-IDF'] = {'accuracy': acc1, 'f1': f1_1}
    print(f"    准确率: {acc1:.4f}, F1分数: {f1_1:.4f}")
    
    # 实验②：仅LLM提取特征
    print("\n  实验②: 仅LLM特征")
    acc2, f1_2 = train_and_evaluate(llm_features, y, params)
    results['LLM'] = {'accuracy': acc2, 'f1': f1_2}
    print(f"    准确率: {acc2:.4f}, F1分数: {f1_2:.4f}")
    
    # 实验③：TF-IDF+LLM融合特征
    print("\n  实验③: TF-IDF+LLM融合特征")
    acc3, f1_3 = train_and_evaluate(fusion_features, y, params)
    results['Fusion'] = {'accuracy': acc3, 'f1': f1_3}
    print(f"    准确率: {acc3:.4f}, F1分数: {f1_3:.4f}")
    
    # 5. 输出结果表格
    print("\n" + "=" * 70)
    print("消融实验结果汇总")
    print("=" * 70)
    
    print("\n" + "-" * 70)
    print(f"{'特征组合':<20} {'准确率':<12} {'F1分数':<12} {'准确率变化':<15} {'F1变化':<15}")
    print("-" * 70)
    
    baseline_acc = results['TF-IDF']['accuracy']
    baseline_f1 = results['TF-IDF']['f1']
    
    for name, metrics in results.items():
        acc_change = (metrics['accuracy'] - baseline_acc) / baseline_acc * 100
        f1_change = (metrics['f1'] - baseline_f1) / baseline_f1 * 100
        
        if name == 'TF-IDF':
            acc_change_str = '-'
            f1_change_str = '-'
        else:
            acc_change_str = f"{acc_change:+.2f}%"
            f1_change_str = f"{f1_change:+.2f}%"
        
        print(f"{name:<20} {metrics['accuracy']:<12.4f} {metrics['f1']:<12.4f} {acc_change_str:<15} {f1_change_str:<15}")
    
    print("-" * 70)
    
    # 6. LLM特征价值分析
    print("\nLLM特征价值分析:")
    print("=" * 70)
    
    print("\n[分析结果]")
    print(f"1. 纯TF-IDF基线模型: 准确率={baseline_acc:.4f}, F1={baseline_f1:.4f}")
    print(f"2. 仅LLM特征模型: 准确率={acc2:.4f}, F1={f1_2:.4f}")
    print(f"3. 融合特征模型: 准确率={acc3:.4f}, F1={f1_3:.4f}")
    
    print("\n[性能变化分析]")
    llm_acc_change = (acc2 - baseline_acc) / baseline_acc * 100
    llm_f1_change = (f1_2 - baseline_f1) / baseline_f1 * 100
    fusion_acc_change = (acc3 - baseline_acc) / baseline_acc * 100
    fusion_f1_change = (f1_3 - baseline_f1) / baseline_f1 * 100
    
    print(f"- 仅LLM特征相对TF-IDF基线:")
    print(f"  * 准确率变化: {llm_acc_change:+.2f}%")
    print(f"  * F1分数变化: {llm_f1_change:+.2f}%")
    
    print(f"\n- 融合特征相对TF-IDF基线:")
    print(f"  * 准确率变化: {fusion_acc_change:+.2f}%")
    print(f"  * F1分数变化: {fusion_f1_change:+.2f}%")
    
    print(f"\n- 融合特征相对仅LLM特征:")
    print(f"  * 准确率变化: {(acc3-acc2)/acc2*100:+.2f}%")
    print(f"  * F1分数变化: {(f1_3-f1_2)/f1_2*100:+.2f}%")
    
    print("\n[结论]")
    if fusion_f1_change > 0:
        print("✓ 融合TF-IDF和LLM特征能够提升模型性能，LLM特征具有补充价值")
    else:
        print("✗ 融合特征未带来明显提升，可能需要优化特征或模型参数")
    
    if llm_f1_change > 0:
        print("✓ 单独使用LLM特征也能取得不错效果，说明LLM提取的结构化信息有价值")
    else:
        print("✗ 仅LLM特征效果不佳，可能需要更多或更有效的LLM特征")
    
    print("\n" + "=" * 70)
    print("消融实验完成！")
    print("=" * 70)

if __name__ == '__main__':
    main()