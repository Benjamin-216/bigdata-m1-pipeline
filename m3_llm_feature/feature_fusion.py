# -*- coding: utf-8 -*-
"""
大数据实验11 任务2：特征融合模型构建
依赖安装：pip install pandas numpy scikit-learn lightgbm
"""

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from scipy.sparse import hstack
import lightgbm as lgb

# 从llm_feature_extractor导入特征提取函数
from llm_feature_extractor import extract_structured_features, get_sentiment_labels

def main():
    print("=" * 60)
    print("大数据实验11 任务2：特征融合模型构建")
    print("=" * 60)
    
    # 1. 读取数据
    print("\n[1/4] 读取数据...")
    df = pd.read_csv('batch_1000_features.csv', encoding='utf-8')
    print(f"数据读取成功，共 {len(df)} 条记录")
    
    # 2. 提取TF-IDF文本特征
    print("\n[2/4] 提取TF-IDF特征...")
    tfidf = TfidfVectorizer(
        token_pattern=r'(?u)\b\w+\b',
        max_features=5000,
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.95
    )
    tfidf_features = tfidf.fit_transform(df['review'].fillna('').astype(str))
    print(f"TF-IDF特征维度: {tfidf_features.shape}")
    
    # 3. 提取LLM结构化特征（sentiment、category）
    print("\n[3/4] 提取LLM结构化特征...")
    cat_features, _ = extract_structured_features(df, feature_cols=['cat'])
    print(f"类别特征维度: {cat_features.shape}")
    
    # 4. 特征拼接
    X = hstack([tfidf_features, cat_features])
    print(f"融合后特征维度: {X.shape}")
    
    # 获取标签
    y, le = get_sentiment_labels(df, label_col='label')
    print(f"标签类别: {le.classes_}")
    
    # 5. 数据集划分（7:3）
    print("\n[4/4] 训练模型...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, 
        test_size=0.3, 
        random_state=42,
        stratify=y
    )
    
    # 6. LightGBM训练
    lgb_train = lgb.Dataset(X_train, y_train)
    lgb_test = lgb.Dataset(X_test, y_test, reference=lgb_train)
    
    params = {
        'objective': 'multiclass',
        'num_class': len(le.classes_),
        'metric': 'multi_logloss',
        'seed': 42,
        'verbose': 0
    }
    
    callbacks = [lgb.early_stopping(10), lgb.log_evaluation(20)]
    model = lgb.train(params, lgb_train, 100, valid_sets=[lgb_test], callbacks=callbacks)
    
    # 7. 评估
    print("\n" + "=" * 60)
    print("模型评估结果")
    print("=" * 60)
    
    y_pred = np.argmax(model.predict(X_test, num_iteration=model.best_iteration), axis=1)
    
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, average='weighted')
    recall = recall_score(y_test, y_pred, average='weighted')
    f1 = f1_score(y_test, y_pred, average='weighted')
    
    print(f"准确率:  {accuracy:.4f}")
    print(f"精确率: {precision:.4f}")
    print(f"召回率:    {recall:.4f}")
    print(f"F1分数: {f1:.4f}")
    
    # 8. 对比任务1基线模型
    print("\n" + "=" * 60)
    print("与任务1基线模型对比")
    print("=" * 60)
    
    baseline = {'accuracy': 0.4967, 'f1': 0.4050}
    
    print(f"任务1基线: 准确率={baseline['accuracy']:.4f}, F1={baseline['f1']:.4f}")
    print(f"任务2融合: 准确率={accuracy:.4f}, F1={f1:.4f}")
    print(f"\n性能变化:")
    print(f"  准确率: {(accuracy-baseline['accuracy'])/baseline['accuracy']*100:+.2f}%")
    print(f"  F1分数: {(f1-baseline['f1'])/baseline['f1']*100:+.2f}%")
    
    print("\n" + "=" * 60)
    print("实验完成！")
    print("=" * 60)

if __name__ == '__main__':
    main()