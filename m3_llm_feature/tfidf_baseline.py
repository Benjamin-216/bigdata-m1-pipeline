# -*- coding: utf-8 -*-
"""
大数据实验11 任务1：传统NLP基线模型构建
依赖安装：pip install pandas numpy scikit-learn lightgbm
"""

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report
import lightgbm as lgb

def main():
    print("=" * 60)
    print("大数据实验11 任务1：传统NLP基线模型构建")
    print("=" * 60)
    
    # 1. 数据读取
    print("\n[1/6] 读取数据...")
    try:
        df = pd.read_csv('batch_1000_features.csv', encoding='utf-8')
        print(f"数据读取成功，共 {len(df)} 条记录")
    except Exception as e:
        print(f"数据读取失败: {e}")
        return
    
    # 2. 特征构建：TF-IDF向量化
    print("\n[2/6] TF-IDF向量化...")
    tfidf = TfidfVectorizer(
        token_pattern=r'(?u)\b\w+\b',
        max_features=5000,
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.95
    )
    X = tfidf.fit_transform(df['review'].fillna('').astype(str))
    print(f"TF-IDF特征维度: {X.shape}")
    
    # 3. 标签处理：以label作为分类目标标签（sentiment列数据无效）
    print("\n[3/6] 标签编码...")
    le = LabelEncoder()
    y = le.fit_transform(df['label'].fillna('').astype(str))
    print(f"标签类别: {le.classes_}")
    
    # 4. 数据集划分（7:3）
    print("\n[4/6] 划分训练集和测试集...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, 
        test_size=0.3, 
        random_state=42,
        stratify=y
    )
    print(f"训练集: {X_train.shape[0]} 条")
    print(f"测试集: {X_test.shape[0]} 条")
    
    # 5. 模型训练
    print("\n[5/6] 训练LightGBM模型...")
    lgb_train = lgb.Dataset(X_train, y_train)
    lgb_test = lgb.Dataset(X_test, y_test, reference=lgb_train)
    
    params = {
        'objective': 'multiclass',
        'num_class': len(le.classes_),
        'metric': 'multi_logloss',
        'seed': 42,
        'verbose': 0
    }
    
    callbacks = [
        lgb.early_stopping(stopping_rounds=10),
        lgb.log_evaluation(period=20)
    ]
    
    model = lgb.train(
        params,
        lgb_train,
        num_boost_round=100,
        valid_sets=[lgb_test],
        callbacks=callbacks
    )
    
    # 6. 模型评估
    print("\n" + "=" * 60)
    print("模型评估结果")
    print("=" * 60)
    
    y_pred = model.predict(X_test, num_iteration=model.best_iteration)
    y_pred = np.argmax(y_pred, axis=1)
    
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, average='weighted')
    recall = recall_score(y_test, y_pred, average='weighted')
    f1 = f1_score(y_test, y_pred, average='weighted')
    
    print(f"准确率 (Accuracy):  {accuracy:.4f}")
    print(f"精确率 (Precision): {precision:.4f}")
    print(f"召回率 (Recall):    {recall:.4f}")
    print(f"F1 分数 (F1-Score): {f1:.4f}")
    
    print("\n分类报告:")
    print(classification_report(y_test, y_pred, target_names=le.classes_))
    
    print("\n" + "=" * 60)
    print("实验完成！")
    print("=" * 60)

if __name__ == '__main__':
    main()