# -*- coding: utf-8 -*-
"""
LLM特征提取器
用于从评论文本中提取结构化特征（sentiment、category）
"""

import pandas as pd
from sklearn.preprocessing import OneHotEncoder

def extract_structured_features(df, feature_cols=['cat']):
    """
    提取结构化特征（类别特征）
    :param df: 包含特征列的DataFrame
    :param feature_cols: 类别特征列名列表
    :return: 编码后的特征矩阵和编码器
    """
    encoder = OneHotEncoder(sparse_output=True, handle_unknown='ignore')
    features = encoder.fit_transform(df[feature_cols])
    return features, encoder

def get_sentiment_labels(df, label_col='label'):
    """
    获取情感标签
    :param df: DataFrame
    :param label_col: 标签列名
    :return: 标签数组
    """
    from sklearn.preprocessing import LabelEncoder
    le = LabelEncoder()
    labels = le.fit_transform(df[label_col].fillna('').astype(str))
    return labels, le