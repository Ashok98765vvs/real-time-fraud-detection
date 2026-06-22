"""
Feature Engineering Pipeline
=============================
Builds fraud detection features from raw transaction data:
- Statistical features (Z-Score, IQR)
- Behavioral features (velocity, frequency)
- ML features (Isolation Forest training + inference)
- Feature store utilities
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.pipeline import Pipeline
import joblib
from loguru import logger
from typing import Tuple, List
import os

MODEL_PATH = "models/isolation_forest.pkl"
SCALER_PATH = "models/scaler.pkl"

NUMERIC_FEATURES = [
    "amount",
    "txn_count_1h",
    "total_amount_1h",
    "avg_amount_1h",
    "stddev_amount_1h",
    "max_amount_1h",
    "country_count_1h",
    "zscore_amount",
    "hour_of_day",
    "is_weekend",
    "is_high_risk_merchant_int",
    "is_card_not_present",
]


# ── Statistical Feature Functions ───────────────────────────────────────────────────

def compute_zscore(series: pd.Series, window: int = 100) -> pd.Series:
    """
    Compute rolling Z-Score for a numeric series.
    Flags values > 3 standard deviations from the rolling mean.
    """
    rolling_mean = series.rolling(window=window, min_periods=10).mean()
    rolling_std  = series.rolling(window=window, min_periods=10).std()
    zscore = (series - rolling_mean) / (rolling_std + 1e-9)
    return zscore


def compute_iqr_outlier(series: pd.Series) -> pd.Series:
    """
    Returns a boolean Series: True where value is an IQR outlier.
    Outlier = value < Q1 - 1.5*IQR  or  value > Q3 + 1.5*IQR
    """
    Q1  = series.quantile(0.25)
    Q3  = series.quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    return (series < lower) | (series > upper)


# ── Behavioral Feature Engineering ──────────────────────────────────────────────────

def build_user_behavioral_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-user behavioral features from transaction history.
    Input dataframe must have columns: user_id, amount, timestamp,
    merchant_country, merchant_category, card_present.
    """
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["user_id", "timestamp"])

    # Time features
    df["hour_of_day"]  = df["timestamp"].dt.hour
    df["day_of_week"]  = df["timestamp"].dt.dayofweek
    df["is_weekend"]   = df["day_of_week"].isin([5, 6]).astype(int)
    df["is_night"]     = ((df["hour_of_day"] >= 22) | (df["hour_of_day"] <= 5)).astype(int)

    # Amount features
    df["log_amount"]   = np.log1p(df["amount"])
    df["zscore_amount"] = df.groupby("user_id")["amount"].transform(
        lambda x: compute_zscore(x)
    )
    df["iqr_outlier"]  = df.groupby("user_id")["amount"].transform(
        lambda x: compute_iqr_outlier(x).astype(int)
    )

    # Velocity features (transactions per user per 1h)
    df = df.set_index("timestamp")
    df["txn_count_1h"] = (
        df.groupby("user_id")["amount"]
        .transform(lambda x: x.rolling("1h").count())
    )
    df["total_amount_1h"] = (
        df.groupby("user_id")["amount"]
        .transform(lambda x: x.rolling("1h").sum())
    )
    df["avg_amount_1h"] = (
        df.groupby("user_id")["amount"]
        .transform(lambda x: x.rolling("1h").mean())
    )
    df["stddev_amount_1h"] = (
        df.groupby("user_id")["amount"]
        .transform(lambda x: x.rolling("1h").std().fillna(0))
    )
    df["max_amount_1h"] = (
        df.groupby("user_id")["amount"]
        .transform(lambda x: x.rolling("1h").max())
    )
    df = df.reset_index()

    # Country velocity (unique countries per user per day)
    df["country_count_1h"] = (
        df.groupby(["user_id", df["timestamp"].dt.date])["merchant_country"]
        .transform("nunique")
    )

    # Binary flags
    df["is_high_risk_merchant_int"] = df["is_high_risk_merchant"].astype(int)
    df["is_card_not_present"] = (~df["card_present"]).astype(int)

    return df


# ── Isolation Forest Model ─────────────────────────────────────────────────────────────

def train_isolation_forest(
    df: pd.DataFrame,
    contamination: float = 0.02,
    n_estimators: int = 200,
    random_state: int = 42
) -> Tuple[IsolationForest, StandardScaler]:
    """
    Train an Isolation Forest on historical transaction features.
    contamination: expected fraction of fraudulent transactions (~2%).
    Returns the trained model and the fitted scaler.
    """
    df_features = build_user_behavioral_features(df)
    X = df_features[NUMERIC_FEATURES].fillna(0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        max_samples="auto",
        bootstrap=True,
        random_state=random_state,
        n_jobs=-1,
    )
    model.fit(X_scaled)

    logger.info(f"Isolation Forest trained | samples={len(X)} | contamination={contamination}")

    # Persist model and scaler
    os.makedirs("models", exist_ok=True)
    joblib.dump(model,  MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)
    logger.info(f"Model saved to {MODEL_PATH}")

    return model, scaler


def load_model() -> Tuple[IsolationForest, StandardScaler]:
    """Load a persisted Isolation Forest model and scaler."""
    model  = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    logger.info("Model and scaler loaded from disk.")
    return model, scaler


def predict_anomaly_scores(df: pd.DataFrame, model, scaler) -> pd.DataFrame:
    """
    Score transactions using the Isolation Forest model.
    Adds columns: isolation_score, is_anomaly_ml
      - isolation_score: raw anomaly score (lower = more anomalous)
      - is_anomaly_ml:   True if model predicts fraud
    """
    df_features = build_user_behavioral_features(df)
    X = df_features[NUMERIC_FEATURES].fillna(0)
    X_scaled = scaler.transform(X)

    df_features["isolation_score"] = model.score_samples(X_scaled)
    df_features["is_anomaly_ml"]   = model.predict(X_scaled) == -1
    return df_features


if __name__ == "__main__":
    # Example: train on synthetic data
    from ingestion.kafka_producer import generate_transaction
    sample_data = pd.DataFrame([generate_transaction() for _ in range(5000)])
    model, scaler = train_isolation_forest(sample_data)
    logger.info("Training complete.")
