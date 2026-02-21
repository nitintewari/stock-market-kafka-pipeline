"""
anomaly_detector.py — Real-Time Stock Price Anomaly Detection
--------------------------------------------------------------
Nitin Tewari | Real-Time Stock Market Data Engineering Project
--------------------------------------------------------------
Detects anomalous stock price movements in real-time using:
  1. Z-Score detection    — flags statistically unusual prices
  2. Rate-of-change (ROC) — flags sudden % jumps between ticks
  3. Rolling window stats — maintains a sliding window of recent prices

"""

import math
import logging
from collections import deque
from typing import Tuple

logger = logging.getLogger(__name__)


class AnomalyDetector:
    

    def __init__(
        self,
        window_size: int   = 50,
        z_threshold: float = 2.5,
        roc_threshold: float = 5.0,  # % change threshold
    ):
        self.window_size   = window_size
        self.z_threshold   = z_threshold
        self.roc_threshold = roc_threshold
        self.window        = deque(maxlen=window_size)
        self.last_price    = None

        logger.info(
            
        )

    # ── Core statistics ────────────────────────────────────────────────────────

    def _mean(self) -> float:
        return sum(self.window) / len(self.window)

    def _std(self) -> float:
        if len(self.window) < 2:
            return 0.0
        mean = self._mean()
        variance = sum((x - mean) ** 2 for x in self.window) / (len(self.window) - 1)
        return math.sqrt(variance)

    def _z_score(self, price: float) -> float:
        """Calculate Z-score of price relative to rolling window."""
        std = self._std()
        if std == 0:
            return 0.0
        return abs(price - self._mean()) / std

    def _rate_of_change(self, price: float) -> float:
        """Calculate % change from last price."""
        if self.last_price is None or self.last_price == 0:
            return 0.0
        return abs((price - self.last_price) / self.last_price) * 100

    # ── Main detection method ──────────────────────────────────────────────────

    def check(self, price: float) -> Tuple[bool, float, str]:
        """
        Check if a price is anomalous.

        Args:
            price: Current stock price to evaluate

        Returns:
            Tuple of:
              - is_anomaly (bool)   : Whether price is anomalous
              - z_score    (float)  : Z-score of the price
              - reason     (str)    : Human-readable explanation
        """
        is_anomaly = False
        reason     = "normal"
        z_score    = 0.0

        # Need minimum data before detecting anomalies
        if len(self.window) >= 10:
            z_score = self._z_score(price)
            roc     = self._rate_of_change(price)

            if z_score > self.z_threshold and roc > self.roc_threshold:
                is_anomaly = True
                reason = (
                    
                )
            elif z_score > self.z_threshold:
                is_anomaly = True
                reason = (
                    
                )
            elif roc > self.roc_threshold:
                is_anomaly = True
                reason = (
                    f"Sudden price jump of {roc:.2f}% from last tick "
                    f"({self.last_price:.2f} → {price:.2f})"
                )

        # Update rolling window and last price
        self.window.append(price)
        self.last_price = price

        return is_anomaly, z_score, reason

    def get_stats(self) -> dict:
        """Return current rolling window statistics."""
        if len(self.window) < 2:
            return {"status": "warming up", "data_points": len(self.window)}
        return {
            "data_points" : len(self.window),
            "mean"        : round(self._mean(), 4),
            "std"         : round(self._std(), 4),
            "min"         : round(min(self.window), 4),
            "max"         : round(max(self.window), 4),
            "window_size" : self.window_size,
        }

    def reset(self):
        """Reset the detector state."""
        self.window.clear()
        self.last_price = None
        logger.info("AnomalyDetector state reset.")


# ── Quick test ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import random

    detector = AnomalyDetector(window_size=20, z_threshold=2.5, roc_threshold=5.0)

    print("Simulating stock price stream...\n")

    # Normal prices around 150
    prices = [150 + random.uniform(-2, 2) for _ in range(25)]

    # Inject anomalies
    prices[15] = 185   # sudden spike
    prices[20] = 110   # sudden crash
    prices[23] = 152   # back to normal

    for i, price in enumerate(prices):
        is_anomaly, z_score, reason = detector.check(price)
        status = "🚨 ANOMALY" if is_anomaly else "✅ normal "
        print(f"Tick {i+1:02d} | Price: ${price:6.2f} | Z: {z_score:.2f} | {status} | {reason}")

    print(f"\nRolling stats: {detector.get_stats()}")
