import pandas as pd
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class Position:
    symbol: str
    size: float = 0.0
    entry_price: float | None = None


@dataclass
class Portfolio:
    cash: float
    positions: Dict[str, Position] = field(default_factory=dict)
    history: list = field(default_factory=list)

    def apply_trade(self, ts, symbol, side: str, price: float, size: float, commission=0.0, slippage=0.0):
        """Apply a simple trade: size positive means units.

        side: 'buy' | 'sell' | 'close'
        """
        slippage_cost = abs(size) * slippage
        commission_cost = commission
        cost = price * size + slippage_cost + commission_cost

        pos = self.positions.get(symbol) or Position(symbol)

        if side == "buy":
            pos.size += size
            pos.entry_price = price if pos.entry_price is None else pos.entry_price
            self.cash -= cost
        elif side == "sell":
            pos.size -= size
            self.cash += (-cost)
        elif side == "close":
            # close entire position
            self.cash += - (pos.size * price) - commission_cost - slippage_cost
            pos.size = 0
            pos.entry_price = None

        self.positions[symbol] = pos
        self.history.append({"ts": ts, "symbol": symbol, "side": side, "price": price, "size": size, "cash": self.cash})

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.history)
