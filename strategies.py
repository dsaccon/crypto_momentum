import backtrader as bt

class MaCrossStrategy(bt.Strategy):

    def __init__(self):
        ma_fast = bt.ind.SMA(period = 10)
        ma_slow = bt.ind.SMA(period = 50)

        self.crossover = bt.ind.CrossOver(ma_fast, ma_slow)

    def next(self):
        if not self.position:
            if self.crossover > 0:
                self.buy()
        elif self.crossover < 0:
            self.close()

class WilliamsRPriceEMA(bt.Strategy):

    def __init__(self):
        self.order = None

        self.ema = bt.ind.MovingAverageExponential(period=50)
        self.williams_r = bt.ind.WilliamsR(period=50)

        close_over_ema = self.datas[0].close > self.ema
        close_under_ema = self.datas[0].close < self.ema

        willr_ema_crossover = bt.ind.CrossOver(self.williams_r, self.ema)
        price_ema_crossover = bt.ind.CrossOver(self.datas[0].close, self.ema)

        self.buy_sig = bt.And(close_over_ema, willr_ema_crossover > 0)
        self.buy_close_sig = price_ema_crossover < 0
        self.sell_sig = bt.And(close_under_ema, willr_ema_crossover < 0)
        self.sell_close_sig = price_ema_crossover > 0

    def next(self):
        if self.order:
            # Only want to be in one position at a time
            return

        if not self.position:
            # We are not in the market, look for signals to open positions
            if self.buy_sig:
                self.buy()
            elif self.sell_sig:
                self.sell()
        else:
            # We are already in the market, look for signals to close positions
            if self.buy_close_sig or self.sell_close_sig:
                self.close()

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # An active Buy/Sell order has been submitted/accepted - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, {order.executed.price:.2f}')
            elif order.issell():
                self.log(f'SELL EXECUTED, {order.executed.price:.2f}')
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Reset orders
        self.order = None
