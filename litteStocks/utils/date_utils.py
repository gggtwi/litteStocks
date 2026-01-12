import pandas as pd
import akshare as ak

class dateUtils():
    def __init__(self):
        self.trade_date_df = ak.tool_trade_date_hist_sina()

    # 获取交易日
    def get_recent_trade_dates(self, n=1):
        """获取最近N个交易日"""
        trade_date_df = self.trade_date_df
        trade_date_df['trade_date'] = pd.to_datetime(trade_date_df['trade_date'])
        trade_date_df = trade_date_df.sort_values('trade_date', ascending=False)

        recent_dates = trade_date_df.head(n)['trade_date'].apply(lambda x:x.strftime('%Y%m%d')).tolist()
        return recent_dates
    
