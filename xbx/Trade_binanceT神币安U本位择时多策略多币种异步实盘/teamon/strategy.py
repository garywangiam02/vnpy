import numpy as np
import pandas as pd
import random


# 随机生成交易信号
def real_signal_random(df, para):
    r = random.random()
    pos = float(df.shape[0] - 1)
    if r <= 0.25:
        df.loc[pos, 'signal'] = 1
    elif r <= 0.5:
        df.loc[pos, 'signal'] = -1
    elif r <= 0.75:
        df.loc[pos, 'signal'] = 0
    else:
        df.loc[pos, 'signal'] = np.nan
    return df
