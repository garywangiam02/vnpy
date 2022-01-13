#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

factors = ['量比', ]


def signal(*args):
	df = args[0]
	n  = args[1]
	df[f'量比_bh_{n}'] = (df['quote_volume'] / df['quote_volume'].rolling(n, min_periods=1).mean()).shift(1)

	return [f'量比_bh_{n}', ]