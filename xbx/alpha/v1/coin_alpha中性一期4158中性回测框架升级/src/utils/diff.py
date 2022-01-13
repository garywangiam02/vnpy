#!/usr/bin/python3
# -*- coding: utf-8 -*-

import numpy as np
from fracdiff import fdiff  # https://github.com/simaki/fracdiff  pip install fracdiff
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)


def add_diff(_df, _d_list, _name, _add=True):
    """ 为 数据列 添加 差分数据列
    :param _add:
    :param _df: 原数据 DataFrame
    :param _d_list: 差分阶数 [0.3, 0.5, 0.7]
    :param _name: 需要添加 差分值 的数据列 名称
    :param _agg_dict:
    :param _agg_type:
    :param _add:
    :return: """
    if _add:
        results = []
        for _d_num in _d_list:
            _f_name = _name + f'_diff_{_d_num}'

            if len(_df) >= 12:  # 数据行数大于等于12才进行差分操作
                _diff_ar = fdiff(_df[_name], n=_d_num, window=10, mode="valid")  # 列差分，不使用未来数据
                _paddings = len(_df) - len(_diff_ar)  # 差分后数据长度变短，需要在前面填充多少数据
                _diff = np.nan_to_num(np.concatenate((np.full(_paddings, 0), _diff_ar)), nan=0)  # 将所有nan替换为0
                _df[_f_name] = _diff  # 将差分数据记录到 DataFrame
            else:
                _df[_f_name] = np.nan  # 数据行数不足12的填充为空数据

            results.append(_f_name)
			
        return results

