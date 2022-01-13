selected_coin_num = 1

stratagy_list = []

hold_hour_list = ['8H']
for hold_hour in hold_hour_list:
    factor_list = [
        ('PmO', True, 60, 0.3, 0.4),
        ('Reg', False, 22, 0.3, 1.0),
    ]

    factor = {
                 'hold_period': f'{hold_hour}',  # 持仓周期
                 'c_factor': f'2103_{hold_hour}',  # 复合因子1号
                 'factors': [

                 ],
                 'selected_coin_num': selected_coin_num,  # 做空或做多币的数量
             },
    for i in range(len(factor_list)):
        factors = {
                      'factor': factor_list[i][0],  # 选币时参考的因子
                      'para': factor_list[i][2],  # 策略的参数
                      'if_reverse': factor_list[i][1],
                      'diff': factor_list[i][3],
                      'weight': round(factor_list[i][4], 1),
                  },

        factor[0]['factors'].extend(factors)

    stratagy_list.extend(factor)

stratagy_list = stratagy_list.copy()
print(stratagy_list)
