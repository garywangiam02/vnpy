import os
import glob
# 获取项目根目录
_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
root_path = os.path.abspath(os.path.join(_, '../..'))  # 返回根目录文件夹
# root_path = 'xxx' # 填写你自己的根目录

# separated_path_list = glob.glob(root_path + f'/factor_bank/spot/all_category/before_preprocessing/separated/*')
# f_path = separated_path_list[0]
# f_path = f_path.replace('before_preprocessing','after_preprocessing')
# print(f_path)
