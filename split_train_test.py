# split_train_test.py

import os
import random
import json
import logging
from datetime import datetime
from tqdm import tqdm


def write_data(file_path, data, desc):
    """
    将数据写入指定文件。

    :param file_path: 输出文件路径
    :param data: 要写入的数据列表
    :param desc: 进度条描述
    """
    logging.info(f"开始写入{desc}到 {file_path}")
    with open(file_path, 'w') as f2:
        for line in tqdm(data, desc=f"写入{desc}", unit="行"):
            f2.write(line)
    logging.info(f"{desc}写入完成，共 {len(data)} 行")


class DataSplitter:
    """
    用于将数据集拆分为训练集和测试集的类。
    """

    def __init__(self, config1):
        """
        初始化 DataSplitter 类。

        :param config1: 包含配置参数的字典
        """
        self.input_file = config1['input_file']
        self.output_dir = config1['output_file']
        self.test_size = config1['sample_size']
        self.user_sample_size = config1.get('user_sample_size', self.test_size)
        self.random_sample = config1['random_sample']
        self.seed = config1.get('seed')
        self.indent = config1.get('indent', 2)

        # 获取模型版本号和当前日期
        self.model_version = os.path.basename(self.input_file).split('_data.jsonl')[0]
        self.current_date = datetime.now().strftime("%Y%m%d")

        # 创建输出文件名
        self.train_file = os.path.join(self.output_dir, f"{self.model_version}_{self.current_date}_train.jsonl")
        self.test_file = os.path.join(self.output_dir, f"{self.model_version}_{self.current_date}_test.jsonl")
        self.user_test_file = os.path.join(self.output_dir, f"{self.model_version}_{self.current_date}_user_test.jsonl")

        # 设置日志
        self.setup_logging()

    def setup_logging(self):
        """
        设置日志配置。
        """
        log_file = os.path.join(self.output_dir, f"{self.model_version}_{self.current_date}_split.log")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    def read_data(self):
        """
        读取输入文件中的所有数据。

        :return: 包含所有数据行的列表
        """
        logging.info(f"开始读取输入文件: {self.input_file}")
        with open(self.input_file, 'r') as f1:
            data = f1.readlines()
        logging.info(f"成功读取 {len(data)} 行数据")
        return data

    def shuffle_data(self, data):
        """
        如果配置为随机采样，则打乱数据。

        :param data: 要打乱的数据列表
        :return: 打乱后的数据列表
        """
        if self.random_sample:
            logging.info("开始随机打乱数据")
            if self.seed is not None:
                random.seed(self.seed)
                logging.info(f"使用随机种子: {self.seed}")
            random.shuffle(data)
            logging.info("数据打乱完成")
        return data

    def split_data(self, data):
        """
        将数据拆分为测试集和训练集。

        :param data: 要拆分的数据列表
        :return: 包含测试集和训练集的元组
        """
        logging.info(f"开始拆分数据，测试集大小: {self.test_size}")
        return data[:self.test_size], data[self.test_size:]

    def create_user_test_set(self, data):
        """
        从测试集中创建用户测试集。

        :param data: 测试集数据
        :return: 用户测试集数据
        """
        user_test_data = data[:self.user_sample_size]
        logging.info(f"创建用户测试集，大小: {len(user_test_data)}")
        return user_test_data

    def split_train_test(self):
        """
        执行训练集和测试集的拆分过程。
        """
        logging.info("开始数据拆分过程")

        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        logging.info(f"输出目录: {self.output_dir}")

        # 读取所有数据
        all_data = self.read_data()
        total_count = len(all_data)

        # 打乱数据（如果需要）
        all_data = self.shuffle_data(all_data)

        # 拆分数据
        test_data, train_data = self.split_data(all_data)

        # 创建用户测试集
        user_test_data = self.create_user_test_set(test_data)

        # 写入测试集、训练集和用户测试集
        write_data(self.test_file, test_data, "测试集")
        write_data(self.train_file, train_data, "训练集")
        write_data(self.user_test_file, user_test_data, "用户测试集")

        # 记录结果
        logging.info("拆分完成")
        logging.info(f"总行数: {total_count}")
        logging.info(f"测试集: {self.test_file}, 行数: {len(test_data)}")
        logging.info(f"训练集: {self.train_file}, 行数: {len(train_data)}")
        logging.info(f"用户测试集: {self.user_test_file}, 行数: {len(user_test_data)}")


def split_train_test(config2):
    """
    用于与原有代码兼容的函数。

    :param config2: 包含配置参数的字典
    """
    splitter = DataSplitter(config2)
    splitter.split_train_test()


if __name__ == "__main__":
    # 读取配置文件
    with open('config.json', 'r') as f:
        config = json.load(f)

    if config['operation'] == 'split_train_test':
        split_train_test(config)
    else:
        logging.error(f"未知操作: {config['operation']}")
