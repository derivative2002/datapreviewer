import json
from collections import Counter
import csv
import os
import sys
import glob
import logging
from tqdm import tqdm

class NpcIdCounter:
    """
    用于统计NPC ID的类
    """

    def __init__(self, config_file='config.json'):
        """
        初始化NpcIdCounter类
        :param config_file: 配置文件路径
        """
        self.config = self._load_config(config_file)
        self.input_path = self.config['input_file']
        self.output_path = self.config['output_file']
        self.npc_id_counter = Counter()
        self.total_records = 0
        self.logger = self._setup_logger()

    def _load_config(self, config_file):
        """
        加载配置文件
        :param config_file: 配置文件路径
        :return: 配置字典
        """
        with open(config_file, 'r') as file:
            return json.load(file)

    def _setup_logger(self):
        """
        设置日志记录器
        :return: 配置好的日志记录器
        """
        logger = logging.getLogger('NpcIdCounter')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def ensure_dir(self, file_path):
        """
        确保目录存在，如果不存在则创建
        :param file_path: 文件路径
        """
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def get_output_file_path(self):
        """
        获取输出文件路径
        :return: 输出文件的完整路径
        """
        if os.path.isdir(self.output_path):
            return os.path.join(self.output_path, "npc_id_collections.csv")
        else:
            return self.output_path

    def process_file(self, file_path):
        """
        处理单个文件
        :param file_path: 要处理的文件路径
        """
        self.logger.info(f"开始处理文件: {file_path}")
        with open(file_path, 'r') as file:
            for line in tqdm(file, desc=f"Processing {os.path.basename(file_path)}", unit="lines"):
                try:
                    item = json.loads(line)
                    data_info = json.loads(item['data'])['data_info']
                    if 'npc_id' in data_info:
                        self.npc_id_counter[data_info['npc_id']] += 1
                        self.total_records += 1
                except json.JSONDecodeError:
                    self.logger.warning("跳过无效的 JSON 行")

    def count_npc_ids(self):
        """
        统计NPC ID
        """
        self.logger.info("开始统计NPC ID")
        if os.path.isdir(self.input_path):
            part_files = glob.glob(os.path.join(self.input_path, "part-*"))
            for file_path in tqdm(part_files, desc="Processing files", unit="file"):
                self.process_file(file_path)
        else:
            self.process_file(self.input_path)

    def write_results(self):
        """
        将结果写入CSV文件
        """
        output_file = self.get_output_file_path()
        self.ensure_dir(output_file)
        self.logger.info(f"开始写入结果到 {output_file}")
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['npc_id', 'count'])
            for npc_id, count in self.npc_id_counter.items():
                writer.writerow([npc_id, count])
        self.logger.info("结果写入完成")

    def run(self):
        """
        运行NPC ID统计程序
        """
        if not os.path.exists(self.input_path):
            self.logger.error(f"错误：输入路径 {self.input_path} 不存在")
            return

        self.count_npc_ids()
        self.write_results()

        self.logger.info(f"总共有 {len(self.npc_id_counter)} 个不同的 npc_id")
        self.logger.info(f"总记录数: {self.total_records}")

def main():
    """
    主函数
    """
    config_file = sys.argv[1] if len(sys.argv) > 1 else 'config.json'
    counter = NpcIdCounter(config_file)
    counter.run()

if __name__ == "__main__":
    main()
