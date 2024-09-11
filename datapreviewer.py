import json
import random
import os

class DataProcessor:
    """
    数据处理器类，用于处理JSONL文件的采样操作。
    """

    def __init__(self, config_path):
        """
        初始化数据处理器。
        
        :param config_path: 配置文件的路径
        """
        self.config = self.load_config(config_path)

    @staticmethod
    def load_config(config_path):
        """
        从指定路径加载JSON配置文件。

        :param config_path: 配置文件的路径
        :return: 包含配置信息的字典
        """
        with open(config_path, 'r') as config_file:
            return json.load(config_file)

    def sample_jsonl(self):
        """
        从JSONL文件中采样数据并输出到新文件。
        采样方式可以是随机采样或顺序采样，具体由配置文件决定。
        """
        # 从配置中获取必要的参数
        input_file = self.config['input_file']
        output_file = self.config['output_file']
        sample_size = self.config['sample_size']
        random_sample = self.config['random_sample']
        seed = self.config['seed']

        # 检查 output_file 是目录还是文件
        if os.path.isdir(output_file):
            # 如果是目录，创建一个新的同类型文件，命名为 (input_file)_sample
            input_filename = os.path.basename(input_file)
            output_file = os.path.join(output_file, f"{input_filename}_sample.jsonl")

        # 确保输出文件的目录存在
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # 如果提供了种子，设置随机数生成器的种子
        if seed is not None:
            random.seed(seed)

        sampled_data = []
        total_count = 0

        # 打开输入文件并进行采样
        with open(input_file, 'r') as f:
            if random_sample:
                # 随机采样
                lines = f.readlines()
                total_count = len(lines)
                sampled_data = random.sample(lines, min(sample_size, total_count))
            else:
                # 顺序采样
                for line in f:
                    total_count += 1
                    if total_count <= sample_size:
                        sampled_data.append(line)
                    else:
                        break

        # 将采样的数据写入输出文件
        with open(output_file, 'w') as f:
            f.writelines(sampled_data)

        # 打印采样结果信息
        print(f"采样完成。输出文件: {output_file}")
        print(f"总行数: {total_count}, 采样行数: {len(sampled_data)}")


if __name__ == "__main__":
    # 创建数据处理器实例并执行采样操作
    processor = DataProcessor('config.json')
    processor.sample_jsonl()