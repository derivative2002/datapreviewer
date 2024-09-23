import json
import random
import os
from collections import defaultdict
from datetime import datetime


class DataProcessor:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)

    @staticmethod
    def load_config(config_path):
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

    def sample_by_user_id(self):
        input_file = self.config['input_file']
        output_file = self.config['output_file']
        sample_size = self.config['sample_size']
        user_sample_size = self.config.get('user_sample_size', 100)  # 默认采样100个用户
        seed = self.config.get('seed')

        if os.path.isdir(output_file):
            input_filename = os.path.basename(input_file)
            output_file = os.path.join(output_file, f"{input_filename}_user_sample.jsonl")

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        if seed is not None:
            random.seed(seed)

        user_data = defaultdict(list)
        total_count = 0
        user_count = 0

        print(f"开始读取输入文件: {input_file}")
        with open(input_file, 'r') as f:
            for line in f:
                total_count += 1
                if total_count % 100000 == 0:
                    print(f"已处理 {total_count} 行数据")
                try:
                    data = json.loads(line)
                    user_id = data.get('raw_data', {}).get('user_id')
                    if user_id:
                        if user_id not in user_data:
                            user_count += 1
                            if user_count > user_sample_size:
                                continue
                        user_data[user_id].append({
                            'user_id': user_id,
                            'raw_data': data.get('raw_data', {})
                        })
                except json.JSONDecodeError:
                    print(f"警告: 第 {total_count} 行的JSON格式无效")

        print(f"文件读取完成。总行数: {total_count}, 采样用户数: {len(user_data)}")

        sampled_data = []
        for user_id, user_entries in user_data.items():
            sampled_data.extend(random.sample(user_entries, min(1, len(user_entries))))
            if len(sampled_data) >= sample_size:
                break

        if len(sampled_data) < sample_size:
            print(f"警告: 采样数量不足。已采样 {len(sampled_data)} 条，目标是 {sample_size} 条")

        print(f"开始写入输出文件: {output_file}")
        with open(output_file, 'w') as f:
            for entry in sampled_data:
                json.dump(entry, f, ensure_ascii=False)
                f.write('\n')

        print(f"采样完成。输出文件: {output_file}")
        print(f"总行数: {total_count}, 采样行数: {len(sampled_data)}")
        print(f"采样用户数: {len(set(entry['user_id'] for entry in sampled_data))}")

    def split_train_test(self):
        """
        将文件拆分成训练集和测试集。
        """
        input_file = self.config['input_file']
        output_dir = self.config['output_file']
        test_size = self.config['sample_size']
        random_sample = self.config['random_sample']
        seed = self.config.get('seed')

        # 获取模型版本号和当前日期
        model_version = os.path.basename(input_file).split('_data.jsonl')[0]
        current_date = datetime.now().strftime("%Y%m%d")

        # 创建输出文件名
        train_file = os.path.join(output_dir, f"{model_version}_{current_date}_train.jsonl")
        test_file = os.path.join(output_dir, f"{model_version}_{current_date}_test.jsonl")

        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)

        if seed is not None:
            random.seed(seed)

        # 读取所有数据
        with open(input_file, 'r') as f:
            all_data = f.readlines()

        total_count = len(all_data)

        if random_sample:
            # 随机打乱数据
            random.shuffle(all_data)

        # 拆分数据
        test_data = all_data[:test_size]
        train_data = all_data[test_size:]

        # 写入测试集
        with open(test_file, 'w') as f:
            f.writelines(test_data)

        # 写入训练集
        with open(train_file, 'w') as f:
            f.writelines(train_data)

        print(f"拆分完成。")
        print(f"总行数: {total_count}")
        print(f"测试集: {test_file}, 行数: {len(test_data)}")
        print(f"训练集: {train_file}, 行数: {len(train_data)}")

if __name__ == "__main__":
    processor = DataProcessor('config.json')
    processor.sample_by_user_id()
    processor.split_train_test()
