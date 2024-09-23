import json
from collections import Counter
import ijson
import csv
from tqdm import tqdm
import os
import sys

def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

def get_output_file_path(output_path):
    if os.path.isdir(output_path):
        return os.path.join(output_path, "user_id_collections.csv")
    else:
        return output_path

def count_user_ids(input_file, output_file):
    user_id_counter = Counter()
    total_records = 0
    
    with open(input_file, 'rb') as file:
        # 尝试作为单个 JSON 对象处理
        try:
            items = ijson.items(file, 'item')
            for item in tqdm(items, desc="Processing"):
                if 'user_id' in item["raw_data"]:
                    user_id_counter[item["raw_data"]['user_id']] += 1
                    total_records += 1
        except ijson.JSONError:
            # 如果失败，尝试作为 JSONL 处理
            file.seek(0)
            for line in tqdm(file, desc="Processing"):
                try:
                    item = json.loads(line)
                    if 'user_id' in item["raw_data"]:
                        user_id_counter[item["raw_data"]['user_id']] += 1
                        total_records += 1
                except json.JSONDecodeError:
                    print(f"警告：跳过无效的 JSON 行")

    # 确保输出目录存在
    ensure_dir(output_file)

    # 将结果写入CSV文件
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['user_id', 'count'])
        for user_id, count in user_id_counter.items():
            writer.writerow([user_id, count])

    return len(user_id_counter), total_records

def main():
    # 读取配置文件
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = 'config.json'

    with open(config_file, 'r') as config_file:
        config = json.load(config_file)

    input_file = config['input_file']
    output_path = config['output_file']

    # 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误：输入文件 {input_file} 不存在")
        return

    # 获取输出文件路径
    output_file = get_output_file_path(output_path)

    # 统计 user_id 并写入CSV
    unique_users, total_records = count_user_ids(input_file, output_file)

    print(f"\n结果已写入 {output_file}")
    print(f"总共有 {unique_users} 个不同的 user_id")
    print(f"总记录数: {total_records}")

if __name__ == "__main__":
    main()
