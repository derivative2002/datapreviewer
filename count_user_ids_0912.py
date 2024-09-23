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
        return os.path.join(output_path, "npc_id_collections.csv")
    else:
        return output_path


def count_npc_ids(input_file, output_file):
    npc_id_counter = Counter()
    total_records = 0

    with open(input_file, 'r') as file:
        for line in tqdm(file, desc="Processing"):
            try:
                item = json.loads(line)
                data_info = json.loads(item['data'])['data_info']
                if 'npc_id' in data_info:
                    npc_id_counter[data_info['npc_id']] += 1
                    total_records += 1
            except json.JSONDecodeError:
                print(f"警告：跳过无效的 JSON 行")

    # 确保输出目录存在
    ensure_dir(output_file)

    # 将结果写入CSV文件
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['npc_id', 'count'])
        for npc_id, count in npc_id_counter.items():
            writer.writerow([npc_id, count])

    return len(npc_id_counter), total_records


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

    # 统计 npc_id 并写入CSV
    unique_npcs, total_records = count_npc_ids(input_file, output_file)

    print(f"\n结果已写入 {output_file}")
    print(f"总共有 {unique_npcs} 个不同的 npc_id")
    print(f"总记录数: {total_records}")


if __name__ == "__main__":
    main()
