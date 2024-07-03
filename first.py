# Версия 1: Простое последовательное чтение
import os
import struct
import random
import struct

def create_binary_file(filename, size_in_gb):
    size_in_bytes = size_in_gb * 1024**3
    num_integers = size_in_bytes // 4  # 4 байта на одно 32-разрядное число

    with open(filename, 'wb') as f:
        for _ in range(num_integers):
            num = random.randint(0, 2**32 - 1)
            f.write(struct.pack('>I', num))  # '>I' - big-endian 32-разрядное беззнаковое целое

if name == "main":
    create_binary_file("random_numbers.bin", 2)

def process_binary_file(filename):
    total_sum = 0
    min_value = None
    max_value = None

    with open(filename, 'rb') as f:
        while chunk := f.read(4):
            num = struct.unpack('>I', chunk)[0]
            total_sum += num
            if min_value is None or num < min_value:
                min_value = num
            if max_value is None or num > max_value:
                max_value = num

    return total_sum, min_value, max_value

if name == "main":
    total_sum, min_value, max_value = process_binary_file("random_numbers.bin")
    print(f"Sum: {total_sum}")
    print(f"Min: {min_value}")
    print(f"Max: {max_value}")


# Версия 2: Многопоточная \+ memory\-mapped files
import mmap
import struct
import concurrent.futures

def process_chunk(data_chunk):
    total_sum = 0
    min_value = None
    max_value = None

    for i in range(0, len(data_chunk), 4):
        num = struct.unpack('>I', data_chunk[i:i+4])[0]
        total_sum += num
        if min_value is None or num < min_value:
            min_value = num
        if max_value is None or num > max_value:
            max_value = num
    
    return total_sum, min_value, max_value

def process_binary_file_multithreaded(filename, num_threads=4):
    with open(filename, 'r+b') as f:
        mmapped_file = mmap.mmap(f.fileno(), 0)
        file_size = mmapped_file.size()
        chunk_size = file_size // num_threads

        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            for i in range(num_threads):
                start = i * chunk_size
                end = start + chunk_size if i != num_threads - 1 else file_size
                futures.append(executor.submit(process_chunk, mmapped_file[start:end]))

        total_sum = 0
        min_value = None
        max_value = None
        
        for future in concurrent.futures.as_completed(futures):
            chunk_sum, chunk_min, chunk_max = future.result()
            total_sum += chunk_sum
            if min_value is None or (chunk_min is not None and chunk_min < min_value):
                min_value = chunk_min
            if max_value is None or (chunk_max is not None and chunk_max > max_value):
                max_value = chunk_max
    
    return total_sum, min_value, max_value

if name == "main":
    import time
    
    start_time = time.time()
    total_sum, min_value, max_value = process_binary_file_multithreaded("random_numbers.bin")
    end_time = time.time()
    
    print(f"Sum: {total_sum}")
    print(f"Min: {min_value}")
    print(f"Max: {max_value}")
    print(f"Time taken: {end_time - start_time} seconds")


# Сравнение времени работы
if name == "main":
    import time
    
    start_time = time.time()
    total_sum, min_value, max_value = process_binary_file("random_numbers.bin")
    end_time = time.time()
    
    print(f"Sum: {total_sum}")
    print(f"Min: {min_value}")
    print(f"Max: {max_value}")
    print(f"Time taken: {end_time - start_time} seconds")
