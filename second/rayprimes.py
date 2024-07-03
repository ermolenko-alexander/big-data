import ray
from ray.util.multiprocessing.pool import ThreadPool

import sympy

ray.init()

@ray.remote
def count_prime_factors(num):
    prime_factors = sympy.primefactors(num)
    return len(prime_factors)

def count_prime_factors_ray(numbers):
    total_prime_factors = 0

    with ThreadPool() as pool:
        futures = [count_prime_factors.remote(num) for num in numbers]
        results = pool.map(ray.get, futures)
        total_prime_factors = sum(results)

    return total_prime_factors

file_path = "numbers.txt"  # Путь к файлу с числами

with open(file_path, 'r') as file:
    numbers = [int(line.strip()) for line in file]

total_prime_factors_ray = count_prime_factors_ray(numbers)
print("Суммарное количество простых множителей с помощью Ray:", total_prime_factors_ray)