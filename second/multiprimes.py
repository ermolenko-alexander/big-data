import sympy
import multiprocessing

def count_prime_factors(num):
    prime_factors = sympy.primefactors(num)
    return len(prime_factors)

def count_prime_factors_multiprocessing(numbers):
    total_prime_factors = multiprocessing.Value('i', 0)
    lock = multiprocessing.Lock()

    def worker(num):
        prime_factors_count = count_prime_factors(num)
        with lock:
            total_prime_factors.value += prime_factors_count
    
    with multiprocessing.Pool() as pool:
        pool.map(worker, numbers)

    return total_prime_factors.value

file_path = "numbers.txt"  # Путь к файлу с числами

with open(file_path, 'r') as file:
    numbers = [int(line.strip()) for line in file]

total_prime_factors_multiprocessing = count_prime_factors_multiprocessing(numbers)
print("Суммарное количество простых множителей многопоточно:", total_prime_factors_multiprocessing)