import sympy
import random

# Генерация случайных чисел
random_numbers = [random.randint(2, 10**9) for _ in range(50000)]

# Функция для подсчета количества простых множителей
def count_prime_factors(num):
    prime_factors = sympy.primefactors(num)
    return len(prime_factors)

# Подсчет суммарного количества простых множителей
total_prime_factors = sum([count_prime_factors(num) for num in random_numbers])

print("Суммарное количество простых множителей при факторизации всех чисел:", total_prime_factors)