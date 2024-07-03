import random

# Генерация случайных чисел
random_numbers = (random.randint(-(2**31), 2**31 - 1) for _ in range(50000))

# Запись чисел в файл
with open('random_numbers.txt', 'w') as file:
    for num in random_numbers:
        file.write(str(num) + '\n')

print("Генерация и запись случайных чисел завершена.")