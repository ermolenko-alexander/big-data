import kotlin.math.*

import kotlinx.coroutines.*

suspend fun countPrimeFactors(num: Int): Int {
    var factors = num
    val primeFactors = mutableListOf<Int>()

    var divisor = 2
    while (factors > 1) {
        if (factors % divisor == 0) {
            primeFactors.add(divisor)
            factors /= divisor
        } else {
            divisor++
        }
    }

    return primeFactors.size
}

fun main() = runBlocking {
    val numbers = listOf(56, 72, 101, 150)

    var totalPrimeFactors = 0
    for (num in numbers) {
        totalPrimeFactors += countPrimeFactors(num)
    }

    println("Total number of prime factors with Coroutines: $totalPrimeFactors")
}