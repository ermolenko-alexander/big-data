import scala.annotation.tailrec

def countPrimeFactors(num: Int): Int = {
  @tailrec
  def calculatePrimeFactors(n: Int, divisor: Int, factors: List[Int]): List[Int] = {
    if (n == 1) factors
    else if (n % divisor == 0) calculatePrimeFactors(n / divisor, divisor, factors :+ divisor)
    else calculatePrimeFactors(n, divisor + 1, factors)
  }

  calculatePrimeFactors(num, 2, List()).length
}

def main(args: Array[String]): Unit = {
  val numbers = List(56, 72, 101, 150)

  val totalPrimeFactors = numbers.map(countPrimeFactors).sum

  println(s"Total number of prime factors in Scala: $totalPrimeFactors")
}
