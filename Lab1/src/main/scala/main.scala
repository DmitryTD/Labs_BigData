object main
{
  // Константы
  val LENGTH_VECTOR = 3
  val MAX_VECTOR = 10

  def generateRandomList(length: Int, max: Int): List[Int] = {
    List.fill(length)(scala.util.Random.nextInt(max))
  }

  def scalarProduct(firstVector: List[Int], secondVector: List[Int]): Int = {
    var answer: Int = 0
    for(i <- firstVector.indices) {
      answer += firstVector(i) * secondVector(i)
    }
    answer
  }

  def main(args: Array[String]): Unit =
  {
    val firstVector: List[Int] = generateRandomList(LENGTH_VECTOR, MAX_VECTOR)
    val secondVector: List[Int] = generateRandomList(LENGTH_VECTOR, MAX_VECTOR)
    printf("first vector: %s\nsecond vector: %s\n", firstVector.toString(), secondVector.toString())
    printf("scalar product: %d\n", scalarProduct(firstVector, secondVector))
  }
}
