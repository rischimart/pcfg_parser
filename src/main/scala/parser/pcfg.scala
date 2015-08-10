package parser
import util.Preprocess._
import scala.math.log
import scala.math.max

import scala.collection.immutable.Map
import scala.io.Source

object PCFG {
  type Pair[T] = Tuple2[T, T]
  type Triple[T] = Tuple3[T, T, T]
  type TwoDArr[T] = Array[Array[T]]
  var unaryRuleCounts = Map.empty[Pair[String], Int]
  var binaryRuleCounts = Map.empty[Triple[String], Int]
  var BinaryRuleIndexes = Map.empty[String, Set[Pair[String]]]
  var nonTerminalCounts = Map.empty[String, Int]
  var ruleProbs = Map.empty[String, Double]
  var commonWords = Set.empty[String]
  val EPSILON = log(Double.MinPositiveValue)
  
  def getCounts(ruleFile: String): Unit = {
    def addToIndexes(nt: String, rightSide: Pair[String]): Unit = {
      val rightSides = BinaryRuleIndexes.getOrElse(nt, Set.empty[Pair[String]])
      BinaryRuleIndexes += (nt -> (rightSides + rightSide))
    }
    for (line <- Source.fromFile(ruleFile).getLines()) {
      val tokens = line.split(" ")
      val count = tokens(0).toInt
      val ruleType = tokens(1)
      if (ruleType.equals("NONTERMINAL")) {
        nonTerminalCounts += (tokens(2) -> count)
      } else if (ruleType.equals("UNARYRULE")) {
        unaryRuleCounts += ((tokens(2), tokens(3)) -> count)        
        commonWords += tokens(3)
        //addToIndexes(tokens(2), (tokens(3), tokens(3)))
      } else if (ruleType.equals("BINARYRULE")) {
        binaryRuleCounts += ((tokens(2), tokens(3), tokens(4)) -> count)
        addToIndexes(tokens(2), (tokens(3), tokens(4)))
      }
    }
  }
  
  def getRuleProb(rule: Triple[String]) : Double = {
    log(binaryRuleCounts(rule).toDouble) - log(nonTerminalCounts(rule._1))   
  }
  
  def getResult(bp: TwoDArr[Map[String, Tuple3[String, String, Int]]]) : String = {
    val len = bp.length
    def buildParseTree(nt: String, lo: Int, hi: Int) : TreeNode = {
      val (n, v, s) = bp(lo)(hi)(nt)
      if (lo == hi) {
        Derivation(s""""$nt"""", List(Terminal(raw""""$n"""")))
      } else {
        val child1 = buildParseTree(n, lo, s)
        val child2 = buildParseTree(v, s + 1, hi)
        Derivation(s""""$nt"""", List(child1, child2))
      }
    }
    val root = buildParseTree("SBARQ", 0, len - 1)
    jsonify(false)(root)
  }
  
  def runCKY(sentence: String): String = {
    val tokens = sentence.split(" ")
    val len = tokens.length
    var pi = Array.ofDim[Map[String, Double]](len, len)
    var bp = Array.ofDim[Map[String, Tuple3[String, String, Int]]](len, len)

    for (r <- 0 until len) {
      for (c <- r until len) {
        pi(r)(c) = Map.empty[String, Double]
        bp(r)(c) = Map.empty[String, Tuple3[String, String, Int]]
        if (r == c) {
          val token = if (commonWords contains tokens(r)) tokens(r) else "_RARE_"
          for (nt <- nonTerminalCounts.keys) {
              val ruleProb = unaryRuleCounts.getOrElse((nt, token), 0).toDouble / 
                             nonTerminalCounts(nt)
              pi(r)(c) += (nt -> log(max(ruleProb, EPSILON)))
              bp(r)(c) += (nt -> (tokens(r), tokens(r), r))
          }
        }
      }
    }
    
    def findMaxProb(lo: Int, hi: Int, nt: String) : Tuple3[Double, Int, Pair[String]] = {
      val candidates = for {
        s <- lo until hi
        (n, v) <- BinaryRuleIndexes(nt)
        val rule = (nt, n, v)
        val p = getRuleProb(rule) + 
                pi(lo)(s).getOrElse(n, EPSILON) + 
                pi(s + 1)(hi).getOrElse(v, EPSILON)
      } yield (p, s, (n, v))
      candidates.maxBy(_._1)
    }
    
    for (l <- 2 to len) {
      for (lo <- 0 until len - l + 1) {
        for (nt <- BinaryRuleIndexes.keys) {
          val hi = lo + l - 1
          val(p, s, (n, v)) = findMaxProb(lo, hi, nt)
          pi(lo)(hi) += (nt -> p)
          bp(lo)(hi) += (nt -> (n, v, s))
        }  
      }
    }
    getResult(bp)
  }
  
  def run(devFile: String, outputFile: String): Unit = {
    val results = Source.fromFile(devFile).getLines() map runCKY
    writeResults(results, outputFile)
  }
}
