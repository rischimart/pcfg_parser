import scala.util.matching.Regex
import scala.collection.mutable.Stack
import scala.io.Source
import java.io.PrintWriter
import java.io.File

package object util {
  sealed trait TreeNode
  //case class BinaryRule(nonTerminal: String, left: TreeNode, right: TreeNode) extends TreeNode
  //case class UnaryRule(nonTerminal: String, terminal: String) extends TreeNode
  case class Derivation(nonTerminal: String, children: List[TreeNode]) extends TreeNode
  case class Terminal(terminal: String) extends TreeNode

  var wordCounts = Map.empty[String, Int]
  
  def buildParseTree(struct: String) : TreeNode = {
    val (root, _) = buildTree(struct)
    root
  }
  
  val prefix = """\[?(\"[^\[\]\s\"]+\")(, )?""".r
  def buildTree(struct: String): (TreeNode, String) = {
    val strippedStruct = struct.dropWhile { x => " \n,".contains(x) }
    prefix.findPrefixMatchOf(strippedStruct) match {
      case Some(m) => {
        val token = m.group(1)
        if (m.group(0).startsWith("[")) {
          val (child1, rest1) = buildTree(strippedStruct.substring(m.end))
          val (child2, rest2) = buildTree(rest1)
          val children = if (child2 == null) List(child1) else List(child1, child2)
          val rest = if (child2 != null) rest2.substring(1) else rest2
          (Derivation(token, children), rest)
        } else {
          val count = wordCounts.getOrElse(token, 0)
          wordCounts += (token -> (count + 1))
          (Terminal(token), strippedStruct.substring(m.end))
        }
      }
      case None    => {
        val rest = if (!strippedStruct.isEmpty()) strippedStruct.substring(1) else ""
        (null, rest)
      }
    }
  }
  
  def parseTrainingData(inputFile: String) : Iterator[TreeNode] = {
    val lines = Source.fromFile(inputFile).getLines()
    lines map buildParseTree
  }
  
  def outputModifiedTrainingData(inputFile: String, outputFile: String) : Unit = {
    val structs = parseTrainingData(inputFile) map jsonify 
    val writer = new PrintWriter(new File(outputFile))
    writer.write(structs.mkString("\n"))
    writer.close()
  }
  
  def jsonify(root: TreeNode): String = {
    root match {
      case Terminal(term) => {
        val count = wordCounts.getOrElse(term, 0)
        if (count >= 5) term else "_RARE_"
      }
      case Derivation(nt, children) => {
        val childStruts = (children.map(jsonify)).mkString(", ")
        s"[$nt, $childStruts]"
      }
    }
  }
  
  def merge(iterators: List[Iterator[Regex.Match]] ): Iterator[Regex.Match] = new Iterator[Regex.Match] {
      val bufferedIterators = iterators map { _.buffered }
      
      def hasNext: Boolean = bufferedIterators exists { _.hasNext }
      
      def next(): Regex.Match = {
        if (hasNext) {
          val groupOrdering: Ordering[Regex.Match] = Ordering.by { m => m.start }
          bufferedIterators.filter(_.hasNext).minBy(itr => itr.head)(groupOrdering).next()
        } else {
          throw new UnsupportedOperationException("No next element!")
        }
      }
   }
  
  def serializeStructure2(struct: String): TreeNode = {
    var st = Stack[TreeNode]()
    val openBracket = """\[(\"[^\[\]\s\"]+\")""".r
    val closingBracket = """(\"[^\[\]\s\"]+\")?\]""".r
    val str = """["S", ["NP", ["DET", "There"]], ["S", ["VP", ["VERB", "is"], ["VP", ["NP", ["DET", "no"], ["NOUN", "asbestos"]], ["VP", ["PP", ["ADP", "in"], ["NP", ["PRON", "our"], ["NOUN", "products"]]], ["ADVP", ["ADV", "now"]]]]], [".", "."]]]"""""""
    val openBrackets =  openBracket findAllMatchIn struct
    val closingBrackets = closingBracket findAllMatchIn struct
    
    def link(child: TreeNode, parent: TreeNode): TreeNode = {
      val Derivation(nt, children) = parent
      Derivation(nt, children :+ child)
    }
       
    for (m <- merge(List(openBrackets, closingBrackets))) {
      if (m.group(0).startsWith("[")) {
        val nt = m.group(1)
        st.push(Derivation(nt, List()))
      } else {
        val parent = st.pop()
        if (m.group(1) != null) {
          val word = m.group(1)
          val count = wordCounts.getOrElse(word, 0)
          wordCounts += (word -> (count + 1))
          st.push(link(Terminal(word), parent))
        } else {
          val child = st.pop()
          st.push(link(child, parent))
        }
      }
      //println(st.top)
    } 
    println(st)
    st.top
    //st.reduce(link)
  }
  

}

object Main {
  
  def main(args : Array[String]) : Unit = {
    util.outputModifiedTrainingData(args(0), args(1))
  }
}
  

