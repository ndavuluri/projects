import akka.actor._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random
import java.security.MessageDigest

object project3 {
  case class closestFinger(Sender: ActorRef, key:Int, id:Int, mode:Int, hops:Int)
  case class find_Successor(Sender: ActorRef, key:Int, id:Int, mode:Int, hops:Int)
  case class returnSuccessor(id:Int, node:finger, mode:Int, hops:Int)
  case class Successor(finger : finger)
  case class Nodejoin(firstNode:ActorRef)
  case class getSuccessor(nodeId:Int)
  case class Predecessor(finger : finger)
  case class finished(avgHops : Float)
  case class inform(id:Int) //np sends n to update id as its predecessor
  case object findKey
  case object printStatus
  case object stabilize
  case object fix_fingers
  case object returnPredecessor 
  val heartBeat : FiniteDuration = 5 milliseconds
  var maxRequests = 0
  var numNodes = 0
  var max = 0
  var set = new mutable.HashSet[Int]()
  var m = 0

  class Node(identifier : Int, watcher: ActorRef) extends Actor{
    var predecessor : finger  = new finger
    var fingers : Array[finger] = new Array(m)
    var keys: mutable.HashMap[Int, ActorRef] = new mutable.HashMap()
    var key = 0
    var reqProcessed = 0
    var hops = 0
    var stabilizeC  : Cancellable = null
    var fixFingersC : Cancellable = null
    var requests    : Cancellable = null
    var sCounter = 5000
    var fCounter = m * 50
    var printC  = 10
    var selfF = new finger
    selfF.id = identifier
    selfF.node = self
    def receive = {
      case `findKey`            =>
          val random = new Random()
          val length  = random.nextInt(100)
          val input   = random.nextString(length)
          val id = returnKey(input)
          val hopCount = 0
          self ! find_Successor(self, 0, id, 0, hopCount)
      case closestFinger(req, k, id, mode, hopCount)   =>
        var loop = false
        var i = m-1
        while(!loop){
          if(fingers(i) != null) {
            if (fingers(i).node != null) {
              if (isInRange(identifier, id, fingers(i).id)) {
                fingers(i).node ! find_Successor(req, k, id, mode, hopCount)
                loop = true
              }
            }
          }
          i-=1
          if(i<0){
            loop = true
          }
        }
      case find_Successor(req, k, id, mode, hopCount)=>
        if(id == identifier){
          req ! returnSuccessor(k, selfF , mode, hopCount)
        }else {
          if (isInRange(identifier, fingers(0).id, id)) {
            req ! returnSuccessor(k, fingers(0), mode, hopCount)
          } else {
            val newHopCount = hopCount + 1
            self ! closestFinger(req, k, id, mode, newHopCount)
          }
        }
      case returnSuccessor(k, finger, mode, hopCount) =>
        mode match {
          case 0 =>
                hops += hopCount
                reqProcessed+=1
                if (reqProcessed == maxRequests){
                  requests.cancel()
                  val avg = hops/maxRequests
                  watcher ! finished(avg)
                }
          case 3 =>
            self ! Successor(finger)
          case 1 =>
            fingers(k).id   = finger.id
            fingers(k).node = finger.node
        }
      case Nodejoin(firstNode)        =>
        firstNode ! getSuccessor(identifier)

      case Successor(finger) =>
        fingers(0)        = new finger
        fingers(0).id     = finger.id
        fingers(0).node   = finger.node
        fingers(0).node ! inform(identifier)

        import context.dispatcher

        stabilizeC  = context.system.scheduler.schedule(0 seconds, heartBeat, self, stabilize)
        context.system.scheduler.schedule(15 seconds, 1 seconds, self, printStatus)
        fixFingersC = context.system.scheduler.schedule(5 seconds, 5 * heartBeat, self, fix_fingers)
        requests    = context.system.scheduler.schedule(20 seconds, 1 seconds, self, findKey)

      case `printStatus` =>
        printC-=1
        if(printC == 0) {
         
        }

      case getSuccessor(id)   =>
        val hopCount = 0
        self ! find_Successor(sender(), 0, id, 3, hopCount)

      case `stabilize` =>
        if(sCounter == 0) {
          if (!stabilizeC.isCancelled) {
            stabilizeC.cancel()
          }
        }
        fingers(0).node ! returnPredecessor
        sCounter-=1


      case `fix_fingers` =>
        if(fCounter == 0) {
          if (!fixFingersC.isCancelled) {
            fixFingersC.cancel()
          }
        }
        val random = new Random()
        val fIndex = 1 + random.nextInt(m-1)
        val start = (identifier + Math.pow(2, fIndex).toInt) % max
        val hopCount = 0
        if(fingers(fIndex) == null){
          fingers(fIndex)  = new finger
        }
        self ! find_Successor(self, fIndex, start, 1, hopCount)
        fCounter-=1

      case inform(id) =>
        if(predecessor.node == null){
          predecessor.id   = id
          predecessor.node = sender()
        }else{
          if(isInRange(predecessor.id, identifier, id)){
            predecessor.id   = id
            predecessor.node = sender()
          }
        }

      case `returnPredecessor` =>
          sender() ! Predecessor(predecessor)

      case Predecessor(finger) =>
        if(finger.node != null) {
          if (isInRange(identifier, fingers(0).id, finger.id)) {
            fingers(0).id = finger.id
            fingers(0).node = finger.node
            fingers(0).node ! inform(identifier)
          }
        }
    }
  }

  class finger{
    var id  :Int      = 0
    var node:ActorRef = null
  }

  class Watcher extends Actor {
    var total = 0.0
    var counter = 0
    def receive = {
      case finished(avg) ⇒
        total += avg
        counter+=1
        if(counter == numNodes - 1){
          val avg = total/counter
          println("Average Number of Hops : " + avg)
          context.system.shutdown()
        }

    }
  }

  def isInRange(s:Int, end:Int, id:Int) : Boolean = {
    val start = s + 1
    if(start < end) {
      if (id <= end && id >=start) {
        return true
      }
    }else if(start > end){
      if((id >= start && id <= max) || (id >= 0 && id <= end)){
        return true
      }
    }

    false
  }

  def main (args: Array[String]) {
    numNodes    = args(1).toInt
    maxRequests = args(2).toInt
    val system = ActorSystem("Chord")
    val watcher = system.actorOf(Props[Watcher], name = "watcher")
    val random = new Random()
    m = 31
    max = Math.pow(2,m).toInt - 1
    val length  = random.nextInt(100)
    val txt = random.nextString(length)
    val id = returnKey(txt)
    val firstNode = system.actorOf(Props(new Node(id, watcher)),"firstNode")
    val finger = new finger
    finger.id = id
    finger.node = firstNode
    set.add(id)
    firstNode ! Successor(finger)
    firstNode ! Predecessor(finger)

    val counter = numNodes

    for(i <- 1 until counter){
      val name = "Node" + i
      var length  = random.nextInt(100)
      var txt = random.nextString(length)
      var id = returnKey(txt)
      while(set.contains(id)) {
        length  = random.nextInt(100000)
        txt = random.nextString(length)
        id = returnKey(txt + length)
      }
      set.add(id)
      val newNode = system.actorOf(Props(new Node(id, watcher)), name)
      newNode ! Nodejoin(firstNode)
    }
  }

  def returnKey(input : String) : Int = {
    val bytes = (m + 1) / 8
    val hash = MessageDigest.getInstance("SHA-1").digest(input.getBytes("UTF-8"))
    var key = 0
    val random = new Random()
        for(i <- 0 until bytes){
          key = key | hash(i) << (i * 8)
        }
    val res = key & 0x7FFFFFFF
    res
  }
}