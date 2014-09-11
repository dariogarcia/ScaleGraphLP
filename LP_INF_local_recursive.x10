/*
 * Calculates the local INF score of all vertices
 *
 * V (vertices) stores a list of paths to other vertices of distance fixed: GrowableMemory[Path],
 *   as well as a list of Ids of those vertices candidates for LP (distance to vertex > 1 || weight of edge to vertex == 0): GrowableMemory[Long]
 * E (edges) store a boolean, 1 if the edge is to be used for training, 0 if the edge is to be used for test: Byte
 * M (messages) must pass lists of paths from one vertex to another: GrowableMemory[Path] as well as the source vertex Id: Long
 * A (aggregator) have no purpose so far: Double
 */

import x10.util.Team;
import org.scalegraph.util.GrowableMemory;
import org.scalegraph.util.MemoryChunk;
import org.scalegraph.graph.Graph;
import org.scalegraph.io.CSV;
import org.scalegraph.id.Type;
import org.scalegraph.Config;

import org.scalegraph.test.STest;

import org.scalegraph.xpregel.VertexContext;
import org.scalegraph.xpregel.XPregelGraph;

public class LP_INF_local_recursive extends STest {


    public static struct PredictedLink{
        val id:Long;
        val TP:Boolean;
        public def this(i: Long, t: Boolean){
            id = i;
            TP = t;
        }
    }
    
    public static struct Message{
        val id_sender:Long;
        val neighbours:GrowableMemory[Step];
        public def this(i: Long, n: GrowableMemory[Step]){
            id_sender = i;
            neighbours = n;
        }
    }

    public static struct VertexData{
        val steps: GrowableMemory[Step];
        val testCandidates: GrowableMemory[Long];
        val candidates: GrowableMemory[Long];
        val Descendants: Long;
        val Ancestors: Long;
        public def this(){
            steps = new GrowableMemory[Step]();
            testCandidates = new GrowableMemory[Long]();
            candidates = new GrowableMemory[Long]();
            Descendants = 0;
            Ancestors = 0;
        }
        public def this(test: GrowableMemory[Long], st: GrowableMemory[Step], d: Long, a: Long){
            steps = st;
            testCandidates = test;
            candidates = new GrowableMemory[Long]();
            Descendants = d;
            Ancestors = a;
        }
    }

    public static struct Step {
        //Direction determines if the node is a descendant (0, a vertex pointing to self) or an ancestor (1, a vertex self points to)
        val direction: x10.lang.Boolean;
        //Id of vertex related though step
        val targetId: x10.lang.Long;
        //Neighbors of that vertex
        val neighbors: GrowableMemory[Step];
        public def this(d: x10.lang.Boolean, t: x10.lang.Long) {
            direction = d;
            targetId = t;
            neighbors = new GrowableMemory[Step]();
        }
        public def this(d: x10.lang.Boolean, t: x10.lang.Long, n: GrowableMemory[Step]) {
            direction = d;
            targetId = t;
            neighbors = n;
        }
    }
        
    public static def main(args:Array[String](1)) {
        new LP_INF_local_recursive().run(args);
    }

    public def run(args :Array[String](1)): Boolean {
        val team = Team.WORLD;
        // load graph from CSV file
        val graph = Graph.make(CSV.read(args(0),[Type.Long as Int, Type.Long, Type.Byte],true));
        // create sparse matrix
        val csr = graph.createDistSparseMatrix[Byte](Config.get().dist1d(), "weight", true, false);
        // create xpregel instance
        val xpregel = XPregelGraph.make[VertexData, Byte](csr);
        xpregel.setLogPrinter(Console.ERR, 0);
        xpregel.updateInEdge();
        xpregel.updateInEdgeAndValue();

        xpregel.iterate[Message,Double]((ctx :VertexContext[VertexData, Byte, Message, Double], messages :MemoryChunk[Message]) => {
            //first superstep, create all vertex with in-edges as path with one step: <0,Id>
	        //and all out-edges as path with one step: <1,Id>. Also, send paths to all vertices
            if(ctx.superstep() == 0){
                var neighbours :GrowableMemory[Step] = new GrowableMemory[Step]();
                var ancestors :Long = 0; var descendants :Long = 0;
                //Load all outgoing edges of vertex
                val tupleOut = ctx.outEdges();
                val idsOut = tupleOut.get1();
                val weightsOut = tupleOut.get2();
                //Store those vertex linked by an outgoing edge which are to be used as test
                var testNeighs :GrowableMemory[Long] = new GrowableMemory[Long]();
                //For each vertex connected through an outgoing edges
                for(idx in weightsOut.range()) {
                    //If the vertex is connected through an edges with weight = 1
                    if (weightsOut(idx).compareTo(1) == 0){
                        ancestors++;
                        //Add the vertex id to the list of one path neighbors
                        val s = Step(true,idsOut(idx));
                        neighbours.add(s);
                    }
                    //Otherwise add the vertex as a test neighbour
                    else testNeighs.add(idsOut(idx));
                }
                //For each vertex connected through an ingoing edges
                for(idx in ctx.inEdgesValue().range()) {
                    //If the vertex is connected through an edges with weight = 1
                    if (ctx.inEdgesValue()(idx).compareTo(1) == 0){
                        descendants++;
                        //Add the vertex id to the list of one path neighbors
                        val s = Step(false,ctx.inEdgesId()(idx));
                        neighbours.add(s);
                    }
                }
                //Save the built list of one step neighbors
                val firstStepRes:VertexData = new VertexData(testNeighs,neighbours,descendants,ancestors);
                ctx.setValue(firstStepRes);
                val m :Message = Message(ctx.id(),neighbours);
                //Send the list to all neighbors
                //ctx.sendMessageToAllNeighbors(m);
                for(idx in ctx.inEdgesId()) ctx.sendMessage(idx,m);
                for(idx in idsOut.range())  ctx.sendMessage(idsOut(idx),m);
            }
            //Second superstep: read the messages, extend the steps with the information arriving
            if(ctx.superstep() == 1){
                var targets :GrowableMemory[Long] = new GrowableMemory[Long]();
                //Load all one step neighbors
                var neighbours :VertexData = ctx.value();
                val currentSteps:GrowableMemory[Step]  = ctx.value().steps;
                //For each message recieved
                for(mess in messages){
                    val messageId:Long = mess.id_sender;
                    //Seek the sender in the currently stored steps
                    for(rangeCurrentSteps in currentSteps.range()){
                        val currentStep = currentSteps(rangeCurrentSteps);
                        if(currentStep.targetId == messageId){
                            //Once found, create as many 2 steps paths as neighbors within the message
                            for(rangeNewSteps in mess.neighbours.range()){
                                //If self, skip!
                                if(mess.neighbours(rangeNewSteps).targetId == ctx.id()) continue;
                                val s1 :Step = Step(mess.neighbours(rangeNewSteps).direction, mess.neighbours(rangeNewSteps).targetId);
                                neighbours.steps(rangeCurrentSteps).neighbors.add(s1);
                                var found :Boolean = false;
                                //Unless already added, add target
                                for(target_idx in targets.range()) if(targets(target_idx)==mess.neighbours(rangeNewSteps).targetId) found = true;
                                if(!found) targets.add(mess.neighbours(rangeNewSteps).targetId);
                            }
                        }
                    }
                }
                //For each target: If edge exists remove from targets. Else store and id and if TP.
                var predictions :GrowableMemory[PredictedLink] = new GrowableMemory[PredictedLink]();
                for (target_idx in targets.range()){
                    var alreadyExistent :Boolean = false;
                    val target = targets(target_idx);
//TODO check only if outgoing edge exists! 
                    for(rangeCurrentSteps in currentSteps.range()){
                        val currentStep = currentSteps(rangeCurrentSteps);
                        if((target == currentStep.targetId) & currentStep.direction){
                            alreadyExistent = true;
                            break;
                        }
                    }
                    if (alreadyExistent) continue;
                    var isTP :Boolean = false;
                    for(rangeTPs in ctx.value().testCandidates.range()){
                        val currentTP = ctx.value().testCandidates(rangeTPs);
                        if(target == currentTP){
                            isTP = true;
                            break;
                        }
                    }
                    predictions.add(new PredictedLink(target,isTP));
                }
                //For each target, calculate the number of paths and their type
                for(current in predictions.range()){
                    val currentId = predictions(current).id;
//println("-"+ctx.id()+"-"+currentId);
                    var DD :Long = 0; var DA:Long = 0; var AD:Long = 0; var AA:Long = 0;
                    //Seek number of paths of each type
                    for(rangeFirstStep in neighbours.steps.range()){
                        val firstStep = neighbours.steps(rangeFirstStep);
                        var firstDirection :Boolean = firstStep.direction;
                        for(rangeSecondStep in firstStep.neighbors.range()){
                            val secondStep = firstStep.neighbors(rangeSecondStep);
                            //Found a path
                            if(secondStep.targetId == currentId){
                                if(secondStep.direction  &  firstDirection) AA++;
                                if(!secondStep.direction &  firstDirection) AD++;
                                if(secondStep.direction  & !firstDirection) DA++;
                                if(!secondStep.direction & !firstDirection) DD++;
                            }
                        }
                    }
println("-"+ctx.id()+"-Descendants:"+ neighbours.Descendants);
println("-"+ctx.id()+"-Ancestors:"+ neighbours.Ancestors);
                    val score_CN = DD+AA+AD+DA;
println("-"+ctx.id()+"-"+ currentId + " CN score:"+ score_CN);
                }

                //TODO: for all t in targets, calculate number of DD and DA paths that reach it. ded = |DD|/|D|, ind = |DA|/|D|
                //TODO: for all t in targets, che
                //TODO: numNodes*(numNodes-1)-|targets_clean| have weight 0.

//PRINT FOR DEBUGGING
//val m :Message = Message(ctx.id(),neighbours);
//printNeighbourhood(m);
//END PRINT FOR DEBUGGING    

                ctx.voteToHalt();
            }
	    },
        //I'm not sure what could I use the aggregator for.
        null,
        //Combiner CombinePaths should take various Message and append them into the same ... is it possible without losing the Ids??
        //The vertex could add its Id to every path before sending it to the combiner. But this increases the size of messages dramatically.
	    //(paths :MemoryChunk[Message]) => combinePaths(paths),
        (superstep :Int, someValue :Double) => (superstep >= 2));
        return true;
    }

    static def printNeighbourhood(m:Message){
        Console.OUT.println("--------------");
        Console.OUT.println("Neighbourhood of node with Id:"+m.id_sender);
        for(p in m.neighbours.range()){
            //for(s in m.neighbours(p).path.range()){
            //    //Console.OUT.println(m.neighbours(p).path(s).direction + " " + m.neighbours(p).path(s).targetId);
            //}
        Console.OUT.println("--------------");
        }
    }

    //static def dummyAggregator(val : MemoryChunk[Double]) :Double {
    //    //TODO
    //    val tmp :MemoryChunk[Double] = new MemoryChunk[Double]();
    //    return tmp;
    //}
    //static def combinePaths(paths : MemoryChunk[Message]) :Message {
    //    //TODO
    //    val tmp :Message = new Message();
    //    return tmp;
    //}

}



//            var iter:Long = 0;
//            val sb = new SStringBuilder();
//            for(i in result.range()) {
//                sb.add(result(i)).add("\n");
//            }
//            val fw = new FileWriter(args(2), FileMode.Create);
//            fw.write(sb.result().bytes());
//            fw.close();
//            return true;

