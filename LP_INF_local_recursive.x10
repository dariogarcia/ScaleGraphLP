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
        val messageGraph:GrowableMemory[Step];
        public def this(i: Long, n: GrowableMemory[Step]){
            id_sender = i;
            messageGraph = n;
        }
    }

    public static struct VertexData{
        val localGraph: GrowableMemory[Step];
        val testCandidates: GrowableMemory[Long];
        val candidates: GrowableMemory[Long];
        val Descendants: Long;
        val Ancestors: Long;
        public def this(){
            localGraph = new GrowableMemory[Step]();
            testCandidates = new GrowableMemory[Long]();
            candidates = new GrowableMemory[Long]();
            Descendants = 0;
            Ancestors = 0;
        }
        public def this(test: GrowableMemory[Long], st: GrowableMemory[Step], d: Long, a: Long){
            localGraph = st;
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
                var localGraph :GrowableMemory[Step] = new GrowableMemory[Step]();
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
                    if(weightsOut(idx).compareTo(1) == 0){
                        ancestors++;
                        //Add the vertex id to the list of one path neighbors
                        val s = Step(true,idsOut(idx));
                        localGraph.add(s);
                    }
                    //Otherwise add the vertex as a test neighbour
                    else testNeighs.add(idsOut(idx));
                }
                //For each vertex connected through an ingoing edges
                for(idx in ctx.inEdgesValue().range()) {
                    //If the vertex is connected through an edges with weight = 1
                    if(ctx.inEdgesValue()(idx).compareTo(1) == 0){
                        descendants++;
                        //Add the vertex id to the list of one path neighbors
                        val s = Step(false,ctx.inEdgesId()(idx));
                        localGraph.add(s);
                    }
                }
                //Save the built list of one step neighbors
                val firstStepRes:VertexData = new VertexData(testNeighs,localGraph,descendants,ancestors);
                ctx.setValue(firstStepRes);
                val m :Message = Message(ctx.id(),localGraph);
                //Send the list to all neighbors
                //ctx.sendMessageToAllNeighbors(m);
                for(idx in ctx.inEdgesId()) ctx.sendMessage(idx,m);
                for(idx in idsOut.range())  ctx.sendMessage(idsOut(idx),m);
            }
            //Second superstep: read the messages, extend the localGraph with the information arriving
            if(ctx.superstep() == 1){
                var localGraphTargets :GrowableMemory[Long] = new GrowableMemory[Long]();
                //Load all one step neighbors
                var vertexData :VertexData = ctx.value();
println("-"+ctx.id()+"-Descendants:"+ vertexData.Descendants+" Ancestors:"+ vertexData.Ancestors);
                val oldLocalGraph:GrowableMemory[Step]  = ctx.value().localGraph;
                //For each message recieved
                for(mess in messages){
                    val messageId:Long = mess.id_sender;
                    //If the sender is myself, skip it cause I already have that information
                    if(messageId == ctx.id()) continue;
                    //Seek the sender in the oldLocalGraph
                    for(rangeCurrentSteps in oldLocalGraph.range()){
                        val oldStep = oldLocalGraph(rangeCurrentSteps);
                        if(oldStep.targetId == messageId){
                            //Once found, extend the localGraph adding one additional step per neighbor within the message
                            for(rangeNewSteps in mess.messageGraph.range()){
                                val newStep :Step = Step(mess.messageGraph(rangeNewSteps).direction, mess.messageGraph(rangeNewSteps).targetId);
                                vertexData.localGraph(rangeCurrentSteps).neighbors.add(newStep);
                                var found :Boolean = false;
                                //Unless already added or is self, add as target according to localGraph
                                for(target_idx in localGraphTargets.range()) if(localGraphTargets(target_idx)==mess.messageGraph(rangeNewSteps).targetId) found = true;
                                if(!found & mess.messageGraph(rangeNewSteps).targetId != ctx.id()) localGraphTargets.add(mess.messageGraph(rangeNewSteps).targetId);
                            }
                        }
                    }
                }
                printLocalGraph(vertexData.localGraph, 0);
                //For each potential target: If outgoing edge from self already exists, remove from targets. Else store id and if TP.
                var LPTargets :GrowableMemory[PredictedLink] = new GrowableMemory[PredictedLink]();
                for(targetIDX in localGraphTargets.range()){
                    val currentTarget = localGraphTargets(targetIDX);
                    var alreadyExistent :Boolean = false;
                    for(rangeCurrentSteps in oldLocalGraph.range()){
                        val currentStep = oldLocalGraph(rangeCurrentSteps);
                        if(currentTarget == currentStep.targetId & currentStep.direction){
                            alreadyExistent = true;
                            break;
                        }
                    }
                    if(alreadyExistent) continue;
                    var isTP :Boolean = false;
                    for(TPsIDX in vertexData.testCandidates.range()){
                        if(currentTarget == vertexData.testCandidates(TPsIDX)){
                            isTP = true;
                            break;
                        }
                    }
                    LPTargets.add(new PredictedLink(currentTarget,isTP));
                }
                //For each target, calculate # of directed (DD/AA/DA/AD) and undirected paths
                for(targetIDX in LPTargets.range()){
                    val target = LPTargets(targetIDX);
                    val undirectedIds :GrowableMemory[Long] = new GrowableMemory[Long]();
                    var DD :Long = 0; var DA:Long = 0; var AD:Long = 0; var AA:Long = 0; 
                    var CN_score :Double = 0; var RA_score:Double = 0; var AA_score:Double = 0; 
                    //Seek number of paths of each type
                    for(rangeFirstStep in vertexData.localGraph.range()){
                        var found :Boolean = false;
                        val firstStep = vertexData.localGraph(rangeFirstStep);
                        var firstDirection :Boolean = firstStep.direction;
                        for(rangeSecondStep in firstStep.neighbors.range()){
                            val secondStep = firstStep.neighbors(rangeSecondStep);
                            //Found a path
                            if(secondStep.targetId == target.id){
println("--Found path from "+ctx.id()+" to "+ target.id + " through " + firstStep.targetId);
                                found = true;
                                if(secondStep.direction  & !firstDirection) DA++;
                                if(secondStep.direction  &  firstDirection) AA++;
                                if(!secondStep.direction & !firstDirection) DD++;
                                if(!secondStep.direction &  firstDirection) AD++;
                            }
                        }
                        //New directed path found, check if an undirected path was already added
                        if(found) {
                            var foundUndirected :Boolean = false;
                            for(undirectedIDX in undirectedIds.range()){
                                if(undirectedIds(undirectedIDX) == firstStep.targetId) {
                                    foundUndirected = true;
                                    break;
                                }
                            }
                            if(foundUndirected) {
println("----REPEAT_UND_PATH"+ctx.id()+"-"+target.id+"-"+firstStep.targetId+" with neighs size "+firstStep.neighbors.size()+" adds to RA "+Double.implicit_operator_as(1)/firstStep.neighbors.size());
                                continue;
                            }
println("----"+ctx.id()+"-"+target.id+"-"+firstStep.targetId+" with neighs size "+firstStep.neighbors.size()+" adds to RA "+Double.implicit_operator_as(1)/firstStep.neighbors.size());
                            CN_score++;
                            AA_score = AA_score + (1/(Math.log(firstStep.neighbors.size())));
                            RA_score = RA_score + (Double.implicit_operator_as(1)/firstStep.neighbors.size());
                            undirectedIds.add(firstStep.targetId);
                        }
                    }
println("-"+ctx.id()+"-"+ target.id + " CN score:"+ CN_score+" RA score:"+ RA_score+ " AA score:"+ AA_score);
                }

                //TODO: for all t in targets, calculate number of DD and DA paths that reach it. ded = |DD|/|D|, ind = |DA|/|D|
                //TODO: for all t in targets, che
                //TODO: numNodes*(numNodes-1)-|targets_clean| have weight 0.

//PRINT FOR DEBUGGING
//val m :Message = Message(ctx.id(),vertexData);
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
        for(p in m.messageGraph.range()){
            //for(s in m.messageGraph(p).path.range()){
            //    //Console.OUT.println(m.messageGraph(p).path(s).direction + " " + m.messageGraph(p).path(s).targetId);
            //}
        Console.OUT.println("--------------");
        }
    }

    static def printLocalGraph(lg :GrowableMemory[Step], depth :Long){
        if(depth==Long.implicit_operator_as(0)) println("-Printing localGraph");
        for(range in lg.range()){
            thisStep :Step = lg(range);
            for(i in 0..depth) print("-");
            if(thisStep.direction == true) print(">");
            if(thisStep.direction == false) print("<");
            println(thisStep.targetId);
            printLocalGraph(thisStep.neighbors,depth+1);
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

