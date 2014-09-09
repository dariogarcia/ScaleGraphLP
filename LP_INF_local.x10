/**
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

import org.scalegraph.xpregel.VertexContext;
import org.scalegraph.xpregel.XPregelGraph;

public class LP_INF_local {

    public static struct Message{
        val id:Long;
        val neighbours:GrowableMemory[Path];
        public def this(i: Long, n: GrowableMemory[Path]){
            id = i;
            neighbours = n;
        }
    }

    public static struct VertexData{
        val paths: GrowableMemory[Path];
        val candidates: GrowableMemory[Long];
        public def this(){
            paths = new GrowableMemory[Path]();
            candidates = new GrowableMemory[Long]();
        }
        public def this(ps: GrowableMemory[Path]){
            paths = ps;
            candidates = new GrowableMemory[Long]();
        }
        public def this(ps: GrowableMemory[Path], cs : GrowableMemory[Long]){
            paths = ps;
            candidates = cs;
        }
    }

    public static struct Path { 
        //A path is a sequence of steps
        val path: MemoryChunk[Step];
        public def this(){
            path = MemoryChunk.make[Step](2);
        }
    }

    public static struct Step {
        //Direction determines if the node is a descendant (0, a vertex pointing to self) or an ancestor (1, a vertex self points to)
        val direction: x10.lang.Boolean;
        //Id of vertex related though step
        val targetId: x10.lang.Long;
        public def this(d: x10.lang.Boolean, t: x10.lang.Long) {
            direction = d;
            targetId = t;
        }
    }
        
    public static def main(args:Array[String](1)) {
        val team = Team.WORLD;

        // load graph from CSV file
        val graph = Graph.make(CSV.read(args(0),[Type.Long as Int, Type.Long, Type.Byte],true));
        // create sparse matrix
        val csr = graph.createDistSparseMatrix[Byte](Config.get().dist1d(), "weight", true, true);

        // create xpregel instance
        val xpregel = XPregelGraph.make[VertexData, Byte](csr);
        xpregel.updateInEdge();

        xpregel.iterate[Message,Double]((ctx :VertexContext[VertexData, Byte, Message, Double], messages :MemoryChunk[Message]) => {
            var neighbours :GrowableMemory[Path] = new GrowableMemory[Path]();
            //first superstep, create all vertex with in-edges as path with one step: <0,Id>
	        //and all out-edges as path with one step: <1,Id>. Also, send paths to all vertices
            if(ctx.superstep() == 0){
                //Load all outgoing edges of vertex
                val tupleOut = ctx.outEdges();
Console.OUT.println(ctx.id() + " has # of OUT id edges:" + ctx.outEdgesId().size());
Console.OUT.println(ctx.id() + " has # of OUT val edges:" + ctx.outEdgesValue().size());
                //val idsOut = tupleOut.get1();
                //val weightsOut = tupleOut.get2();
                //For each vertex connected through an outgoing edges
                //for(idx in weightsOut.range()) {
//Console.OUT.println("out:"+idsOut(idx));
                    //If the vertex is connected through an edges with weight = 1
                    //if (weightsOut(idx).compareTo(1) == 0){
                        //Add the vertex id to the list of one path neighbors
                        //val s = Step(true,idsOut(idx));
                        //val p = Path();
                        //p.path(0) = s;
                        //neighbours.add(p);
                    //}
//else Console.OUT.println("out with a 0:"+idsOut(idx));
                //}
Console.OUT.println(ctx.id() + " has # of IN ids:" + ctx.inEdgesId().size());
Console.OUT.println(ctx.id() + " has # of IN val:" + ctx.inEdgesValue().size());
                //For each vertex connected through an ingoing edges
                //for(idx in ctx.inEdgesValue().range()) {
//Console.OUT.println("in:"+ctx.inEdgesId()(idx));
                    //If the vertex is connected through an edges with weight = 1
                    //if (ctx.inEdgesValue()(idx).compareTo(1) == 0){
                        //Add the vertex id to the list of one path neighbors
                        //val s = Step(false,ctx.inEdgesId()(idx));
                        //val p = Path();
                        //p.path(0) = s;
                        //neighbours.add(p);
                    //}
//else Console.OUT.println("in with a 0:"+ctx.inEdgesId()(idx));
                //}
                //Save the built list of one step neighbors
                val firstStepRes:VertexData = new VertexData(neighbours);
                ctx.setValue(firstStepRes);
                val m :Message = Message(ctx.id(),neighbours);
                //Send the list to all neighbors
                ctx.sendMessageToAllNeighbors(m);
            }
            //Second superstep: read the messages, extend the paths with the information arriving
            if(ctx.superstep() == 1){
                var targets :GrowableMemory[Long] = new GrowableMemory[Long]();
                //Load all one step neighbors
                val firstStepNeighbors:GrowableMemory[Path]  = ctx.value().paths;
                //For each message recieved
                for(mess in messages){
                    val messageId:Long = mess.id;
                    //Seek the sender in the firstStepNeighbors
                    for(rangePaths in firstStepNeighbors.range()){
                        val currentPath = firstStepNeighbors(rangePaths);
                        if(currentPath.path(0).targetId == messageId){
                            //Once found, create as many 2 steps paths as neighbors within the message
                            for(rangePaths2 in mess.neighbours.range()){
                                val currentPath2 = mess.neighbours(rangePaths2);
                                var fullPath:Path = Path();
                                val s1:Step = Step(currentPath.path(0).direction, currentPath.path(0).targetId);
                                fullPath.path(0) = s1;
                                val s2:Step = Step(currentPath2.path(0).direction, currentPath2.path(0).targetId);
                                fullPath.path(1) = s2;        
                                neighbours.add(fullPath);
                                targets.add(currentPath2.path(0).targetId);
                            }
                        }
                    }
                }
                val secondStepRes:VertexData = new VertexData(neighbours,targets);
                //TODO: for all t in targets, if t in firstneighbours and weight = 1, remove t from targets. If weight = 0 mark a TP. 
                //Is there a structure in x10 or scalegraph it is is fast to search??
                //TODO: for all t in targets, calculate number of DD and DA paths that reach it. ded = |DD|/|D|, ind = |DA|/|D|
                //TODO: for all t in targets, che
                //TODO: numNodes*(numNodes-1)-|targets_clean| have weight 0.

//PRINT FOR DEBUGGING
//val m :Message = Message(ctx.id(),neighbours);
//printNeighbourhood(m);
//END PRINT FOR DEBUGGING    

                ctx.setValue(secondStepRes);
                ctx.voteToHalt();
            }
	    },
        //I'm not sure what could I use the aggregator for.
        null,
        //Combiner CombinePaths should take various Message and append them into the same ... is it possible without losing the Ids??
        //The vertex could add its Id to every path before sending it to the combiner. But this increases the size of messages dramatically.
	    //(paths :MemoryChunk[Message]) => combinePaths(paths),
        (superstep :Int, someValue :Double) => (superstep >= 2));
    }

    static def printNeighbourhood(m:Message){
        Console.OUT.println("--------------");
        Console.OUT.println("Neighbourhood of node with Id:"+m.id);
        for(p in m.neighbours.range()){
            for(s in m.neighbours(p).path.range()){
                Console.OUT.println(m.neighbours(p).path(s).direction + " " + m.neighbours(p).path(s).targetId);
            }
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

