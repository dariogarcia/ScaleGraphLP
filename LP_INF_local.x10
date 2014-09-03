/**
 * Calculates the local INF score of all vertex
 *
 * V (vertices) stores a list of paths to other vertices of distance fixed: GrowableMemory[Path]
 * E (edges) do not store data: Double
 * M (messages) must pass lists of paths from one vertex to another: GrowableMemory[Path]
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

    public static struct Path {
        //A path is a sequence of steps
        val path: MemoryChunk[Step];
        public def this(){
            path = MemoryChunk.make[Step](2);
        }
    }

    public static struct Step {
        //Direction determines if the node is a descendant (0) or an ancestor (1)
        val direction: Boolean;
        //Id of node destination of step
        val targetId: x10.lang.Long;
        public def this(d: Boolean, t: x10.lang.Long) {
            direction = d;
            targetId = t;
        }
    }
        
    public static def main(args:Array[String](1)) {
        val team = Team.WORLD;

        // load graph from CSV file
        val graph = Graph.make(CSV.read(args(0),[Type.Long as Int, Type.Long, Type.Double],true));
        // create sparse matrix
        val csr = graph.createDistSparseMatrix[Double](Config.get().dist1d(), "weight", true, true);

        // create xpregel instance
        val xpregel = XPregelGraph.make[GrowableMemory[Path], Double](csr);
        xpregel.updateInEdge();

        // for each iteration
        xpregel.iterate[GrowableMemory[Path],Double]((ctx :VertexContext[GrowableMemory[Path], Double, Message, Double], messages :MemoryChunk[Message]) => {
            var neighbours :GrowableMemory[Path] = new GrowableMemory[Path]();
            //first superstep, create all vertex with in-edges as path with one step: <0,Id>
	        //and all out-edges as path with one step: <1,Id>
            //also, send paths to all vertices
            if(ctx.superstep() == 0){
                for(id in ctx.outEdgesId().range()) {
                    val s = Step(true,id);
                    val p = Path();
                    p.path(0) = s;
                    neighbours.add(p);
                }
                for(id in ctx.inEdgesId().range()) {
                    val s = Step(false,id);
                    val p = Path();
                    p.path(0) = s;
                    neighbours.add(p);
                }
                ctx.setValue(neighbours);
                val m :Message = Message(ctx.realId(),neighbours);
                ctx.sendMessageToAllNeighbors(m);
             }
            //second superstep: read the messages, extend the paths with the information arriving
            if(ctx.superstep() == 1){
                val firstStepNeighbors  = ctx.value();
                //val firstStepNeighbors  = new GrowableMemory[Path]();
                //firstStepNeighbors = ctx.value();
                //For each message recieved
                for(i in messages.range()){
                    val messageId:Long = messages(i).id;
                    //Seek it in the firstStepNeighbors
                    for(currentPath in firstStepNeighbors.range()){
                        if(firstStepNeighbors(currentPath).path(0).targetId == messageId){
                            //Once found, for each neighbour, add a new 2-step-path
                            for(k in messages(i).neighbours.range()){
                                var fullPath:Path = Path();
                                val s1:Step = Step(firstStepNeighbors(currentPath).path(0).direction, firstStepNeighbors(currentPath).path(0).targetId);
                                fullPath.path(0) = s1;
                                val s2:Step = Step(messages(i).neighbours(k).path(0).direction, messages(i).neighbours(k).path(0).targetId);
                                fullPath.path(1) = s2;        
                                neighbours.add(fullPath);
                            }
                        }
                    }
                }
                ctx.setValue(neighbours);

                //PRINT FOR DEBUGGING
                val m :Message = Message(ctx.realId(),neighbours);
                printNeighbourhood(m);
                //END PRINT FOR DEBUGGING    

                ctx.voteToHalt();
            }


	    },
        //I'm not sure what could I use the aggregator for.
        //null,
	    (dummy :MemoryChunk[Double]) => {},
        //Combiner CombinePaths should take various Message and append them into the same ... is it possible without losing the Ids??
        //The vertex could add its Id to every path before sending it to the combiner. But this increases the size of messages dramatically.
	    //(paths :MemoryChunk[Message]) => combinePaths(paths),
        //null,
        (superstep :Int, someValue :Long) => (superstep >= 2));
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
