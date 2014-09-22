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
import x10.util.HashMap;
import org.scalegraph.util.GrowableMemory;
import org.scalegraph.util.MemoryChunk;
import org.scalegraph.graph.Graph;
import org.scalegraph.io.CSV;
import org.scalegraph.id.Type;
import org.scalegraph.Config;
import org.scalegraph.test.STest;

import org.scalegraph.xpregel.VertexContext;
import org.scalegraph.xpregel.XPregelGraph;

import org.scalegraph.io.FileWriter;
import org.scalegraph.io.FileMode;
import org.scalegraph.util.SStringBuilder;

public class LP_INF_local_recursive extends STest {

    public static struct HitRate{
        val tp :Long;
        val fp :Long;
        public def this(t :Long, f :Long){
            tp = t;
            fp = f;
        }
    }

    public static struct ScorePair{
        val scoreName :String;
        val weights :HashMap[Double,HitRate];
        public def this(s :String, w :HashMap[Double,HitRate]){
            scoreName = s;
            weights = w;
        }
    }

    public static struct PredictedLink{
        val id:Long;
        val TP:Boolean;
        public def this(i: Long, t: Boolean){
            id = i;
            TP = t;
        }
    }
    
    public static struct MessageReduce{
        val last_data: GrowableMemory[ScorePair];
        public def this(last: GrowableMemory[ScorePair]){
            last_data = last;
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

    //TODO: convert testCandidates into HashMap
    public static struct VertexData{
        val localGraph: GrowableMemory[Step];
        val testCandidates: GrowableMemory[Long];
        val candidates: GrowableMemory[Long];
        val Descendants: Long;
        val Ancestors: Long;
        val last_data: GrowableMemory[ScorePair];
        public def this(test: GrowableMemory[Long], st: GrowableMemory[Step], d: Long, a: Long){
            localGraph = st;
            testCandidates = test;
            candidates = new GrowableMemory[Long]();
            Descendants = d;
            Ancestors = a;
            last_data = new GrowableMemory[ScorePair]();
        }
        public def this(last: GrowableMemory[ScorePair]){
            localGraph = new GrowableMemory[Step]();
            testCandidates = new GrowableMemory[Long]();
            candidates = new GrowableMemory[Long]();
            Descendants = 0;
            Ancestors = 0;
            last_data = last;
        }
        public def this(){
            localGraph = new GrowableMemory[Step]();
            testCandidates = new GrowableMemory[Long]();
            candidates = new GrowableMemory[Long]();
            Descendants = 0;
            Ancestors = 0;
            last_data = new GrowableMemory[ScorePair]();
        }
    }

    //TODO: convert step into HashMap
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
        finish for(p in Team.WORLD.placeGroup()) at(p) async { flush(); }
    }

    public def run(args :Array[String](1)): Boolean {
        val team = Team.WORLD;
        // load graph from CSV file
        //Renumbering:To be enabled when using STRINGS!! 
        val graph = Graph.make(CSV.read(args(0),[Type.String, Type.String, Type.Byte],true), true);
        //val graph = Graph.make(CSV.read(args(0),[Type.Long as Int, Type.Long, Type.Byte],true));
        // create sparse matrix
        val csr = graph.createDistSparseMatrix[Byte](Config.get().dist1d(), "weight", true, false);
        // create xpregel instance
        val xpregel = XPregelGraph.make[VertexData, Byte](csr);
        //xpregel.setLogPrinter(Console.ERR, 0);
        xpregel.updateInEdge();
        xpregel.updateInEdgeAndValue();
        //val Vertices:Long = graph.numberOfVertices();
        //val TestEdgesWordnet :Long = 69857;
        //val TestEdgesCyc :Long = 34559;
        val TestEdges:Long = Long.parse(args(1));
        val actualVertices:Long = Long.parse(args(2));
        val Edges:Long = graph.numberOfEdges() - TestEdges;
        xpregel.iterate[Message,GrowableMemory[ScorePair]]((ctx :VertexContext[VertexData, Byte, Message, GrowableMemory[ScorePair]], messages :MemoryChunk[Message]) => {
            //first superstep, create all vertex with in-edges as path with one step: <0,Id>
	        //and all out-edges as path with one step: <1,Id>. Also, send paths to all vertices
            if(ctx.superstep() == 0){
                var localGraph :GrowableMemory[Step] = new GrowableMemory[Step]();
                var ancestors :Long = 0; var descendants :Long = 0;
                //Load all outgoing edges of vertex
                val tupleOut = ctx.outEdges();
                val idsOut = tupleOut.get1();
                val weightsOut = tupleOut.get2();
//TODO: Optimization: avoid storing twice data of nodes connected in various ways
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
                //Disconnected vertices vote to halt
                if(ancestors==Long.implicit_operator_as(0) && descendants==Long.implicit_operator_as(0)) {
                    ctx.setvalue(new VertexData());
                    ctx.voteToHalt();
                }
                //Save the built list of one step neighbors
                val firstStepRes:VertexData = new VertexData(testNeighs,localGraph,descendants,ancestors);
                ctx.setValue(firstStepRes);
                val m :Message = Message(ctx.id(),localGraph);
                //Send the list to all neighbors connected through positive link
                //ctx.sendMessageToAllNeighbors(m);
                for(idx in ctx.inEdgesId().range()) if(ctx.inEdgesValue()(idx).compareTo(1) == 0) ctx.sendMessage(ctx.inEdgesId()(idx),m);
                for(idx in idsOut.range())  if(weightsOut(idx).compareTo(1) == 0) ctx.sendMessage(idsOut(idx),m);
            }
            //Second superstep: read the messages, extend the localGraph with the information arriving
            if(ctx.superstep() == 1){
                var localGraphTargets :HashMap[Long,Boolean] = new HashMap[Long,Boolean]();
                //var localGraphTargets :GrowableMemory[Long] = new GrowableMemory[Long]();
                //Load all one step neighbors
                var vertexData :VertexData = ctx.value();
                val oldLocalGraph:GrowableMemory[Step]  = ctx.value().localGraph;
                //For each message recieved
                for(mess in messages){
                    val messageId:Long = mess.id_sender;
                    //If the sender is myself, skip it cause I already have that information
                    if(messageId == ctx.id()) continue;
                    //Seek the sender in the oldLocalGraph
                    for(rangeCurrentSteps in oldLocalGraph.range()){
                        val oldStep = oldLocalGraph(rangeCurrentSteps);
                        if(oldStep.targetId == messageId & vertexData.localGraph(rangeCurrentSteps).neighbors.size()==Long.implicit_operator_as(0)){
                            //Once found, extend the localGraph adding one additional step per neighbor within the message, (unless already extended)
                            for(rangeNewSteps in mess.messageGraph.range()){
                                vertexData.localGraph(rangeCurrentSteps).neighbors.add(new Step(mess.messageGraph(rangeNewSteps).direction, mess.messageGraph(rangeNewSteps).targetId));
                                //Unless already added or is self, add as target according to localGraph
                                var found :Boolean = false;
                                //for(target_idx in localGraphTargets.range()) if(localGraphTargets(target_idx)==mess.messageGraph(rangeNewSteps).targetId) found = true;
                                //if(!found & mess.messageGraph(rangeNewSteps).targetId != ctx.id()) localGraphTargets.add(mess.messageGraph(rangeNewSteps).targetId);
                                if(!localGraphTargets.containsKey(mess.messageGraph(rangeNewSteps).targetId) & mess.messageGraph(rangeNewSteps).targetId != ctx.id()) localGraphTargets.put(mess.messageGraph(rangeNewSteps).targetId, true);
                            }
                        }
                    }
                }
//printLocalGraph(vertexData.localGraph, 0);
                //For each possible target: If outedge from self exists, remove from targets. Else store id, if TP, |Desc| and |Ances|
                //Convert into HashMap id,bool (id,TP)
                var LPTargets :GrowableMemory[PredictedLink] = new GrowableMemory[PredictedLink]();
                for(currentTarget in localGraphTargets.keySet()){
                    var alreadyExistent :Boolean = false;
                    for(rangeCurrentSteps in vertexData.localGraph.range()){
                        val currentStep = vertexData.localGraph(rangeCurrentSteps);
                        if(currentTarget == currentStep.targetId & currentStep.direction){
                            alreadyExistent = true;
                            break;
                        }
                    }
                    if(alreadyExistent) continue;
                    //Find if TP or FP
                    var isTP :Boolean = false;
                    for(TPsIDX in vertexData.testCandidates.range()){
                        if(currentTarget == vertexData.testCandidates(TPsIDX)){
                            isTP = true;
                            break;
                        }
                    }
//bufferedPrintln("Adding target:"+currentTarget);
                    LPTargets.add(new PredictedLink(currentTarget,isTP));
                }

                output :GrowableMemory[ScorePair] = new GrowableMemory[ScorePair]();
                var cn_scores :HashMap[Double,HitRate] = new HashMap[Double,HitRate]();
                var ra_scores :HashMap[Double,HitRate] = new HashMap[Double,HitRate]();
                var aa_scores :HashMap[Double,HitRate] = new HashMap[Double,HitRate]();
                var inf_scores :HashMap[Double,HitRate] = new HashMap[Double,HitRate]();
                var inf_log_scores :HashMap[Double,HitRate] = new HashMap[Double,HitRate]();
                var inf_2d_scores :HashMap[Double,HitRate] = new HashMap[Double,HitRate]();
                var inf_log_2d_scores :HashMap[Double,HitRate] = new HashMap[Double,HitRate]();
                var tpsFound :Long = 0;
                //For each target, calculate # of directed (DD/AA/DA/AD) and undirected paths
                for(targetIDX in LPTargets.range()){
                    val target = LPTargets(targetIDX);
                    if(target.TP) tpsFound++;
                    val undirectedIds :GrowableMemory[Long] = new GrowableMemory[Long]();
                    var DD :Double = 0; var DA:Double = 0; var AD:Double = 0; var AA:Double = 0;
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
//bufferedPrintln("--Found path from "+ctx.id()+" to "+ target.id + " through " + firstStep.targetId);
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
//bufferedPrintln("----REPEAT_UND_PATH"+ctx.id()+"-"+target.id+"-"+firstStep.targetId+" with neighs size "+firstStep.neighbors.size()+" adds to RA "+Double.implicit_operator_as(1)/firstStep.neighbors.size());
                                continue;
                            }
//bufferedPrintln("----"+ctx.id()+"-"+target.id+"-"+firstStep.targetId+" with neighs size "+firstStep.neighbors.size()+" adds to RA "+Double.implicit_operator_as(1)/firstStep.neighbors.size());
//bufferedPrintln("----"+ctx.id()+"-"+target.id+"-"+firstStep.targetId+" with neighs size "+firstStep.neighbors.size()+" adds to AA "+Double.implicit_operator_as(1)/firstStep.neighbors.size());
                            CN_score++;
                            AA_score = AA_score + (1/(Math.log(firstStep.neighbors.size())));
                            RA_score = RA_score + (Double.implicit_operator_as(1)/firstStep.neighbors.size());
                            undirectedIds.add(firstStep.targetId);
                        }
                    }
                    //Calculate INF related scores
                    var ded_score :Double = 0; var ind_score :Double = 0; var INF_LOG_score :Double = 0; var INF_LOG_2D_score :Double = 0;
                    if(vertexData.Ancestors > 0){
                        ded_score = AA/vertexData.Ancestors;
                        INF_LOG_score = ded_score*Math.log10(vertexData.Ancestors);
                        INF_LOG_2D_score = (ded_score*Math.log10(vertexData.Ancestors))*2;
                        if(vertexData.Descendants > 0){
                            ind_score = DA/vertexData.Descendants;
                            INF_LOG_score = INF_LOG_score + ind_score*Math.log10(vertexData.Descendants);
                            INF_LOG_2D_score = INF_LOG_2D_score + ind_score*Math.log10(vertexData.Descendants);
                        }
                    }
                    else {
                        if(vertexData.Descendants > 0){
                            ind_score = DA/vertexData.Descendants;
                            INF_LOG_score = ind_score*Math.log10(vertexData.Descendants);
                            INF_LOG_2D_score = ind_score*Math.log10(vertexData.Descendants);
                        }
                    }
                    val INF_score = ded_score + ind_score;
                    val INF_2D_score = (ded_score*2) + ind_score;
                    //Store values 
                    if(cn_scores.containsKey(CN_score)){
                        if(target.TP) cn_scores.put(CN_score,new HitRate(cn_scores.get(CN_score)().tp+1,cn_scores.get(CN_score)().fp));
                        else cn_scores.put(CN_score,new HitRate(cn_scores.get(CN_score)().tp,cn_scores.get(CN_score)().fp+1));
                    }
                    else {
                        if(target.TP) cn_scores.put(CN_score,new HitRate(1,0));
                        else cn_scores.put(CN_score,new HitRate(0,1));
                    }
                    if(ra_scores.containsKey(RA_score)){
                        if(target.TP) ra_scores.put(RA_score,new HitRate(ra_scores.get(RA_score)().tp+1,ra_scores.get(RA_score)().fp));
                        else ra_scores.put(RA_score,new HitRate(ra_scores.get(RA_score)().tp,ra_scores.get(RA_score)().fp+1));
                    }
                    else {
                        if(target.TP) ra_scores.put(RA_score,new HitRate(1,0));
                        else ra_scores.put(RA_score,new HitRate(0,1));
                    }
                    if(aa_scores.containsKey(AA_score)){
                        if(target.TP) aa_scores.put(AA_score,new HitRate(aa_scores.get(AA_score)().tp+1,aa_scores.get(AA_score)().fp));
                        else aa_scores.put(AA_score,new HitRate(aa_scores.get(AA_score)().tp,aa_scores.get(AA_score)().fp+1));
                    }
                    else {
                        if(target.TP) aa_scores.put(AA_score,new HitRate(1,0));
                        else aa_scores.put(AA_score,new HitRate(0,1));
                    }
                    if(inf_scores.containsKey(INF_score)){
                        if(target.TP) inf_scores.put(INF_score,new HitRate(inf_scores.get(INF_score)().tp+1,inf_scores.get(INF_score)().fp));
                        else inf_scores.put(INF_score,new HitRate(inf_scores.get(INF_score)().tp,inf_scores.get(INF_score)().fp+1));
                    }
                    else {
                        if(target.TP) inf_scores.put(INF_score,new HitRate(1,0));
                        else inf_scores.put(INF_score,new HitRate(0,1));
                    }
                    if(inf_log_scores.containsKey(INF_LOG_score)){
                        if(target.TP) inf_log_scores.put(INF_LOG_score,new HitRate(inf_log_scores.get(INF_LOG_score)().tp+1,inf_log_scores.get(INF_LOG_score)().fp));
                        else inf_log_scores.put(INF_LOG_score,new HitRate(inf_log_scores.get(INF_LOG_score)().tp,inf_log_scores.get(INF_LOG_score)().fp+1));
                    }
                    else {
                        if(target.TP) inf_log_scores.put(INF_LOG_score,new HitRate(1,0));
                        else inf_log_scores.put(INF_LOG_score,new HitRate(0,1));
                    }
                    if(inf_2d_scores.containsKey(INF_2D_score)){
                        if(target.TP) inf_2d_scores.put(INF_2D_score,new HitRate(inf_2d_scores.get(INF_2D_score)().tp+1,inf_2d_scores.get(INF_2D_score)().fp));
                        else inf_2d_scores.put(INF_2D_score,new HitRate(inf_2d_scores.get(INF_2D_score)().tp,inf_2d_scores.get(INF_2D_score)().fp+1));
                    }
                    else {
                        if(target.TP) inf_2d_scores.put(INF_2D_score,new HitRate(1,0));
                        else inf_2d_scores.put(INF_2D_score,new HitRate(0,1));
                    }
                    if(inf_log_2d_scores.containsKey(INF_LOG_2D_score)){
                        if(target.TP) inf_log_2d_scores.put(INF_LOG_2D_score,new HitRate(inf_log_2d_scores.get(INF_LOG_2D_score)().tp+1,inf_log_2d_scores.get(INF_LOG_2D_score)().fp));
                        else inf_log_2d_scores.put(INF_LOG_2D_score,new HitRate(inf_log_2d_scores.get(INF_LOG_2D_score)().tp,inf_log_2d_scores.get(INF_LOG_2D_score)().fp+1));
                    }
                    else {
                        if(target.TP) inf_log_2d_scores.put(INF_LOG_2D_score,new HitRate(1,0));
                        else inf_log_2d_scores.put(INF_LOG_2D_score,new HitRate(0,1));
                    }
//bufferedPrintln("== "+ctx.id()+" has for weight "+INF_score+" TP:"+inf_scores.get(INF_score)().tp+" FP:"+inf_scores.get(INF_score)().fp);
                } 
                //Store 0 values, add unrelated TPs and FPs to the ones already found
                val unrelatedTPsAtZero = vertexData.testCandidates.size()-tpsFound;
                val unrelatedFPsAtZero = actualVertices - 1 - vertexData.Ancestors - unrelatedTPsAtZero - LPTargets.size();
//bufferedPrintln("=== "+actualVertices+" "+unrelatedTPsAtZero+" "+LPTargets.size());
                if(cn_scores.containsKey(0)) cn_scores.put(0,new HitRate(unrelatedTPsAtZero + cn_scores.get(0)().tp, unrelatedFPsAtZero + cn_scores.get(0)().fp));
                else cn_scores.put(0,new HitRate(unrelatedTPsAtZero, unrelatedFPsAtZero));
                if(ra_scores.containsKey(0)) ra_scores.put(0,new HitRate(unrelatedTPsAtZero + ra_scores.get(0)().tp, unrelatedFPsAtZero + ra_scores.get(0)().fp));
                else ra_scores.put(0,new HitRate(unrelatedTPsAtZero, unrelatedFPsAtZero));
                if(aa_scores.containsKey(0)) aa_scores.put(0,new HitRate(unrelatedTPsAtZero + aa_scores.get(0)().tp, unrelatedFPsAtZero + aa_scores.get(0)().fp));
                else aa_scores.put(0,new HitRate(unrelatedTPsAtZero, unrelatedFPsAtZero));
                if(inf_scores.containsKey(0)) inf_scores.put(0,new HitRate(unrelatedTPsAtZero + inf_scores.get(0)().tp, unrelatedFPsAtZero + inf_scores.get(0)().fp));
                else inf_scores.put(0,new HitRate(unrelatedTPsAtZero, unrelatedFPsAtZero));
                if(inf_log_scores.containsKey(0)) inf_log_scores.put(0,new HitRate(unrelatedTPsAtZero + inf_log_scores.get(0)().tp, unrelatedFPsAtZero + inf_log_scores.get(0)().fp));
                else inf_log_scores.put(0,new HitRate(unrelatedTPsAtZero, unrelatedFPsAtZero));
                if(inf_2d_scores.containsKey(0)) inf_2d_scores.put(0,new HitRate(unrelatedTPsAtZero + inf_2d_scores.get(0)().tp, unrelatedFPsAtZero + inf_2d_scores.get(0)().fp));
                else inf_2d_scores.put(0,new HitRate(unrelatedTPsAtZero, unrelatedFPsAtZero));
                if(inf_log_2d_scores.containsKey(0)) inf_log_2d_scores.put(0,new HitRate(unrelatedTPsAtZero + inf_log_2d_scores.get(0)().tp, unrelatedFPsAtZero + inf_log_2d_scores.get(0)().fp));
                else inf_log_2d_scores.put(0,new HitRate(unrelatedTPsAtZero, unrelatedFPsAtZero));
//bufferedPrintln("== "+ctx.id()+" has for weight 0 TP:"+cn_scores.get(0)().tp+" FP:"+cn_scores.get(0)().fp);
                //Store for aggregate
                output.add(new ScorePair("CN",cn_scores));
                output.add(new ScorePair("RA",ra_scores));
                output.add(new ScorePair("AA",aa_scores));
                output.add(new ScorePair("INF",inf_scores));
                output.add(new ScorePair("INF_LOG",inf_log_scores));
                output.add(new ScorePair("INF_2D",inf_2d_scores));
                output.add(new ScorePair("INF_LOG_2D",inf_log_2d_scores));
                ctx.setValue(new VertexData(output));

                ctx.voteToHalt();
            }
	    },
        null,
        //(aggregation :MemoryChunk[GrowableMemory[ScorePair]]) => predictionAggregator(aggregation),
        //TODO: use the combiner: CombinePaths should take various Message and append them into the same ... is it possible without losing the Ids? The vertex could add its Id to every path before sending it to the combiner. But this increases the size of messages dramatically.
	    //(paths :MemoryChunk[Message]) => combinePaths(paths),
        (superstep :Int, someValue :GrowableMemory[ScorePair]) => (superstep >= 2));

        //First part is done. In the second we reduce the data of all vertices by sending it to vertexId=0 and using combiner and compute.
        xpregel.setLogPrinter(Console.ERR, 0);
        xpregel.iterate[MessageReduce,GrowableMemory[ScorePair]]((ctx :VertexContext[VertexData, Byte, MessageReduce, GrowableMemory[ScorePair]], messages :MemoryChunk[MessageReduce]) => {
            //In the first superstep all vertices send their data to vertex 0
            if(ctx.superstep() == 0){
                if((ctx.value().last_data).size()==Long.implicit_operator_as(0)) ctx.voteToHalt();
                val m :MessageReduce = MessageReduce(ctx.value().last_data);
                ctx.sendMessage(0,m);
            }
            //In the second superstep the vertex with Id 0 combines all partially combined data and writes results
            if(ctx.superstep() == 1){
                if(messages.size()!=Long.implicit_operator_as(0)){
                    //For each score calculated
                    for(rangeScores in messages(0).last_data.range()){
//bufferedPrintln("---Going IN for score with idx:"+rangeScores+" total vertex providing data: "+messages.size());
                        //Obtain the unique list of weights and their combined tp/fp
                        var reduction :HashMap[Double,HitRate] = new HashMap[Double,HitRate]();
                        val name :String = messages(0).last_data(rangeScores).scoreName; 
                        val fw_roc = new FileWriter(name+"_points.roc", FileMode.Create);
                        val fw_pr = new FileWriter(name+"_points.pr", FileMode.Create);
                        //For each vertex
                        for(vertexRange in messages.range()){
                            val currentScorePair = messages(vertexRange).last_data(rangeScores);
//bufferedPrintln("Score Name: "+name+ " vertex idx:"+vertexRange);
                            //For each weight in scorePair
                            for(weight in currentScorePair.weights.keySet()){
                                //If weight existed, increase tp/fp counters, else add it
                                if(reduction.containsKey(weight)) {
//bufferedPrintln("Adding Weights found: "+weight+ " with TP:"+currentScorePair.weights.get(weight)().tp+" and FP:"+currentScorePair.weights.get(weight)().fp);
                                    reduction.put(weight, new HitRate(reduction.get(weight)().tp + currentScorePair.weights.get(weight)().tp , reduction.get(weight)().fp + currentScorePair.weights.get(weight)().fp));
//bufferedPrintln("Resultant Weights found: "+weight+ " with TP:"+reduction.get(weight)().tp+" and FP:"+reduction.get(weight)().fp);
                                }
                                else {
                                    reduction.put(weight, new HitRate(currentScorePair.weights.get(weight)().tp, currentScorePair.weights.get(weight)().fp));
//bufferedPrintln("New weights found: "+weight+ " with TP:"+currentScorePair.weights.get(weight)().tp+" and FP:"+currentScorePair.weights.get(weight)().fp);
                                }
                            }
                        }
                        val SP :ScorePair = new ScorePair(name,reduction);
                        //ret_val.add(SP);
//bufferedPrintln("Total weights found: "+reduction.size());
                        //Once we have everything reduced in var reduction, we can calculate the points: For each weight calculate the accumulated tp/fp
                        for(threshold in reduction.keySet()){
                            var tpTotal :Long = 0; var fpTotal :Long = 0; 
                            //Calculate against all weights
                            for(currentWeight in reduction.keySet()){
                                if(currentWeight>=threshold){
                                    tpTotal +=reduction.get(currentWeight)().tp;
                                    fpTotal +=reduction.get(currentWeight)().fp;
                                }
                            }
                            //Calculate points
//bufferedPrintln("Weight: "+threshold+" has a total FP:"+fpTotal+ " and TP:"+tpTotal+ " actualVertices:"+actualVertices+" Edges:"+Edges);
                            var roc_x :Double = fpTotal/Double.implicit_operator_as((actualVertices*(actualVertices-1)-Edges));
                            var roc_y :Double = tpTotal/Double.implicit_operator_as(TestEdges);
                            var pr_x :Double = tpTotal/Double.implicit_operator_as(TestEdges);
                            var pr_y :Double = tpTotal/Double.implicit_operator_as((tpTotal+fpTotal));
                            //Write points
                            val sb_roc = new SStringBuilder();
                            sb_roc.add(roc_x).add(" ").add(roc_y).add("\n");
                            fw_roc.write(sb_roc.result().bytes());
                            val sb_pr = new SStringBuilder();
                            sb_pr.add(pr_x).add(" ").add(pr_y).add("\n");
                            fw_pr.write(sb_pr.result().bytes());
                        }
                        fw_roc.close();
                        fw_pr.close();
                    }
                }
                ctx.voteToHalt();
            }
        },
        null,
        (allVertexPoints :MemoryChunk[MessageReduce]) :MessageReduce => {
bufferedPrintln("Entering combiner at " + here.id);
            if(allVertexPoints.size()==Long.implicit_operator_as(0)){
bufferedPrintln("Leaving combiner because void" + here.id);
                return new MessageReduce(new GrowableMemory[ScorePair]());
            }
            else bufferedPrintln("Number of elements" + allVertexPoints.size());
            ret_val :GrowableMemory[ScorePair] = new GrowableMemory[ScorePair]();
            //For each score calculated
try{
            for(rangeScores in allVertexPoints(0).last_data.range()){
//bufferedPrintln("---Going IN for score with idx:"+rangeScores+" total vertex providing data: "+allVertexPoints.size());
                //Obtain the unique list of weights and their combined tp/fp
                var reduction :HashMap[Double,HitRate] = new HashMap[Double,HitRate]();
                val name :String = allVertexPoints(0).last_data(rangeScores).scoreName; 
                //For each vertex
                for(vertexRange in allVertexPoints.range()){
                    val currentScorePair = allVertexPoints(vertexRange).last_data(rangeScores);
//bufferedPrintln("Score Name: "+name+ " vertex idx:"+vertexRange);
                    //For each weight in scorePair
                    for(weight in currentScorePair.weights.keySet()){
                        //If weight existed, increase tp/fp counters, else add it
                        if(reduction.containsKey(weight)) {
                            reduction.put(weight, new HitRate(reduction.get(weight)().tp + currentScorePair.weights.get(weight)().tp ,reduction.get(weight)().fp + currentScorePair.weights.get(weight)().fp));
//bufferedPrintln("Weights found: "+weight+ " with TP:"+reduction.get(weight)().tp+" and FP:"+reduction.get(weight)().fp);
                        }
                        else {
                            reduction.put(weight, new HitRate(currentScorePair.weights.get(weight)().tp, currentScorePair.weights.get(weight)().fp));
//bufferedPrintln("Weights found: "+weight+ " with TP:"+currentScorePair.weights.get(weight)().tp+" and FP:"+currentScorePair.weights.get(weight)().fp);
                        }
                    }
                }
                val SP :ScorePair = new ScorePair(name,reduction);
                ret_val.add(SP);
            }
}
catch (e :CheckedThrowable) { e.printStackTrace(); bufferedPrintln("Leaving combiner after exception " + here.id);}
bufferedPrintln("Leaving combiner at " + here.id);
            return new MessageReduce(ret_val);
        },
        (superstep :Int, someValue :GrowableMemory[ScorePair]) => (superstep >= 2));
        return true;
    }

    static def printLocalGraph(lg :GrowableMemory[Step], depth :Long){
        if(depth==Long.implicit_operator_as(0)) bufferedPrintln("-Printing localGraph");
        for(range in lg.range()){
            thisStep :Step = lg(range);
            for(i in 0..depth) print("-");
            if(thisStep.direction == true) print(">");
            if(thisStep.direction == false) print("<");
            bufferedPrintln(thisStep.targetId);
            printLocalGraph(thisStep.neighbors,depth+1);
        }
    }

}
