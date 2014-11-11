/*
 * Calculates the local score of all vertices and builds the performance roc and pr graphs
 * 
 * 1st ITERATION: Evaluate all possible links and store the performance results on each vertex.
 * V (vertices): list of directly related nodes: HashMap[Long,NeighborData],
 *              list of vertices which are destination candidates: HashMap[Long,Boolean];
 *              number of direct descendants: Long;
 *              number of direct ancestors: Long;
 *              results of link evaluation, required for post-processing : GrowableMemory[ScorePair];
 * E (edges) :  1 if the edge is to be used for training, 0 if the edge is to be used for test: Byte
 * M (messages):id of sender :Long, and context of sender :HashMap[Long,NeighborData];
 * A (aggregator) 
 *
 * 2nd ITERATION: Combine the performance of all vertices, and reduce it to build the roc and pr graphs.
 * V (vertices): results achieved in all links with origin itself :GrowableMemory[ScorePair]
 * E (edges) :  
 * M (messages): same as V, which is passed to a single vertex for reduction :GrowableMemory[ScorePair]
 * A (aggregator) same as V, which is passed to a single vertex for reduction :GrowableMemory[ScorePair]
 *
 *
 * RUN INSTRUCTIONS
 * 1st Parameter: Number of TestEdges, that is edges with weight equal to 0. These should not include the ones regarding disconnected nodes
 * 2nd Parameter: Number of ActualNodes, that is nodes not disconnected according to edges with weight equal to 1. 
 */

import x10.util.Team;
import x10.util.HashMap;
import x10.util.Pair;
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


    public static struct Evidence{
            val TP :Boolean;
            val DD :Double;
            val DA :Double;
            val AD :Double;
            val AA :Double;
            val CN_score :Double;
            val AA_score :Double;
            val RA_score :Double;
            public def this(tp: Boolean){
                TP = tp;
                DD = 0;
                DA = 0;
                AD = 0;
                AA = 0;
                CN_score = 0;
                AA_score = 0;
                RA_score = 0;
            }
            public def this(tp :Boolean, dd :Double, da :Double, ad:Double, aa:Double, cn:double, aa_s:double, ra:double){
                TP = tp;
                DD = dd;
                DA = da;
                AD = ad;
                AA = aa;
                CN_score = cn;
                AA_score = aa_s;
                RA_score = ra;
            }
    }

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
        val weights :GrowableMemory[Pair[Double,HitRate] ];
        public def this(s :String, w :GrowableMemory[Pair[Double,HitRate] ]){
            scoreName = s;
            weights = w;
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
        val messageGraph :HashMap[Long,NeighborData];
        public def this(i: Long, n :HashMap[Long,NeighborData]){
            id_sender = i;
            messageGraph = n;
        }
    }

    public static struct VertexData{
        val localGraph :HashMap[Long,NeighborData];
        val localGraphOriginal :HashMap[Long,NeighborData];
        val testCandidates: GrowableMemory[Long];
        val Descendants: Long;
        val Ancestors: Long;
        val last_data: GrowableMemory[ScorePair];
        public def this(test: GrowableMemory[Long], st :HashMap[Long,NeighborData], orig :HashMap[Long,NeighborData], d: Long, a: Long){
            localGraph = st;
            localGraphOriginal = orig;
            testCandidates = test;
            Descendants = d;
            Ancestors = a;
            last_data = new GrowableMemory[ScorePair]();
        }
        //Constructor before return
        public def this(last: GrowableMemory[ScorePair], orig :HashMap[Long,NeighborData]){
            localGraph = new HashMap[Long,NeighborData]();
            localGraphOriginal = orig;
            testCandidates = new GrowableMemory[Long]();
            Descendants = 0;
            Ancestors = 0;
            last_data = last;
        }
        //Constructor used for empty return of disconnected vertices
        public def this(){
            localGraph = new HashMap[Long,NeighborData]();
            localGraphOriginal = new HashMap[Long,NeighborData]();
            Descendants = 0;
            Ancestors = 0;
            last_data = new GrowableMemory[ScorePair]();
            testCandidates = new GrowableMemory[Long]();
        }
    }

    public static struct NeighborData {
        //Direction determines if node is descendant (0, a vertex pointing to self) or ancestor (1, a vertex self points to). 2 means both
        val direction: x10.lang.Int;
        val neighbors :HashMap[Long,NeighborData];
        //val neighbors :GrowableMemory[Pair[Long,NeighborData] ];
        public def this(d: x10.lang.Int) {
            direction = d;
            //neighbors = new GrowableMemory[Pair[Long,NeighborData] ]();
            neighbors = null;
        }
        public def this(nd: NeighborData) {
            direction = nd.direction;
            neighbors = new HashMap[Long,NeighborData]();
            //neighbors = new GrowableMemory[Pair[Long,NeighborData] ]();
        }
//TODO:Watch out with this. Used in processing of messages as the begining of superstep 1. Its getting out of hand.
        public def this() {
            direction = -1;
            neighbors = new HashMap[Long,NeighborData]();
            //neighbors = new GrowableMemory[Pair[Long,NeighborData] ]();
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
        val Edges:Long = graph.numberOfEdges();
        //val Vertices:Long = graph.numberOfVertices();
        //val TestEdgesWordnet :Long = 69857;
        //val TestEdgesCyc :Long = 34559;
        val TestEdges:Long = Long.parse(args(1));
        val actualVertices:Long = Long.parse(args(2));
        //Number of message splits -1. Should be 1 or higher! Never 0.
        val splitMessage =  Int.parse(args(3));
        if(splitMessage < 1) {
            bufferedPrintln("Too few steps defined as third parameter. Minim steps is 1");
            return false;
        }
        xpregel.iterate[Message,GrowableMemory[ScorePair]]((ctx :VertexContext[VertexData, Byte, Message, GrowableMemory[ScorePair]], messages :MemoryChunk[Message]) => {
            //first superstep, create all vertex with in-edges as path with one step: <0,Id>
	        //and all out-edges as path with one step: <1,Id>. Also, send paths to all vertices
            if(ctx.superstep() == 0){
                var localGraph :HashMap[Long,NeighborData] = new HashMap[Long,NeighborData]();
                var localGraphOrig :HashMap[Long,NeighborData] = new HashMap[Long,NeighborData]();
                var ancestors :Long = 0; var descendants :Long = 0;
                //Load all outgoing edges of vertex
                val tupleOut = ctx.outEdges();
                val idsOut = tupleOut.get1();
                val weightsOut = tupleOut.get2();
                //Store those vertex linked by an outgoing edge which are to be used as test
                var testNeighs :GrowableMemory[Long] = new GrowableMemory[Long]();
                //For each vertex connected through an outgoing edges
                for(idx in weightsOut.range()) {
                    //If the vertex is connected through an edge with weight = 1 (train edge), add the vertex to list of neighbors
                    if(weightsOut(idx).compareTo(1) == 0){
                        //If it was already added, update 
                        if(localGraph.containsKey(idsOut(idx))){
                            if(localGraph.get(idsOut(idx))().direction == 0){
                                localGraph.put(idsOut(idx), new NeighborData(2));
                                localGraphOrig.put(idsOut(idx), new NeighborData(2));
                                ancestors++;
                            }
                        }
                        if(!localGraph.containsKey(idsOut(idx))){
                            localGraph.put(idsOut(idx), new NeighborData(1));
                            localGraphOrig.put(idsOut(idx), new NeighborData(1));
                            ancestors++;
                        }
                    }
                    //Otherwise add the vertex as a test neighbour
                    else testNeighs.add(idsOut(idx));
                }
                //For each vertex connected through an ingoing edges
                for(idx in ctx.inEdgesValue().range()) {
                    //If the vertex is connected through an edge with weight = 1, add the vertex to list of neighbors
                    if(ctx.inEdgesValue()(idx).compareTo(1) == 0){
                        if(localGraph.containsKey(ctx.inEdgesId()(idx))){
                            if(localGraph.get(ctx.inEdgesId()(idx))().direction == 1){
                                localGraph.put(ctx.inEdgesId()(idx),new NeighborData(2));
                                localGraphOrig.put(ctx.inEdgesId()(idx),new NeighborData(2));
                                descendants++;
                            }
                        }
                        if(!localGraph.containsKey(ctx.inEdgesId()(idx))){
                            localGraph.put(ctx.inEdgesId()(idx),new NeighborData(0));
                            localGraphOrig.put(ctx.inEdgesId()(idx),new NeighborData(0));
                            descendants++;
                        }
                    }
                }
                //Disconnected vertices vote to halt
                if(ancestors==Long.implicit_operator_as(0) && descendants==Long.implicit_operator_as(0)) {
                    ctx.setValue(new VertexData());
                    ctx.voteToHalt();
                }
                else{
                    //Save the built list of one step neighbors
                    val firstStepRes:VertexData = new VertexData(testNeighs,localGraph,localGraphOrig,descendants,ancestors);
                    ctx.setValue(firstStepRes);
                    val m :Message = Message(ctx.id(),localGraphOrig);
                    //Structure to make sure messages are sent only once per target
                    var bidiVertices :HashMap[Long,Boolean] = new HashMap[Long,Boolean]();
                    //Send the list to all neighbors connected through positive link within this iteration
                    for(idx in ctx.inEdgesId().range()) {
                        if(ctx.inEdgesValue()(idx).compareTo(1) == 0 & ctx.inEdgesId()(idx) % splitMessage == Long.implicit_operator_as(ctx.superstep())) {
                            ctx.sendMessage(ctx.inEdgesId()(idx),m);
                            bidiVertices.put(ctx.inEdgesId()(idx),false);
                        }
                    }
                    for(idx in idsOut.range())  {
                        if(weightsOut(idx).compareTo(1) == 0 & idsOut(idx) % splitMessage == Long.implicit_operator_as(ctx.superstep())) {
                            if(!bidiVertices.containsKey(idsOut(idx))) ctx.sendMessage(idsOut(idx),m);
                        }
                    }
                }
            }

            //[1:before-last] superstep: read the messages, extend the localGraph with the information arriving
            if(ctx.superstep() > 0 & ctx.superstep() < splitMessage){
                val m :Message = Message(ctx.id(),ctx.value().localGraphOriginal);
                val tupleOut = ctx.outEdges();
                val idsOut = tupleOut.get1();
                val weightsOut = tupleOut.get2();
                //Structure to make sure messages are sent only once per target
                var bidiVertices :HashMap[Long,Boolean] = new HashMap[Long,Boolean]();
                //Send the list to all neighbors connected through positive link within this iteration
                for(idx in ctx.inEdgesId().range()) {
                    if(ctx.inEdgesValue()(idx).compareTo(1) == 0 & ctx.inEdgesId()(idx) % splitMessage == Long.implicit_operator_as(ctx.superstep())) {
                        ctx.sendMessage(ctx.inEdgesId()(idx),m);
                            bidiVertices.put(ctx.inEdgesId()(idx),false);
                    }
                }
                for(idx in idsOut.range())  {
                    if(weightsOut(idx).compareTo(1) == 0 & idsOut(idx) % splitMessage == Long.implicit_operator_as(ctx.superstep())) {
                        if(!bidiVertices.containsKey(idsOut(idx))) ctx.sendMessage(idsOut(idx),m);
                    }
                }
                //If its messages were not sent the previous superstep
                if(ctx.id() % splitMessage!=Long.implicit_operator_as(ctx.superstep())-1) {
                    return;
                    //Not right to halt! nodes with no incoming vertices will not be evaluated, and vertices with info still to be sent wont send it
                }

                else {
                    //Load all one step neighbors
                    var vertexData :VertexData = ctx.value();
                    //NULL_CODE: Initialize all HashMaps
                    for(currentEntry in vertexData.localGraph.entries()){
                        if(currentEntry.getValue().neighbors == null){
                             currentEntry.setValue(new NeighborData(currentEntry.getValue()));
                        }
                    }
                    var LPTargetsEvidence :HashMap[Long,Evidence] = new HashMap[Long,Evidence] ();
                    output :GrowableMemory[ScorePair] = new GrowableMemory[ScorePair]();
                    var cn_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                    var ra_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                    var aa_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                    var inf_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                    var inf_log_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                    var inf_2d_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                    var inf_log_2d_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                    var tpsFound :Long = 0;
                    //Insert score 0 on first pos of all scores
                    cn_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                    ra_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                    aa_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                    inf_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                    inf_log_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                    inf_2d_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                    inf_log_2d_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                    //For each message recieved
                    for(mess in messages){
                        val first_key = mess.id_sender;
                        //TODO: Moving this check to the sending (ss=0) may slightly reduce imbalance
                        //If the sender is myself, skip it cause I already have that information
                        if(first_key == ctx.id()) continue;
                        val  firstStep = vertexData.localGraph(first_key)();
                        //If this vertex has not been updated yet, read all paths from its message. Avoid duplicate messages: A<--->B
                        if(firstStep.neighbors.size()==0){
                            val first_dir = firstStep.direction;
                            val first_degree = mess.messageGraph.size();
                            for(secondStep in mess.messageGraph.entries()){
                                //TODO: Is this necessary?
                                //vertexData.localGraph(messageId)().neighbors.put(newStep.getKey(),newStep.getValue());
                                val second_key = secondStep.getKey();
                                //If valid target !(self or direct out-neighbor)
                                if(vertexData.localGraph.containsKey(second_key)){
                                    if(vertexData.localGraph.get(second_key)().direction > 0) continue;
                                }
                                if(second_key != ctx.id()){
                                    val second_dir = secondStep.getValue().direction;
                                    //If not yet added, add as target and if TP. Initialize scores
                                    if(!LPTargetsEvidence.containsKey(second_key)) {
                                        var testFound :Boolean = false;
                                        for(testIDX in vertexData.testCandidates.range()) {
                                            if(vertexData.testCandidates(testIDX)==second_key) {
                                                testFound = true;
                                                break;
                                            }
                                        }
                                        var dd :Double=0 , da :Double=0, ad:Double=0, aa:Double=0, cn :Double = 0, aa_s :Double = 0, ra :Double = 0;
                                        if(second_dir == 0 & first_dir == 0) dd = dd+1;
                                        else if(second_dir == 1 & first_dir == 0) da = da+1;
                                        else if(second_dir == 1 & first_dir == 1) aa = aa+1;
                                        else if(second_dir == 0 & first_dir == 1) ad = ad+1;
                                        else if(second_dir == 0 & first_dir == 2) {dd = dd+1; ad = ad+1;}
                                        else if(second_dir == 1 & first_dir == 2) {da = da+1; aa = aa+1;}
                                        else if(second_dir == 2 & first_dir == 0) {da = da+1; dd = dd+1;}
                                        else if(second_dir == 2 & first_dir == 1) {aa = aa+1; ad = ad+1;}
                                        else if(second_dir == 2 & first_dir == 2) {dd = dd+1; ad = ad+1; da = da+1; aa = aa+1;}
                                        cn = 1;
                                        aa_s = (1/(Math.log(first_degree)));
                                        ra = (Double.implicit_operator_as(1)/first_degree);
                                        LPTargetsEvidence.put(second_key,new Evidence(testFound,dd,da,ad,aa,cn,aa_s,ra));
//bufferedPrintln("SOURCE:"+ctx.id()+" PATH:"+first_key+" TARGET:"+second_key+" TP:"+testFound+" FistDegree:"+first_degree+" DD:"+dd+" DA:"+da+" AD:"+ad+" AA:"+aa+" CN:"+cn+" AA:"+aa+" RA:"+ra);
                                    }
                                    //Else new path, increase scores
                                    else{
                                        val evidence = LPTargetsEvidence(second_key)();
                                        var dd :Double=evidence.DD , da :Double=evidence.DA, ad:Double=evidence.AD, aa:Double=evidence.AA, cn :Double = 0, aa_s :Double = 0, ra :Double = 0;
                                        if(second_dir == 0 & first_dir == 0) dd = dd+1;
                                        else if(second_dir == 1 & first_dir == 0) da = da+1;
                                        else if(second_dir == 1 & first_dir == 1) aa = aa+1;
                                        else if(second_dir == 0 & first_dir == 1) ad = ad+1;
                                        else if(second_dir == 0 & first_dir == 2) {dd = dd+1; ad = ad+1;}
                                        else if(second_dir == 1 & first_dir == 2) {da = da+1; aa = aa+1;}
                                        else if(second_dir == 2 & first_dir == 0) {da = da+1; dd = dd+1;}
                                        else if(second_dir == 2 & first_dir == 1) {aa = aa+1; ad = ad+1;}
                                        else if(second_dir == 2 & first_dir == 2) {dd = dd+1; ad = ad+1; 
                                                                                 da = da+1; aa = aa+1;}
                                        cn = evidence.CN_score+1;
                                        aa_s = evidence.AA_score + (1/(Math.log(first_degree)));
                                        ra = evidence.RA_score + (Double.implicit_operator_as(1)/first_degree);
//bufferedPrintln("SOURCE:"+ctx.id()+" PATH:"+first_key+" TARGET:"+second_key+" TP:"+evidence.TP+" FistDegree:"+first_degree+" DD:"+dd+" DA:"+da+" AD:"+ad+" AA:"+aa+" CN:"+cn+" AA:"+aa+" RA:"+ra);
                                        LPTargetsEvidence.put(second_key, new Evidence(evidence.TP,dd,da,ad,aa,cn,aa_s,ra));
                                    }
                                }
                            }
                        }
                    }
                        
                    //Once we have all the evidence, calculate all weights
                    for(evidenceKey in LPTargetsEvidence.keySet()){ 
                        val evidence = LPTargetsEvidence(evidenceKey)();
                        if(evidence.TP) tpsFound++;
                        //Calculate INF related scores
                        var ded_score :Double = 0; var ind_score :Double = 0; var INF_LOG_score :Double = 0; var INF_LOG_2D_score :Double = 0;
                        if(vertexData.Ancestors > 0){
                            ded_score = evidence.AA/vertexData.Ancestors;
                            INF_LOG_score = ded_score*Math.log10(vertexData.Ancestors);
                            INF_LOG_2D_score = (ded_score*Math.log10(vertexData.Ancestors))*2;
                            if(vertexData.Descendants > 0){
                                ind_score = evidence.DA/vertexData.Descendants;
                                INF_LOG_score = INF_LOG_score + ind_score*Math.log10(vertexData.Descendants);
                                INF_LOG_2D_score = INF_LOG_2D_score + ind_score*Math.log10(vertexData.Descendants);
                            }
                        }
                        else {
                            if(vertexData.Descendants > 0){
                                ind_score = evidence.DA/vertexData.Descendants;
                                INF_LOG_score = ind_score*Math.log10(vertexData.Descendants);
                                INF_LOG_2D_score = ind_score*Math.log10(vertexData.Descendants);
                            }
                        }
                        val INF_score = ded_score + ind_score;
                        val INF_2D_score = (ded_score*2) + ind_score;
                        //Store values
                        var found :Boolean = false;
                        for(scoreIDX in cn_scores.range()){
                            if(cn_scores(scoreIDX).first == evidence.CN_score) {
                                found = true;
                                if(evidence.TP) cn_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.CN_score,new HitRate(cn_scores(scoreIDX).second.tp+1,cn_scores(scoreIDX).second.fp));
                                else cn_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.CN_score, new HitRate(cn_scores(scoreIDX).second.tp,cn_scores(scoreIDX).second.fp+1));
                                break;
                            }
                        }
                        if(!found) {
                            if(evidence.TP) cn_scores.add(new Pair[Double,HitRate] (evidence.CN_score,new HitRate(1,0)));
                            else cn_scores.add(new Pair[Double,HitRate] (evidence.CN_score,new HitRate(0,1)));
                        }
                        found = false;
                        for(scoreIDX in aa_scores.range()){
                            if(aa_scores(scoreIDX).first == evidence.AA_score) {
                                found = true;
                                if(evidence.TP) aa_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.AA_score,new HitRate(aa_scores(scoreIDX).second.tp+1,aa_scores(scoreIDX).second.fp));
                                else aa_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.AA_score, new HitRate(aa_scores(scoreIDX).second.tp,aa_scores(scoreIDX).second.fp+1));
                                break;
                            }
                        }
                        if(!found) {
                            if(evidence.TP) aa_scores.add(new Pair[Double,HitRate] (evidence.AA_score,new HitRate(1,0)));
                            else aa_scores.add(new Pair[Double,HitRate] (evidence.AA_score,new HitRate(0,1)));
                        }
                        found = false;
                        for(scoreIDX in ra_scores.range()){
                            if(ra_scores(scoreIDX).first == evidence.RA_score) {
                                found = true;
                                if(evidence.TP) ra_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.RA_score,new HitRate(ra_scores(scoreIDX).second.tp+1,ra_scores(scoreIDX).second.fp));
                                else ra_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.RA_score, new HitRate(ra_scores(scoreIDX).second.tp,ra_scores(scoreIDX).second.fp+1));
                                break;
                            }
                        }
                        if(!found) {
                            if(evidence.TP) ra_scores.add(new Pair[Double,HitRate] (evidence.RA_score,new HitRate(1,0)));
                            else ra_scores.add(new Pair[Double,HitRate] (evidence.RA_score,new HitRate(0,1)));
                        }
                        found = false;
                        for(scoreIDX in inf_scores.range()){
                            if(inf_scores(scoreIDX).first == INF_score) {
                                found = true;
                                if(evidence.TP) inf_scores(scoreIDX) = new Pair[Double,HitRate] (INF_score,new HitRate(inf_scores(scoreIDX).second.tp+1,inf_scores(scoreIDX).second.fp));
                                else inf_scores(scoreIDX) = new Pair[Double,HitRate] (INF_score, new HitRate(inf_scores(scoreIDX).second.tp,inf_scores(scoreIDX).second.fp+1));
                                break;
                            }
                        }
                        if(!found) {
                            if(evidence.TP) inf_scores.add(new Pair[Double,HitRate] (INF_score,new HitRate(1,0)));
                            else inf_scores.add(new Pair[Double,HitRate] (INF_score,new HitRate(0,1)));
                        }
                        found = false;
                        for(scoreIDX in inf_log_scores.range()){
                            if(inf_log_scores(scoreIDX).first == INF_LOG_score) {
                                found = true;
                                if(evidence.TP) inf_log_scores(scoreIDX) = new Pair[Double,HitRate] (INF_LOG_score,new HitRate(inf_log_scores(scoreIDX).second.tp+1,inf_log_scores(scoreIDX).second.fp));
                                else inf_log_scores(scoreIDX) = new Pair[Double,HitRate] (INF_LOG_score, new HitRate(inf_log_scores(scoreIDX).second.tp,inf_log_scores(scoreIDX).second.fp+1));
                                break;
                            }
                        }
                        if(!found) {
                            if(evidence.TP) inf_log_scores.add(new Pair[Double,HitRate] (INF_LOG_score,new HitRate(1,0)));
                            else inf_log_scores.add(new Pair[Double,HitRate] (INF_LOG_score,new HitRate(0,1)));
                        }
                        found = false;
                        for(scoreIDX in inf_2d_scores.range()){
                            if(inf_2d_scores(scoreIDX).first == INF_2D_score) {
                                found = true;
                                if(evidence.TP) inf_2d_scores(scoreIDX) = new Pair[Double,HitRate] (INF_2D_score,new HitRate(inf_2d_scores(scoreIDX).second.tp+1,inf_2d_scores(scoreIDX).second.fp));
                                else inf_2d_scores(scoreIDX) = new Pair[Double,HitRate] (INF_2D_score, new HitRate(inf_2d_scores(scoreIDX).second.tp,inf_2d_scores(scoreIDX).second.fp+1));
                                break;
                            }
                        }
                        if(!found) {
                            if(evidence.TP) inf_2d_scores.add(new Pair[Double,HitRate] (INF_2D_score,new HitRate(1,0)));
                            else inf_2d_scores.add(new Pair[Double,HitRate] (INF_2D_score,new HitRate(0,1)));
                        }
                        found = false;
                        for(scoreIDX in inf_log_2d_scores.range()){
                            if(inf_log_2d_scores(scoreIDX).first == INF_LOG_2D_score) {
                                found = true;
                                if(evidence.TP) inf_log_2d_scores(scoreIDX) = new Pair[Double,HitRate] (INF_LOG_2D_score,new HitRate(inf_log_2d_scores(scoreIDX).second.tp+1,inf_log_2d_scores(scoreIDX).second.fp));
                                else inf_log_2d_scores(scoreIDX) = new Pair[Double,HitRate] (INF_LOG_2D_score, new HitRate(inf_log_2d_scores(scoreIDX).second.tp,inf_log_2d_scores(scoreIDX).second.fp+1));
                                break;
                            }
                        }
                        if(!found) {
                            if(evidence.TP) inf_log_2d_scores.add(new Pair[Double,HitRate] (INF_LOG_2D_score,new HitRate(1,0)));
                            else inf_log_2d_scores.add(new Pair[Double,HitRate] (INF_LOG_2D_score,new HitRate(0,1)));
                        }
//bufferedPrintln("== "+ctx.id()+" has for weight "+INF_score+" TP:"+inf_scores.get(INF_score)().tp+" FP:"+inf_scores.get(INF_score)().fp);
                    } 
                    //Store 0 values, add unrelated TPs and FPs to the ones already found
                    val unrelatedTPsAtZero = vertexData.testCandidates.size()-tpsFound;
                    val unrelatedFPsAtZero = actualVertices - 1 - vertexData.Ancestors - unrelatedTPsAtZero - LPTargetsEvidence.size();
//bufferedPrintln("=== "+actualVertices+" "+unrelatedTPsAtZero+" "+LPTargets.size());
                    cn_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + cn_scores(0).second.tp, unrelatedFPsAtZero + cn_scores(0).second.fp));
                    ra_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + ra_scores(0).second.tp, unrelatedFPsAtZero + ra_scores(0).second.fp));
                    aa_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + aa_scores(0).second.tp, unrelatedFPsAtZero + aa_scores(0).second.fp));
                    inf_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + inf_scores(0).second.tp, unrelatedFPsAtZero + inf_scores(0).second.fp));
                    inf_log_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + inf_log_scores(0).second.tp, unrelatedFPsAtZero + inf_log_scores(0).second.fp));
                    inf_2d_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + inf_2d_scores(0).second.tp, unrelatedFPsAtZero + inf_2d_scores(0).second.fp));
                    inf_log_2d_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + inf_log_2d_scores(0).second.tp, unrelatedFPsAtZero + inf_log_2d_scores(0).second.fp));
//bufferedPrintln("== "+ctx.id()+" has for weight 0 TP:"+cn_scores.get(0)().tp+" FP:"+cn_scores.get(0)().fp);
                    //Store for aggregate
                    output.add(new ScorePair("CN",cn_scores));
                    output.add(new ScorePair("RA",ra_scores));
                    output.add(new ScorePair("AA",aa_scores));
                    output.add(new ScorePair("INF",inf_scores));
                    output.add(new ScorePair("INF_LOG",inf_log_scores));
                    output.add(new ScorePair("INF_2D",inf_2d_scores));
                    output.add(new ScorePair("INF_LOG_2D",inf_log_2d_scores));
                    ctx.setValue(new VertexData(output,new HashMap[Long,NeighborData]()));
                    return;
                }
            }
            //Last superstep: read the messages, extend the localGraph with the information arriving
            if(ctx.superstep() == splitMessage){
                //If its messages were not sent the previous superstep
                if(ctx.id() % splitMessage!=Long.implicit_operator_as(ctx.superstep())-1) {
//bufferedPrintln("-Vertex :"+ctx.id()+" votes to halt in superstep:"+ctx.superstep());
                    return;
                    //Not right to halt! nodes with no incoming vertices will not be evaluated
                }
//bufferedPrintln("-Vertex :"+ctx.id()+" enters superstep:"+ctx.superstep());
                //Load all one step neighbors
                var vertexData :VertexData = ctx.value();
                //NULL_CODE: Initialize all HashMaps
                for(currentEntry in vertexData.localGraph.entries()){
                    if(currentEntry.getValue().neighbors == null){
                         currentEntry.setValue(new NeighborData(currentEntry.getValue()));
                    }
                }
                var LPTargetsEvidence :HashMap[Long,Evidence] = new HashMap[Long,Evidence] ();
                output :GrowableMemory[ScorePair] = new GrowableMemory[ScorePair]();
                var cn_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                var ra_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                var aa_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                var inf_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                var inf_log_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                var inf_2d_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                var inf_log_2d_scores :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                var tpsFound :Long = 0;
                //Insert score 0 on first pos of all scores
                cn_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                ra_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                aa_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                inf_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                inf_log_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                inf_2d_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                inf_log_2d_scores.add(new Pair[Double,HitRate] (0,new HitRate(0,0))); 
                //For each message recieved
                for(mess in messages){
                    val first_key = mess.id_sender;
                    //TODO: Moving this check to the sending (ss=0) may slightly reduce imbalance
                    //If the sender is myself, skip it cause I already have that information
                    if(first_key == ctx.id()) continue;
                    val  firstStep = vertexData.localGraph(first_key)();
                    //If this vertex has not been updated yet, read all paths from its message
                    if(firstStep.neighbors.size()==0){
                        val first_dir = firstStep.direction;
                        val first_degree = mess.messageGraph.size();
                        for(secondStep in mess.messageGraph.entries()){
                            //TODO: Is this necessary?
                            //vertexData.localGraph(messageId)().neighbors.put(newStep.getKey(),newStep.getValue());
                            val second_key = secondStep.getKey();
                            //If valid target !(self or direct out-neighbor)
                            if(vertexData.localGraph.containsKey(second_key)){
                                if(vertexData.localGraph.get(second_key)().direction > 0) continue;
                            }
                            if(second_key != ctx.id()){
                                val second_dir = secondStep.getValue().direction;
                                //If not yet added, add as target and if TP. Initialize scores
                                if(!LPTargetsEvidence.containsKey(second_key)) {
                                    var testFound :Boolean = false;
                                    for(testIDX in vertexData.testCandidates.range()) {
                                        if(vertexData.testCandidates(testIDX)==second_key) {
                                            testFound = true;
                                            break;
                                        }
                                    }
                                    var dd :Double=0 , da :Double=0, ad:Double=0, aa:Double=0, cn :Double = 0, aa_s :Double = 0, ra :Double = 0;
                                    if(second_dir == 0 & first_dir == 0) dd = dd+1;
                                    else if(second_dir == 1 & first_dir == 0) da = da+1;
                                    else if(second_dir == 1 & first_dir == 1) aa = aa+1;
                                    else if(second_dir == 0 & first_dir == 1) ad = ad+1;
                                    else if(second_dir == 0 & first_dir == 2) {dd = dd+1; ad = ad+1;}
                                    else if(second_dir == 1 & first_dir == 2) {da = da+1; aa = aa+1;}
                                    else if(second_dir == 2 & first_dir == 0) {da = da+1; dd = dd+1;}
                                    else if(second_dir == 2 & first_dir == 1) {aa = aa+1; ad = ad+1;}
                                    else if(second_dir == 2 & first_dir == 2) {dd = dd+1; ad = ad+1; da = da+1; aa = aa+1;}
                                    cn = 1;
                                    aa_s = (1/(Math.log(first_degree)));
                                    ra = (Double.implicit_operator_as(1)/first_degree);
//bufferedPrintln("SOURCE:"+ctx.id()+" PATH:"+first_key+" TARGET:"+second_key+" TP:"+testFound+" FistDegree:"+first_degree+" DD:"+dd+" DA:"+da+" AD:"+ad+" AA:"+aa+" CN:"+cn+" AA:"+aa+" RA:"+ra);
                                    LPTargetsEvidence.put(second_key,new Evidence(testFound,dd,da,ad,aa,cn,aa_s,ra));
                                }
                                //Else new path, increase scores
                                else{
                                    val evidence = LPTargetsEvidence(second_key)();
                                    var dd :Double=evidence.DD , da :Double=evidence.DA, ad:Double=evidence.AD, aa:Double=evidence.AA, cn :Double = 0, aa_s :Double = 0, ra :Double = 0;
                                    if(second_dir == 0 & first_dir == 0) dd = dd+1;
                                    else if(second_dir == 1 & first_dir == 0) da = da+1;
                                    else if(second_dir == 1 & first_dir == 1) aa = aa+1;
                                    else if(second_dir == 0 & first_dir == 1) ad = ad+1;
                                    else if(second_dir == 0 & first_dir == 2) {dd = dd+1; ad = ad+1;}
                                    else if(second_dir == 1 & first_dir == 2) {da = da+1; aa = aa+1;}
                                    else if(second_dir == 2 & first_dir == 0) {da = da+1; dd = dd+1;}
                                    else if(second_dir == 2 & first_dir == 1) {aa = aa+1; ad = ad+1;}
                                    else if(second_dir == 2 & first_dir == 2) {dd = dd+1; ad = ad+1; 
                                                                             da = da+1; aa = aa+1;}
                                    cn = evidence.CN_score+1;
                                    aa_s = evidence.AA_score + (1/(Math.log(first_degree)));
                                    ra = evidence.RA_score + (Double.implicit_operator_as(1)/first_degree);
//bufferedPrintln("SOURCE:"+ctx.id()+" PATH:"+first_key+" TARGET:"+second_key+" TP:"+evidence.TP+" FistDegree:"+first_degree+" DD:"+dd+" DA:"+da+" AD:"+ad+" AA:"+aa+" CN:"+cn+" AA:"+aa+" RA:"+ra);
                                    LPTargetsEvidence.put(second_key, new Evidence(evidence.TP,dd,da,ad,aa,cn,aa_s,ra));
                                }
                            }
                        }
                    }
                }
                    
                //Once we have all the evidence, calculate all weights
                for(evidenceKey in LPTargetsEvidence.keySet()){ 
                    val evidence = LPTargetsEvidence(evidenceKey)();
                    if(evidence.TP) tpsFound++;
                    //Calculate INF related scores
                    var ded_score :Double = 0; var ind_score :Double = 0; var INF_LOG_score :Double = 0; var INF_LOG_2D_score :Double = 0;
                    if(vertexData.Ancestors > 0){
                        ded_score = evidence.AA/vertexData.Ancestors;
                        INF_LOG_score = ded_score*Math.log10(vertexData.Ancestors);
                        INF_LOG_2D_score = (ded_score*Math.log10(vertexData.Ancestors))*2;
                        if(vertexData.Descendants > 0){
                            ind_score = evidence.DA/vertexData.Descendants;
                            INF_LOG_score = INF_LOG_score + ind_score*Math.log10(vertexData.Descendants);
                            INF_LOG_2D_score = INF_LOG_2D_score + ind_score*Math.log10(vertexData.Descendants);
                        }
                    }
                    else {
                        if(vertexData.Descendants > 0){
                            ind_score = evidence.DA/vertexData.Descendants;
                            INF_LOG_score = ind_score*Math.log10(vertexData.Descendants);
                            INF_LOG_2D_score = ind_score*Math.log10(vertexData.Descendants);
                        }
                    }
                    val INF_score = ded_score + ind_score;
                    val INF_2D_score = (ded_score*2) + ind_score;
                    //Store values
                    var found :Boolean = false;
                    for(scoreIDX in cn_scores.range()){
                        if(cn_scores(scoreIDX).first == evidence.CN_score) {
                            found = true;
                            if(evidence.TP) cn_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.CN_score,new HitRate(cn_scores(scoreIDX).second.tp+1,cn_scores(scoreIDX).second.fp));
                            else cn_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.CN_score, new HitRate(cn_scores(scoreIDX).second.tp,cn_scores(scoreIDX).second.fp+1));
                            break;
                        }
                    }
                    if(!found) {
                        if(evidence.TP) cn_scores.add(new Pair[Double,HitRate] (evidence.CN_score,new HitRate(1,0)));
                        else cn_scores.add(new Pair[Double,HitRate] (evidence.CN_score,new HitRate(0,1)));
                    }
                    found = false;
                    for(scoreIDX in aa_scores.range()){
                        if(aa_scores(scoreIDX).first == evidence.AA_score) {
                            found = true;
                            if(evidence.TP) aa_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.AA_score,new HitRate(aa_scores(scoreIDX).second.tp+1,aa_scores(scoreIDX).second.fp));
                            else aa_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.AA_score, new HitRate(aa_scores(scoreIDX).second.tp,aa_scores(scoreIDX).second.fp+1));
                            break;
                        }
                    }
                    if(!found) {
                        if(evidence.TP) aa_scores.add(new Pair[Double,HitRate] (evidence.AA_score,new HitRate(1,0)));
                        else aa_scores.add(new Pair[Double,HitRate] (evidence.AA_score,new HitRate(0,1)));
                    }
                    found = false;
                    for(scoreIDX in ra_scores.range()){
                        if(ra_scores(scoreIDX).first == evidence.RA_score) {
                            found = true;
                            if(evidence.TP) ra_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.RA_score,new HitRate(ra_scores(scoreIDX).second.tp+1,ra_scores(scoreIDX).second.fp));
                            else ra_scores(scoreIDX) = new Pair[Double,HitRate] (evidence.RA_score, new HitRate(ra_scores(scoreIDX).second.tp,ra_scores(scoreIDX).second.fp+1));
                            break;
                        }
                    }
                    if(!found) {
                        if(evidence.TP) ra_scores.add(new Pair[Double,HitRate] (evidence.RA_score,new HitRate(1,0)));
                        else ra_scores.add(new Pair[Double,HitRate] (evidence.RA_score,new HitRate(0,1)));
                    }
                    found = false;
                    for(scoreIDX in inf_scores.range()){
                        if(inf_scores(scoreIDX).first == INF_score) {
                            found = true;
                            if(evidence.TP) inf_scores(scoreIDX) = new Pair[Double,HitRate] (INF_score,new HitRate(inf_scores(scoreIDX).second.tp+1,inf_scores(scoreIDX).second.fp));
                            else inf_scores(scoreIDX) = new Pair[Double,HitRate] (INF_score, new HitRate(inf_scores(scoreIDX).second.tp,inf_scores(scoreIDX).second.fp+1));
                            break;
                        }
                    }
                    if(!found) {
                        if(evidence.TP) inf_scores.add(new Pair[Double,HitRate] (INF_score,new HitRate(1,0)));
                        else inf_scores.add(new Pair[Double,HitRate] (INF_score,new HitRate(0,1)));
                    }
                    found = false;
                    for(scoreIDX in inf_log_scores.range()){
                        if(inf_log_scores(scoreIDX).first == INF_LOG_score) {
                            found = true;
                            if(evidence.TP) inf_log_scores(scoreIDX) = new Pair[Double,HitRate] (INF_LOG_score,new HitRate(inf_log_scores(scoreIDX).second.tp+1,inf_log_scores(scoreIDX).second.fp));
                            else inf_log_scores(scoreIDX) = new Pair[Double,HitRate] (INF_LOG_score, new HitRate(inf_log_scores(scoreIDX).second.tp,inf_log_scores(scoreIDX).second.fp+1));
                            break;
                        }
                    }
                    if(!found) {
                        if(evidence.TP) inf_log_scores.add(new Pair[Double,HitRate] (INF_LOG_score,new HitRate(1,0)));
                        else inf_log_scores.add(new Pair[Double,HitRate] (INF_LOG_score,new HitRate(0,1)));
                    }
                    found = false;
                    for(scoreIDX in inf_2d_scores.range()){
                        if(inf_2d_scores(scoreIDX).first == INF_2D_score) {
                            found = true;
                            if(evidence.TP) inf_2d_scores(scoreIDX) = new Pair[Double,HitRate] (INF_2D_score,new HitRate(inf_2d_scores(scoreIDX).second.tp+1,inf_2d_scores(scoreIDX).second.fp));
                            else inf_2d_scores(scoreIDX) = new Pair[Double,HitRate] (INF_2D_score, new HitRate(inf_2d_scores(scoreIDX).second.tp,inf_2d_scores(scoreIDX).second.fp+1));
                            break;
                        }
                    }
                    if(!found) {
                        if(evidence.TP) inf_2d_scores.add(new Pair[Double,HitRate] (INF_2D_score,new HitRate(1,0)));
                        else inf_2d_scores.add(new Pair[Double,HitRate] (INF_2D_score,new HitRate(0,1)));
                    }
                    found = false;
                    for(scoreIDX in inf_log_2d_scores.range()){
                        if(inf_log_2d_scores(scoreIDX).first == INF_LOG_2D_score) {
                            found = true;
                            if(evidence.TP) inf_log_2d_scores(scoreIDX) = new Pair[Double,HitRate] (INF_LOG_2D_score,new HitRate(inf_log_2d_scores(scoreIDX).second.tp+1,inf_log_2d_scores(scoreIDX).second.fp));
                            else inf_log_2d_scores(scoreIDX) = new Pair[Double,HitRate] (INF_LOG_2D_score, new HitRate(inf_log_2d_scores(scoreIDX).second.tp,inf_log_2d_scores(scoreIDX).second.fp+1));
                            break;
                        }
                    }
                    if(!found) {
                        if(evidence.TP) inf_log_2d_scores.add(new Pair[Double,HitRate] (INF_LOG_2D_score,new HitRate(1,0)));
                        else inf_log_2d_scores.add(new Pair[Double,HitRate] (INF_LOG_2D_score,new HitRate(0,1)));
                    }
//bufferedPrintln("== "+ctx.id()+" has for weight "+INF_score+" TP:"+inf_scores.get(INF_score)().tp+" FP:"+inf_scores.get(INF_score)().fp);
                } 
                //Store 0 values, add unrelated TPs and FPs to the ones already found
                val unrelatedTPsAtZero = vertexData.testCandidates.size()-tpsFound;
                val unrelatedFPsAtZero = actualVertices - 1 - vertexData.Ancestors - unrelatedTPsAtZero - LPTargetsEvidence.size();
//bufferedPrintln("=== "+actualVertices+" "+unrelatedTPsAtZero+" "+LPTargets.size());
                cn_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + cn_scores(0).second.tp, unrelatedFPsAtZero + cn_scores(0).second.fp));
                ra_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + ra_scores(0).second.tp, unrelatedFPsAtZero + ra_scores(0).second.fp));
                aa_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + aa_scores(0).second.tp, unrelatedFPsAtZero + aa_scores(0).second.fp));
                inf_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + inf_scores(0).second.tp, unrelatedFPsAtZero + inf_scores(0).second.fp));
                inf_log_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + inf_log_scores(0).second.tp, unrelatedFPsAtZero + inf_log_scores(0).second.fp));
                inf_2d_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + inf_2d_scores(0).second.tp, unrelatedFPsAtZero + inf_2d_scores(0).second.fp));
                inf_log_2d_scores(0) = new Pair[Double,HitRate] (0,new HitRate(unrelatedTPsAtZero + inf_log_2d_scores(0).second.tp, unrelatedFPsAtZero + inf_log_2d_scores(0).second.fp));
//bufferedPrintln("== "+ctx.id()+" has for weight 0 TP:"+cn_scores.get(0)().tp+" FP:"+cn_scores.get(0)().fp);
                //Store for aggregate
                output.add(new ScorePair("CN",cn_scores));
                output.add(new ScorePair("RA",ra_scores));
                output.add(new ScorePair("AA",aa_scores));
                output.add(new ScorePair("INF",inf_scores));
                output.add(new ScorePair("INF_LOG",inf_log_scores));
                output.add(new ScorePair("INF_2D",inf_2d_scores));
                output.add(new ScorePair("INF_LOG_2D",inf_log_2d_scores));
                ctx.setValue(new VertexData(output,new HashMap[Long,NeighborData]()));
                ctx.voteToHalt();
            }
	    },
        null,
        //(aggregation :MemoryChunk[GrowableMemory[ScorePair]]) => predictionAggregator(aggregation),
        //TODO: use the combiner: CombinePaths should take various Message and append them into the same ... is it possible without losing the Ids? The vertex could add its Id to every path before sending it to the combiner. But this increases the size of messages dramatically.
	    //(paths :MemoryChunk[Message]) => combinePaths(paths),
        (superstep :Int, someValue :GrowableMemory[ScorePair]) => (superstep >= splitMessage));



        //First part is done. In the second we reduce the data of all vertices by sending it to vertexId=0 and using combiner and compute.
        xpregel.setLogPrinter(Console.ERR, 0);
        xpregel.iterate[MessageReduce,GrowableMemory[ScorePair]]((ctx :VertexContext[VertexData, Byte, MessageReduce, GrowableMemory[ScorePair]], messages :MemoryChunk[MessageReduce]) => {
            //In the first superstep all vertices with data send it to vertex 0
            if(ctx.superstep() == 0){
                if(ctx.value().last_data.size()==Long.implicit_operator_as(0)) ctx.voteToHalt();
                else{
//bufferedPrintln("DEBUG_0:"+(ctx.value().last_data.size()));
                val m :MessageReduce = MessageReduce(ctx.value().last_data);
                ctx.sendMessage(0,m);
                }
            }
            //In the second superstep the vertex with Id 0 combines all partially combined data  and writes results
            if(ctx.superstep() == 1){
                if(messages.size()!=Long.implicit_operator_as(0)){
                    //For each score calculated
                    for(rangeScores in messages(0).last_data.range()){
//bufferedPrintln("---Going IN for score with idx:"+rangeScores+" total vertex providing data: "+messages.size());
                        //Obtain the unique list of weights and their combined tp/fp
                        var reduction :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                        val name :String = messages(0).last_data(rangeScores).scoreName; 
                        val fw_roc = new FileWriter(name+"_points.roc", FileMode.Create);
                        val fw_pr = new FileWriter(name+"_points.pr", FileMode.Create);
                        //For each vertex
                        for(vertexRange in messages.range()){
                            val currentScorePair = messages(vertexRange).last_data(rangeScores);
//bufferedPrintln("Score Name: "+name+ " vertex idx:"+vertexRange);
                            //For each weight in scorePair
                            for(scoreIDX in currentScorePair.weights.range()){
                                val weight = currentScorePair.weights(scoreIDX).first;
                                //If weight existed, increase tp/fp counters, else add it
                                var found :Boolean = false;
                                for(redIDX in reduction.range()){
                                    if(reduction(redIDX).first == weight){
                                        found = true;
                                        reduction(redIDX) = new Pair[Double,HitRate] (weight, new HitRate(reduction(redIDX).second.tp + currentScorePair.weights(scoreIDX).second.tp ,reduction(redIDX).second.fp + currentScorePair.weights(scoreIDX).second.fp));
                                        break;
                                    }
                                }
                                if(!found){
                                    reduction.add(new Pair[Double,HitRate] (weight,new HitRate(currentScorePair.weights(scoreIDX).second.tp, currentScorePair.weights(scoreIDX).second.fp)));
                                }
                            }
                        }
                        val SP :ScorePair = new ScorePair(name,reduction);
//bufferedPrintln("Total weights found: "+reduction.size());
                        //Once we have everything reduced in var reduction, we can calculate the points: For each weight calculate the accumulated tp/fp
                        for(redIDX in reduction.range()){
                            val threshold = reduction(redIDX).first; 
                            var tpTotal :Long = 0; var fpTotal :Long = 0; 
                            //Calculate against all weights
                            for(redIDX2 in reduction.range()){
                                val currentWeight = reduction(redIDX2).first;
                                if(currentWeight>=threshold){
                                    tpTotal +=reduction(redIDX2).second.tp;
                                    fpTotal +=reduction(redIDX2).second.fp;
                                }
                            }
                            //Calculate points
//bufferedPrintln("Weight: "+threshold+" has a total FP:"+fpTotal+ " and TP:"+tpTotal+ " Vertices:"+actualVertices+" Edges:"+TestEdges);
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
//bufferedPrintln("combiner at " + here.id);
            if(allVertexPoints.size()==Long.implicit_operator_as(0)) return new MessageReduce(new GrowableMemory[ScorePair]());
            ret_val :GrowableMemory[ScorePair] = new GrowableMemory[ScorePair]();
//bufferedPrintln("DEBUG:" + (allVertexPoints.size()));
//bufferedPrintln("DEBUG_2:" + allVertexPoints(0).last_data.size());
            //For each score calculated
            for(rangeScores in allVertexPoints(0).last_data.range()){
//bufferedPrintln("---Going IN for score with idx:"+rangeScores+" total vertex providing data: "+allVertexPoints.size());
                //Obtain the unique list of weights and their combined tp/fp
                var reduction :GrowableMemory[Pair[Double,HitRate] ] = new GrowableMemory[Pair[Double,HitRate] ]();
                val name :String = allVertexPoints(0).last_data(rangeScores).scoreName; 
                //For each vertex
                for(vertexRange in allVertexPoints.range()){
                    val currentScorePair = allVertexPoints(vertexRange).last_data(rangeScores);
                    //For each weight in scorePair
                    for(scoreIDX in currentScorePair.weights.range()){
                        val weight = currentScorePair.weights(scoreIDX).first;
                        //If weight existed, increase tp/fp counters, else add it
                        var found :Boolean = false;
                        for(redIDX in reduction.range()){
                            if(reduction(redIDX).first == weight){
                                found = true;
                                reduction(redIDX) = new Pair[Double,HitRate] (weight, new HitRate(reduction(redIDX).second.tp + currentScorePair.weights(scoreIDX).second.tp ,reduction(redIDX).second.fp + currentScorePair.weights(scoreIDX).second.fp));
                                break;
                            }
                        }
                        if(!found){
                            reduction.add(new Pair[Double,HitRate] (weight,new HitRate(currentScorePair.weights(scoreIDX).second.tp, currentScorePair.weights(scoreIDX).second.fp)));
                        }
                    }
                }
                val SP :ScorePair = new ScorePair(name,reduction);
                ret_val.add(SP);
            }
            return new MessageReduce(ret_val);
        },
        (superstep :Int, someValue :GrowableMemory[ScorePair]) => (superstep >= 2));
        return true;
    }

//    static def printLocalGraph(lg :GrowableMemory[Pair[Long,NeighborData] ], depth :Long){
//        if(depth==Long.implicit_operator_as(0)) bufferedPrintln("-Printing localGraph");
//        for(rangeIDX in lg.range()){
//            val range = lg(rangeIDX);
//            thisStep :NeighborData = range.second;
//            for(i in 0..depth) print("-");
//            if(thisStep.direction == 0) print("<");
//            if(thisStep.direction == 1) print(">");
//            if(thisStep.direction == 2) print("<>");
//            bufferedPrintln(range.first);
//            printLocalGraph(thisStep.neighbors,depth+1);
//        }
//    }

}
