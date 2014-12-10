
import x10.util.Pair;
import x10.compiler.Native;
import x10.compiler.NativeCPPInclude;

import org.scalegraph.Config;
import org.scalegraph.io.NamedDistData;
import org.scalegraph.io.CSV;
import org.scalegraph.test.AlgorithmTest;
import org.scalegraph.graph.Graph;
import org.scalegraph.xpregel.XPregelGraph;
import org.scalegraph.xpregel.VertexContext;
import org.scalegraph.util.MemoryChunk;
import org.scalegraph.util.GrowableMemory;
import org.scalegraph.util.MathAppend;

@NativeCPPInclude("CountTripletsNative.hpp")

public class CountTriplets extends AlgorithmTest {
	public static def main(args: Array[String](1)) {
		new CountTriplets().execute(args);
	}
	
	@Native("c++", "::test::NUM_SCORES")
	public static val NUM_SCORES :Int = 0;

	private static struct Message {
		public val inOutFlag :Byte;
		public val degree :Long;
		public val index :MemoryChunk[Long];
		public val value :MemoryChunk[Byte];
		public def this() {
			inOutFlag = 0;
			degree = 0;
			index = new MemoryChunk[Long]();
			value = new MemoryChunk[Byte]();
		}
		public def this(inOutFlag :Byte, degree :Long, index: MemoryChunk[Long], value: MemoryChunk[Byte]) {
			this.inOutFlag = inOutFlag;
			this.degree = degree;
			this.index = index;
			this.value = value;
		}
	}
	
	public static struct HitRate {
		val score :Double;
		val tp :Long;
		val fp :Long;
		public def this(score :Double, t :Long, f :Long){
			this.score = score;
			tp = t;
			fp = f;
		}
	}

	private static type ScorePair = GrowableMemory[HitRate];
	private static type DegreePair = Pair[Long,Long];
	private static type V = MemoryChunk[ScorePair];
	private static type E = Byte;
	private static type M = Message;

	@Native("c++", "::test::merge_in_out_edges(#outEdges, #inEdges, #edgesId, #edgesVal)")
	public static native mergeInOutEdges(outEdges :MemoryChunk[Long], inEdges :MemoryChunk[Long],
			edgesId :GrowableMemory[Long], edgesVal :GrowableMemory[Byte]) :void;
	
	@Native("c++", "::test::merge(#inArr, #edgesId, #edgesVal, #outArr, #hr, #score)")
	private static native def merge(inArr: MemoryChunk[M],
			edgesId :MemoryChunk[Long], edgesVal :MemoryChunk[Byte], outArr: V, hr: HitRate, score: ScorePair): void;
	
	public def run(args :Array[String](1), g :Graph): Boolean {

		if(args.size < 1) {
			println("Usage: <path>");
			return false;
		}
		
		val sw = Config.get().stopWatch();
		val csr = g.createDistEdgeIndexMatrix(Config.get().distXPregel(), true, false);
		sw.lap("Graph construction");
		val xpregel = new XPregelGraph[V,E](csr);
		
		// release graph data
		g.del();

		xpregel.setLogPrinter(Console.ERR, 0);

		val NUM_SPLIT = 8;
		// merge in-out edges
		xpregel.iterate[Long,Byte]((ctx :VertexContext[V, E, Long, Byte], inEdges :MemoryChunk[Long]) => {
			val ss = ctx.superstep();
			if(ss == 0) {
				val outEdges = ctx.outEdgesId();
				val id = ctx.id();
				for(i in outEdges.range()) ctx.sendMessage(outEdges(i), id);
			}
			else if(ss == 1) {
				val newEdgesId = new GrowableMemory[Long]();
				val newEdgesVal = new GrowableMemory[Byte]();
				mergeInOutEdges(ctx.outEdgesId(), inEdges, newEdgesId, newEdgesVal);
				ctx.setOutEdges(newEdgesId.raw(), newEdgesVal.raw());
			}
		},
		null,
		(superstep :Int, aggVal :Byte) => {
			if (here.id == 0) sw.lap("Merge IN-OUT Edges at superstep " + superstep + " = " + aggVal + " ");
			return superstep == 1;
		});

		// calc
		xpregel.iterate[M,Byte]((ctx :VertexContext[V,E,M,Byte], messages :MemoryChunk[M]) => {
			val ss = ctx.superstep();
			val outEdgesId = ctx.outEdgesId();
			val outEdgesVal = ctx.outEdgesValue();
			if(messages.size() > 0) {
				ctx.setValue(MemoryChunk.make[ScorePair](NUM_SCORES));
				merge(messages, outEdgesId, outEdgesVal, ctx.value(), new HitRate(0,0,0), new ScorePair());
			}
			if(ss < NUM_SPLIT) {
				for(idx in outEdgesId.range()) {
					if((outEdgesId(idx) % NUM_SPLIT) as Int == ss) {
						ctx.sendMessage(outEdgesId(idx), new Message(outEdgesVal(idx), outEdgesId.size(), outEdgesId, outEdgesVal));
					}
				}
			}
		},
		null,
		(superstep :Int, aggVal :Byte) => {
			if (here.id == 0) sw.lap("CALC at superstep " + superstep + " = " + aggVal + " ");
			return superstep == NUM_SPLIT;
		});


		return true;
	}
}
