
#include <algorithm> // heap
#include <map>

enum ENUM_SCORE {
	SCORE_CN,
	SCORE_RA,
	SCORE_AA,
	SCORE_INF,
	SCORE_INF_LOG,
	SCORE_INF_2D,
	SCORE_INF_LOG_2D,

	NUM_SCORES
};

struct InOutFlag {
    static const uint8_t OUT_FLAG = 0x01;
	static const uint8_t IN_FLAG = 0x02;

	uint8_t v;

	InOutFlag() : v(0) { }
	InOutFlag(uint8_t v) : v(v) { }

	void addOutFlag() { v |= OUT_FLAG; }
	void addInFlag() { v |= IN_FLAG; }
	bool hasOutFlag() { return (v & OUT_FLAG) != 0; }
	bool hasInFlag() { return (v & IN_FLAG) != 0; }
};

template <typename MC_ID, typename GM_ID, typename GM_VAL>
void merge_in_out_edges(MC_ID out_edges, MC_ID& in_edges, GM_ID* edge_ids, GM_VAL* edge_vals) {
	typedef std::map<int64_t, InOutFlag> map_t;
	map_t edge_map;
	// merge in and out edges
	for(int i = 0; i < out_edges.size(); ++i) {
	    edge_map[out_edges.pointer()[i]].addOutFlag();
	}
	for(int i = 0; i < in_edges.size(); ++i) {
	    edge_map[in_edges.pointer()[i]].addInFlag();
	}
	// reserve memory
	edge_ids->grow(edge_map.size());
	edge_vals->grow(edge_map.size());
	// store
	for(map_t::iterator it = edge_map.begin(); it != edge_map.end(); ++it) {
	    edge_ids->add(it->first);
		edge_vals->add(it->second.v);
	}
}

struct MMPointer {
	long remain;
	InOutFlag in_out_flag;
	long degree;
	long* index_ptr;
	uint8_t* value_ptr;
};

struct MMComp {
	bool operator()(const MMPointer* s1, const MMPointer* s2) {
		return s1->index_ptr[0] > s2->index_ptr[0];
	}
};

template <typename MC_MES, typename MC_ID, typename MC_VAL, typename MC_OUT, typename RATE, typename SCORE>
void merge(MC_MES& in, MC_ID& edges_id, MC_VAL& edges_val, MC_OUT out, const RATE&, const SCORE&) {
	int num_array = in.size();
	// make heap
	MMPointer heap_value[num_array];
	MMPointer* heap[num_array];
	for(int i = 0; i < num_array; ++i) {
		// in.pointer(): Message*
		MMPointer& cur = heap_value[i];
		cur.remain = in.pointer()[i].FMGL(index).size();
		cur.in_out_flag = in.pointer()[i].FMGL(inOutFlag);
		cur.degree = in.pointer()[i].FMGL(degree);
		cur.index_ptr = in.pointer()[i].FMGL(index).pointer();
		cur.value_ptr = reinterpret_cast<uint8_t*>(in.pointer()[i].FMGL(value).pointer());
		heap[i] = &cur;
	}
	std::make_heap(heap, heap + num_array, MMComp());
	// merge
	long cur_index = -1;

	while(num_array > 0) {
		std::pop_heap(heap, heap + num_array, MMComp());
		MMPointer& cur = *heap[num_array-1];
		long index = *(cur.index_ptr++);
		InOutFlag value = *(cur.value_ptr++);

		// a connected triplet is found
		// self: the vertex that calls this function
		// first neighbor: ? (this information is not sent)
		// direction of edge between self and the first neighbor: cur.in_out_flag (direction is from the view of the first neighbor)
		// degree of the first neighbor: cur.degree
		// second neighbor: index
		// direction of edge between the first and the second neighbor: value (direction is from the view of the first neighbor)
		// degree of the second neighbor: ? (if you need this information, you have to change Byte -> Pair[Byte, Long]
		//                                    and gather neighbor's degree before)

		// TODO: calculate score
		// the following branch may help to simplify the code
		if(cur_index != index) {
			// the new second neighbor
		}
		else {
			// second neighbor is not changed
		}

		if(--(cur.remain) == 0) {
			// no more element in this array
			--num_array;
		}
		else {
			std::push_heap(heap, heap + num_array, MMComp());
		}
	}
}

