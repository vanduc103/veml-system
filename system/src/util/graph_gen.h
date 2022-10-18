// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef UTIL_GRAPH_GEN_H_
#define UTIL_GRAPH_GEN_H_

// Project include
#include "util/zipf.h"
#include "concurrency/rti_thread.h"

// C & C++ system include
#include <cstring>

namespace util {

/*
 * random graph generator
 * input: the number of vertex/edge, zipf distribution factor
 * output: csv file for <src-vertex-id,dest-vertex-id>
 */
class GraphGen {
  public:
    struct Edge {
        Edge(int s, int d) : src(s), dest(d) {}
        int src;
        int dest;

        struct cmp {
            bool operator()(const Edge& l, const Edge& r) {
                if (l.src < r.src) return true;
                else if (l.src == r.src) {
                    return l.dest < r.dest;
                } else return false;
            }
        };
    };

    GraphGen(int num_vertex, int num_edge, float alpha, int dop = 8)
        : num_vertex_(num_vertex), num_edge_(num_edge),
        alpha_(alpha), dop_(dop) {}

    class Worker : public RTIThread {
      public:
        Worker(const std::map<int, int>& freq_map,
                std::vector<int> numbers,
                std::vector<int> assigned)
            : freq_map_(freq_map), numbers_(numbers), assigned_(assigned) {}

        std::vector<Edge>::iterator begin() { return result_.begin(); }
        std::vector<Edge>::iterator end() { return result_.end(); }

        void* run() {
            for (auto src : assigned_) {
                unsigned seed = std::chrono::system_clock::now()
                                    .time_since_epoch().count();
                std::shuffle(numbers_.begin(), numbers_.end(),
                        std::default_random_engine(seed));
                int num_dest = freq_map_.find(src)->second;
                if (num_dest > (int)numbers_.size()) {
                    num_dest = numbers_.size();
                }
                for (int i = 0; i < num_dest; i++) {
                    result_.push_back(Edge(src, numbers_[i]));
                }
            }
            return nullptr;
        }

      private:
        const std::map<int, int>& freq_map_;
        std::vector<int> numbers_;
        std::vector<int> assigned_;
        std::vector<Edge> result_;
    };

    std::vector<int> getVertex() { return numbers_; }

    void generate(std::vector<Edge>& result) {
        // source vertex is selected by zipf distribution
        Zipf zipf(num_edge_, num_vertex_, alpha_);
        std::map<int, int>& freq_map = zipf.get_freq_map();
        numbers_ = zipf.get_numbers();

        // create workers
        int per_thr = numbers_.size() / dop_;
        std::vector<Worker> workers;
        for (int i = 0; i < dop_; i++) {
            if (i == dop_-1) {
                workers.push_back(Worker(freq_map, numbers_,
                            std::vector<int>(numbers_.begin() + (i*per_thr),
                                numbers_.end())));
            } else {
                workers.push_back(Worker(freq_map, numbers_,
                            std::vector<int>(numbers_.begin() + (i*per_thr),
                                numbers_.begin() + ((i+1)*per_thr))));
            }
        }

        for (auto& w: workers) {
            w.start();
        }

        for (auto& w: workers) {
            w.join();
        }

        for (auto& w: workers) {
            result.insert(result.begin(), w.begin(), w.end());
        }

        unsigned seed = std::chrono::system_clock::now()
                            .time_since_epoch().count();
        std::shuffle(result.begin(), result.end(),
                std::default_random_engine(seed));
    }

  private:
    int num_vertex_;
    int num_edge_;
    float alpha_;
    std::vector<int> numbers_;

    int dop_;
};

}  // namespace util

#endif  // UTIL_GRAPH_GEN_H_
