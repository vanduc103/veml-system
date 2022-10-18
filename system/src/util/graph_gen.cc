// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "util/graph_gen.h"

int main(int argc, char *argv[]) {
    using namespace util;

    int num_vertex = 100;
    int num_edge = 1000;
    int dop = 8;
    float alpha = 0.5;
    std::string output("output.csv");
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0) {
            std::cout << "random graph generator usage: " << std::endl;
            std::cout << "\t--vertex <the number of vertex, default: 100>" << std::endl;
            std::cout << "\t--edge <the number of vertex, default: 1000>" << std::endl;
            std::cout << "\t--zipf <zipf distribution factor(alpha), default 0.5>" << std::endl;
            std::cout << "\t--dop <degree of parallelism, default 8>" << std::endl;
            std::cout << "\t--output <output file name>, default output.csv" << std::endl;
            std::cout << "\t--help, show this message" << std::endl;
            std::cout << std::endl;
            std::cout << "\tex) ./graph_gen --vertex 10 --edge 100 "
                        << "--zipf 1.0 --output graph.csv" << std::endl;
            std::cout << std::endl;
        }

        if (strcmp(argv[i], "--vertex") == 0) {
            num_vertex = atoi(argv[++i]);
        }
        
        if (strcmp(argv[i], "--edge") == 0) {
            num_edge = atoi(argv[++i]);
        }

        if (strcmp(argv[i], "--zipf") == 0) {
            alpha = atof(argv[++i]);
        }

        if (strcmp(argv[i], "--dop") == 0) {
            dop = atoi(argv[++i]);
        }

        if (strcmp(argv[i], "--output") == 0) {
            output = std::string(argv[++i]);
        }
    }
    
    // graph generator
    GraphGen generator(num_vertex, num_edge, alpha, dop);

    // generate edges
    std::vector<GraphGen::Edge> result;
    generator.generate(result);

    // write result
    std::ofstream ofs(output.c_str(), std::ofstream::out);
    if (ofs.good()) {
        for (auto& r : result) {
            ofs << r.src << ',' << r.dest << '\n';
        }
    }
}
