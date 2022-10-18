// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "persistency/archive_manager.h"

// Project include
#include "storage/relation/table.h"
#include "storage/graph/graph.h"
#include "concurrency/rti_thread.h"

// C & C++ system include
#include <fstream>

// Other include
#include <boost/filesystem.hpp>
#include <boost/serialization/vector.hpp>

namespace persistency {

std::string ArchiveManager::path_ = "";
namespace fs = boost::filesystem;

bool ArchiveManager::archive(const std::string path) {
    // set archive path
    ArchiveManager::path_ = path;
    fs::create_directories(path + "/paged_array/");
    fs::create_directories(path + "/table/");
    fs::create_directories(path + "/graph/");

    bool success = true;
    
    // archive tables
    std::vector<Table*> tables;
    Metadata::getTableList(tables);

    for (auto t : tables) {
        success &= ArchiveManager::archive(t);
        if (!success) return false;
    }

    // archive graphs
    std::vector<Graph*> graphs;
    Metadata::getGraphList(graphs);

    for (auto g : graphs) {
        success &= ArchiveManager::archive(g);
        if (!success) return false;
    }

    return true;
}

bool ArchiveManager::archive(const paged_array_base* pa) {
    // set path
    uint32_t pa_id = pa->getId();
    std::string path = path_ + "/paged_array/" + std::to_string(pa_id);
    fs::create_directories(path);

    // open archive file
    std::string fname = path + "/header.archive";
    std::ofstream ofs(fname);
    if (!ofs.good()) {
        std::string err("failed to open archive file " + fname);
        RTI_EXCEPTION(err);
        return false;
    }

    // serialize paged_array header
    boost::archive::binary_oarchive oa(ofs);
    pa->serialize(oa);

    // serialize paged_array segments
    SgmtLptrVec sgmt_ids = pa->getAllSgmtIds();
    for (auto id : sgmt_ids) {
        // open archive file
        std::string fname = path + "/" + std::to_string(id) + ".segment";
        std::ofstream ofs(fname);
        if (!ofs.good()) {
            std::string err("failed to open archive file " + fname);
            RTI_EXCEPTION(err);
            return false;
        }

        // serialize paged_array header
        boost::archive::binary_oarchive oa(ofs);
        pa->serialize(id, oa);
    }
    
    return true;
}

bool ArchiveManager::archive(const Table* table) {
    std::string path = path_ + "/table/" + table->getTableName() + ".archive";
    std::ofstream ofs(path.c_str());
    if (!ofs.good()) {
        std::string err("failed to open archive file " + path);
        RTI_EXCEPTION(err);
        return false;
    }
    boost::archive::binary_oarchive oa(ofs);

    // serialize table header
    table->serialize(oa);

    // serialize paged_array in table
    std::vector<uint32_t> pa_ids;
    table->getPagedArrayIds(pa_ids);
    for (auto id : pa_ids) {
        paged_array_base* pa = Metadata::getPagedArray(id);
        if (pa) ArchiveManager::archive(pa);
    }

    return true;
}

bool ArchiveManager::archive(const Graph* graph) {
    std::string path = path_ + "/graph/" + graph->getName() + ".archive";
    std::ofstream ofs(path.c_str());
    if (!ofs.good()) {
        std::string err("failed to open archive file " + path);
        RTI_EXCEPTION(err);
        return false;
    }
    boost::archive::binary_oarchive oa(ofs);

    // serialize graph header
    graph->serialize(oa);

    // serialize paged_array in graph
    std::vector<uint32_t> pa_ids;
    graph->getPagedArrayIds(pa_ids);
    for (auto id : pa_ids) {
        paged_array_base* pa = Metadata::getPagedArray(id);
        if (pa) ArchiveManager::archive(pa);
    }

    return true;
}

bool ArchiveManager::recover(const std::string path, int dop) {
    ArchiveManager::path_ = path;

    class RecoveryWorker : public RTIThread {
      public:
        struct RecoveryJob {
            RecoveryJob(std::string fpath, bool is_table)
                : fpath_(fpath), is_table_(is_table) {}
            std::string fpath_;
            bool is_table_;
        };

        RecoveryWorker() : success_(true) {}

        void addJob(std::string fpath, bool is_table) {
            job_.push_back(RecoveryJob(fpath, is_table));
        }

        void* run() {
            for (auto& job : job_) {
                std::vector<uint32_t> pa_ids;
                std::vector<Table*> tables;
                
                std::ifstream ifs(job.fpath_);
                if (ifs.good()) {
                    // recover data store
                    boost::archive::binary_iarchive ia(ifs);
                    if (job.is_table_) {
                        Table* t = Table::deserialize(ia);
                        if (t) {
                            t->getPagedArrayIds(pa_ids);
                            tables.push_back(t);
                        } else {
                            success_ = false;
                        }
                    } else {
                        Graph* g = Graph::deserialize(ia);
                        if (g) {
                            g->getPagedArrayIds(pa_ids);
                        } else {
                            success_ = false;
                        }
                    }
                }
                ifs.close();

                // recover paged_arrays
                for (auto pa_id: pa_ids) {
                    std::string path = path_ + "/paged_array/"
                                        + std::to_string(pa_id);
                    PABase* pa = Metadata::getPagedArray(pa_id);
                    if (!pa->recover(path)) {
                        success_ = false;
                        return nullptr;
                    }
                }

                // rebuild index in tables
                for (auto t: tables) {
                    t->rebuildIndex();
                }
            }
            return nullptr;
        }

        bool success() { return success_; }

      private:
        std::vector<RecoveryJob> job_;
        bool success_;
    };


    // recovery tables
    std::vector<RecoveryWorker> tworkers(dop, RecoveryWorker());
    int i = 0;
    for (const auto& file : fs::directory_iterator(path + "/table/")) {
        std::string fpath = file.path().string();
        std::string ext = fs::extension(file.path().filename().string());
        if (ext != ".archive") continue;

        tworkers[i%dop].addJob(fpath, true);
        i++;
    }

    for (auto& w : tworkers) {
        w.start();
    }

    for (auto& w : tworkers) {
        w.join();
    }

    for (auto& w : tworkers) {
        if (!w.success()) return false;
    }

    // recovery graphs
    std::vector<RecoveryWorker> gworkers(dop, RecoveryWorker());
    i = 0;
    for (const auto& file : fs::directory_iterator(path + "/graph/")) {
        std::string fpath = file.path().string();
        std::string ext = fs::extension(file.path().filename().string());
        if (ext != ".archive") continue;

        gworkers[i%dop].addJob(fpath, false);
        i++;
    }

    for (auto& w : gworkers) {
        w.start();
    }

    for (auto& w : gworkers) {
        w.join();
    }

    for (auto& w : gworkers) {
        if (!w.success()) return false;
    }

    return true;
}

}  // namespace persistency
