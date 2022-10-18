// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "persistency/log_manager.h"

// Project include
#include "util/buffer.h"

// Other include
#include <boost/filesystem.hpp>

namespace persistency {

LogManager::LogManager() : log_file_("archive/log/log_file"), lsn_(0), enable_(true) {
    namespace fs = boost::filesystem;
    fs::path path = fs::current_path();
    path += "/archive/log";
    boost::filesystem::create_directories(path.string());

    for (unsigned i = 0; i < NUM_LOG_FILE; i++) {
        std::stringstream ss;
        ss << log_file_ << "_" << i;
        fp_[i] = fopen(ss.str().c_str(), "wb");
        //fp_[i] = fopen(ss.str().c_str(), "ab");
        fcntl(fileno(fp_[i]), F_SETFL, fcntl(fileno(fp_[i]), F_GETFL) | O_SYNC);
    }
}


void LogManager::setEnable_(bool enable) {
    enable_ = enable;
}

bool LogManager::isEnable_() {
    return enable_;
}

void LogManager::recover_() {
    if (!enable_)
        return;

    if (NUM_LOG_FILE == 1) {
        //recoverSingle_(md);
    } else {
        //recoverMulti_(md);
    }
    truncate_();
}

void LogManager::recoverSingle_() {
    /*
    std::stringstream ss;
    ss << log_file_ << "_" << 0;

    try {
        FILE* fp = fopen(ss.str().c_str(), "rb");
        int64_t lsn;
        size_t size;

        while (1) {
            if (fread(&lsn, 1, sizeof(int64_t), fp) != sizeof(int64_t))
                return;

            if (fread(&size, 1, sizeof(size_t), fp) != sizeof(size_t))
                return;

            char* buffer = new char[size];
            if (fread(buffer, 1, size, fp) != size)
                return;

            BufferReader reader(buffer);
            recover_(md, reader);
            updateLsn_(lsn);
        }
    } catch (...) {}
    */
}

void LogManager::recoverMulti_() {
    /*
    typedef std::pair<int64_t, BufferReader> LogItem;
    std::vector<LogItem> reader_vec;

    for (unsigned i = 0; i < NUM_LOG_FILE; i++) {
        std::stringstream ss;
        ss << log_file_ << "_" << i;

        try {
            FILE* fp = fopen(ss.str().c_str(), "rb");
            int64_t lsn;
            size_t size;

            while (1) {
                if (fread(&lsn, 1, sizeof(int64_t), fp) != sizeof(int64_t))
                    break;

                if (fread(&size, 1, sizeof(size_t), fp) != sizeof(size_t))
                    break;

                char* buffer = new char[size];
                if (fread(buffer, 1, size, fp) != size)
                    break;

                BufferReader reader(buffer);
                reader_vec.push_back(std::make_pair(lsn, reader));

                updateLsn_(lsn);
            }
        } catch (...) {}
    }

    auto less = [] (LogItem lhs, LogItem rhs) {
        return lhs.first < rhs.first;
    };
    std::sort(reader_vec.begin(), reader_vec.end(), less);

    for (unsigned i = 0; i < reader_vec.size(); i++) {
        recover_(md, reader_vec[i].second);
    }
    */
}

void LogManager::recover_(util::BufferReader& reader) {
    /*
    LogType type;
    int64_t tid;
    reader >> type;
    
    bool is_end = false;
    while (!is_end) {
        switch (type) {
            case START:
            {
                reader >> tid;
                reader >> type;
                break;
            }
            case DML:
            {
                recoverDML_(md, reader);
                reader >> type;
                break;
            }
            case END:
            {
                is_end = true;
                break;
            }
            default:
                assert(false && "not supported log type");
        }
    }
    */
}

void LogManager::recoverDML_(util::BufferReader& reader) {
    /*
    uint32_t table_id;
    LogicalPtr record;
    Version::VersionType type;

    reader >> table_id >> record >> type;
    Table* table = md.getTablePtr(table_id);

    switch (type) {
        case Version::INSERT:
        {
            std::vector<EvalValue> values;
            reader >> values;
            table->insertRecord(values, record);
            break;
        }
        case Version::UPDATE:
        {
            std::vector<EvalValue> values;
            std::vector<unsigned> field_ids;
            reader >> values;
            reader >> field_ids;
            table->updateRecord(record, field_ids, values);
            break;
        }
        case Version::DELETE:
        {
            table->deleteRecord(record);
            break;
        }
        default:
            assert(false && "not supported version type");
    }
    */
}

}  // namespace persistency
