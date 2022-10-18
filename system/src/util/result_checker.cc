// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "util/result_checker.h"

// C & C++ system include
#include <fstream>
#include <algorithm>

// Other include
#include <gtest/gtest.h>

namespace util {

ResultChecker::ResultChecker(std::vector<EvalVec>& result, std::vector<ValueType>& vt)
    : result_(result), vt_(vt) {}

bool ResultChecker::checkSize(size_t correct) {
    EXPECT_EQ(correct, result_.size());
    return correct == result_.size();
}

void ResultChecker::checkRow(uint64_t idx, std::string correct_row, const char* delimeters) {
    // get result
    EvalVec& res = result_[idx];

    // copy correct row to char*
    std::vector<char> buf(correct_row.begin(), correct_row.end());
    buf.push_back('\0');

    // split the correct_row to tokens
    std::vector<char*> correct;
    char* token = nullptr;
    token = strtok(&buf[0], delimeters);
    while (token != nullptr) {
        correct.push_back(token);
        token = strtok(0, delimeters);
    }
    EXPECT_EQ(correct.size(), res.size());

    for (unsigned i = 0; i < res.size(); i++) {
        switch (vt_[i]) {
            case VT_INT: {
                EXPECT_EQ(BigInt(atoi(correct[i])), res[i].getBigInt());
                break;
            }
            case VT_DOUBLE: {
                EXPECT_EQ(Decimal(correct[i]), Decimal(res[i].getDouble()));
                break;
            }
            case VT_DATE: {
                EXPECT_EQ(Date(correct[i]), res[i].getDate());
                break;
            }
            case VT_DECIMAL: {
                EXPECT_EQ(Decimal(correct[i]), res[i].getDecimal());
                break;
            }
            case VT_CHAR: {
                EXPECT_EQ(String(correct[i]), res[i].getString());
                break;
            }
            case VT_VARCHAR: {
                EXPECT_EQ(String(correct[i]), res[i].getString());
                break;
            }
            default: {
                std::string err("invalue value type @ResultChecker::checkRow, ");
                RTI_EXCEPTION(err);
            }
        }
    }
}

void ResultChecker::checkFile(std::string file_path,
        const char* delimeters, const bool is_ordered) {
    // open correct file
    std::fstream correct_file(file_path, std::fstream::in);
    assert(correct_file.is_open() == true);
    
    char buf[4096];
    // correct file and result are ordered already.
    if(is_ordered) {
        unsigned i = 0;
        while (correct_file.eof() == false) {
            correct_file.getline(buf, 4096);
            if (strlen(buf) == 0) break;
            checkRow(i++, std::string(buf), delimeters);
        }
        checkSize(i);
    } else {
        std::vector<std::string> correct;
        std::vector<std::string> result;

        while (correct_file.eof() == false) {
            correct_file.getline(buf, 4096);
            correct.push_back(std::string(buf));
        }

        checkSize(correct.size());
        for (auto& res : result_) {
            std::string res_string("");
            for (unsigned c = 0; c < res.size(); c++) {
                if (c != 0) res_string += delimeters;
                res_string += res[c].to_string();
            }

            result.push_back(res_string);
        }

        std::sort(correct.begin(), correct.end());
        std::sort(result.begin(), result.end());

        for (unsigned i = 0; i < correct.size(); i++) {
            EXPECT_EQ(correct[i], result[i]);
        }
    }
}

}  // namespace util
