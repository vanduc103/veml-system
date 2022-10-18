// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef UTIL_RESULT_CHECKER_H_
#define UTIL_RESULT_CHECKER_H_

// Project include
#include "common/def_const.h"
#include "common/types/eval_type.h"

namespace util {

class ResultChecker {
 public:
    ResultChecker(std::vector<EvalVec>& result, std::vector<ValueType>& vt);

    // check the size of result
    bool checkSize(uint64_t correct_size);
    
    // check the materialized result with string splited by delimeter
    void checkRow(uint64_t idx, std::string correct_row, const char* delimeters);

    // check the result with file
    void checkFile(std::string correct_file, const char* delimeters, const bool is_ordered);

 private:
    DISALLOW_COMPILER_GENERATED(ResultChecker);

    std::vector<EvalVec>& result_;
    std::vector<ValueType>& vt_;
};

}  // namespace util

#endif  // UTIL_RESULT_CHECKER_H_
