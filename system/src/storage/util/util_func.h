#ifndef STORAGE_UTIL_UTIL_FUNC_H_
#define STORAGE_UTIL_UTIL_FUNC_H_

// Project include
#include "storage/def_const.h"
#include "storage/catalog.h"

// gRPC generated file
#include "interface/interface.grpc.pb.h"

namespace storage {

/**
 * APIs from rti value to protobuf value
 */
void generatePBTable(dan::Table* table, const std::vector<EvalVec>& records);
dan::Table* generatePBTable(const std::vector<EvalVec>& records);

void generatePBRecord(dan::Record* record, const EvalVec& values);
dan::Record* generatePBRecord(const EvalVec& values);

void generatePBEval(dan::Eval* eval, const EvalValue& val);
dan::Eval* generatePBEval(const EvalValue& val);

/**
 * APIs from protobuf value to rti value
 */
void generateEvalTable(std::vector<EvalVec>& table, const dan::Table& values);

void generateEvalRecord(EvalVec& record, const dan::Record& values);
EvalVec generateEvalRecord(const dan::Record& values);

EvalValue generateEvalValue(const dan::Eval& val);

/**
 * APIs from std value to protobuf value
 */
void replace_std_to_pb(const IntVec& std, dan::IntVec* pb);
void replace_std_to_pb(const LongVec& std, dan::LongVec* pb);
void replace_std_to_pb(const StrVec& std, dan::StrVec* pb);

/**
 * APIs from protobuf value to std value
 */
void replace_pb_to_std(const dan::IntVec& pb, IntVec& std);
void replace_pb_to_std(const dan::LongVec& pb, LongVec& std);
void replace_pb_to_std(const dan::StrVec& pb, StrVec& std);

IntVec replace_pb_to_std(const dan::IntVec& pb);
LongVec replace_pb_to_std(const dan::LongVec& pb);
StrVec replace_pb_to_std(const dan::StrVec& pb);

}  // namespace storage

#endif  // STORAGE_UTIL_UTIL_FUNC_H_
