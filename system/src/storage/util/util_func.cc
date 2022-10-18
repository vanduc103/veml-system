#include "storage/util/util_func.h"

namespace storage {

void generatePBTable(dan::Table* table, const std::vector<EvalVec>& records) {
    // can parallel here
    for (const auto& record : records) {
        auto row = table->add_v();
        generatePBRecord(row, record);
    }
}

dan::Table* generatePBTable(const std::vector<EvalVec>& records) {
    dan::Table* table = new dan::Table();
    generatePBTable(table, records);
    return table;
}

void generatePBRecord(dan::Record* record, const EvalVec& values) {
    for (const auto& val : values) {
        auto eval = record->add_v();
        generatePBEval(eval, val);
    }
}

dan::Record* generatePBRecord(const EvalVec& values) {
    dan::Record* record = new dan::Record();
    generatePBRecord(record, values);
    return record;
}

void generatePBEval(dan::Eval* eval, const EvalValue& val) {
    using namespace google::protobuf;

    switch (val.getEvalType()) {
        case EvalType::ET_BIGINT:
            eval->set_l(val.getBigIntConst().get());
            break;
        case EvalType::ET_DOUBLE:
            eval->set_d(val.getDoubleConst().get());
            break;
        case EvalType::ET_STRING:
            eval->set_s(val.getStringConst().get());
            break;
        case EvalType::ET_BOOL:
            eval->set_b(val.getBoolConst().get());
            break;
        case EvalType::ET_INTLIST: {
            IntList& il = const_cast<IntList&>(val.getIntListConst());
            dan::IntVec* eil = new dan::IntVec();
            IntVec il_data;
            il.copyTo(il_data);
            RepeatedField<int32_t> data(il_data.begin(), il_data.end());
            eil->mutable_v()->CopyFrom(data);
            eval->set_allocated_il(eil);
            break;
        }
        case EvalType::ET_LONGLIST: {
            LongList& ll = const_cast<LongList&>(val.getLongListConst());
            dan::LongVec* ell = new dan::LongVec();
            LongVec ll_data;
            ll.copyTo(ll_data);
            RepeatedField<int64_t> data(ll_data.begin(), ll_data.end());
            ell->mutable_v()->CopyFrom(data);
            eval->set_allocated_ll(ell);
            break;
        }
        case EvalType::ET_FLOATLIST: {
            FloatList& fl = const_cast<FloatList&>(val.getFloatListConst());
            dan::FloatVec* efl = new dan::FloatVec();
            FloatVec fl_data;
            fl.copyTo(fl_data);
            RepeatedField<float> data(fl_data.begin(), fl_data.end());
            efl->mutable_v()->CopyFrom(data);
            eval->set_allocated_fl(efl);
            break;
        }
        case EvalType::ET_DOUBLELIST: {
            DoubleList& dl = const_cast<DoubleList&>(val.getDoubleListConst());
            dan::DoubleVec* edl = new dan::DoubleVec();
            DoubleVec dl_data;
            dl.copyTo(dl_data);
            RepeatedField<double> data(dl_data.begin(), dl_data.end());
            edl->mutable_v()->CopyFrom(data);
            eval->set_allocated_dl(edl);
            break;
        }
        case EvalType::ET_INTVEC: {
            IntVec& iv = const_cast<IntVec&>(val.getIntVecConst());
            dan::IntVec* eiv = new dan::IntVec();
            RepeatedField<int32_t> data(iv.begin(), iv.end());
            eiv->mutable_v()->CopyFrom(data);
            eval->set_allocated_il(eiv);
            break;
        }
        case EvalType::ET_LONGVEC: {
            LongVec& lv = const_cast<LongVec&>(val.getLongVecConst());
            dan::LongVec* elv = new dan::LongVec();
            RepeatedField<int64_t> data(lv.begin(), lv.end());
            elv->mutable_v()->CopyFrom(data);
            eval->set_allocated_ll(elv);
            break;
        }
        case EvalType::ET_FLOATVEC: {
            FloatVec& fv = const_cast<FloatVec&>(val.getFloatVecConst());
            dan::FloatVec* efv = new dan::FloatVec();
            RepeatedField<float> data(fv.begin(), fv.end());
            efv->mutable_v()->CopyFrom(data);
            eval->set_allocated_fl(efv);
            break;
        }
        case EvalType::ET_DOUBLEVEC: {
            DoubleVec& dv = const_cast<DoubleVec&>(val.getDoubleVecConst());
            dan::DoubleVec* edv = new dan::DoubleVec();
            RepeatedField<double> data(dv.begin(), dv.end());
            edv->mutable_v()->CopyFrom(data);
            eval->set_allocated_dl(edv);
            break;
        }
        default: {
            std::string err("invalid value type @generatePBRecord, ");
            RTI_EXCEPTION(err);
        }
    }
}

dan::Eval* generatePBEval(const EvalValue& val){
    dan::Eval* eval = new dan::Eval();
    generatePBEval(eval, val);
    return eval;
}

void generateEvalTable(std::vector<EvalVec>& table, const dan::Table& values) {
    size_t size = values.v_size();
    table.reserve(size);
    // can parallel here.
    for (unsigned i = 0; i < size; i++) {
        table.push_back(generateEvalRecord(values.v(i)));
    }
}

void generateEvalRecord(EvalVec& record, const dan::Record& values) {
    for (int i = 0; i < values.v_size(); i++) {
        const dan::Eval& val = values.v(i);
        record.push_back(generateEvalValue(val));
    }
}

EvalVec generateEvalRecord(const dan::Record& values) {
    EvalVec record;
    generateEvalRecord(record, values);
    return record;
}

EvalValue generateEvalValue(const dan::Eval& val) {
    using namespace google::protobuf;

    switch (val.v_case()) {
        case dan::Eval::kL:
            return BigInt(val.l());
        case dan::Eval::kD:
            return Double(val.d());
        case dan::Eval::kS:
            return String(val.s());
        case dan::Eval::kB:
            return Bool(val.b());
        case dan::Eval::kIl: {
            IntVec iv;
            dan::IntVec data = val.il();
            RepeatedField<int32_t>* il_data = data.mutable_v();
            std::copy(il_data->begin(), il_data->end(), std::back_inserter(iv));
            return iv;
        }
        case dan::Eval::kLl: {
            LongVec lv;
            dan::LongVec data = val.ll();
            RepeatedField<int64_t>* ll_data = data.mutable_v();
            std::copy(ll_data->begin(), ll_data->end(), std::back_inserter(lv));
            return lv;
        }
        case dan::Eval::kFl: {
            FloatVec fv;
            dan::FloatVec data = val.fl();
            RepeatedField<float>* fl_data = data.mutable_v();
            std::copy(fl_data->begin(), fl_data->end(), std::back_inserter(fv));
            return fv;
        }
        case dan::Eval::kDl: {
            DoubleVec dv;
            dan::DoubleVec data = val.dl();
            RepeatedField<double>* dl_data = data.mutable_v();
            std::copy(dl_data->begin(), dl_data->end(), std::back_inserter(dv));
            return dv;
        }
        default: {
            std::string err("invalid value type @generateEvalRecord, ");
            RTI_EXCEPTION(err);
        }
    }

    return EvalValue();
}

void replace_std_to_pb(const IntVec& std, dan::IntVec* pb) {
    google::protobuf::RepeatedField<int32_t> data(std.begin(), std.end());
    pb->mutable_v()->CopyFrom(data);
}

void replace_std_to_pb(const LongVec& std, dan::LongVec* pb) {
    google::protobuf::RepeatedField<int64_t> data(std.begin(), std.end());
    pb->mutable_v()->CopyFrom(data);
}

void replace_std_to_pb(const StrVec& std, dan::StrVec* pb) {
    *pb->mutable_v() = {std.begin(), std.end()};
}

void replace_pb_to_std(const dan::IntVec& pb, IntVec& std) {
    google::protobuf::RepeatedField<std::int32_t>* data
        = const_cast<dan::IntVec&>(pb).mutable_v();
    std::copy(data->begin(), data->end(), std::back_inserter(std));
}

void replace_pb_to_std(const dan::LongVec& pb, LongVec& std) {
    google::protobuf::RepeatedField<std::int64_t>* data
        = const_cast<dan::LongVec&>(pb).mutable_v();
    std::copy(data->begin(), data->end(), std::back_inserter(std));
}

void replace_pb_to_std(const dan::StrVec& pb, StrVec& std) {
    google::protobuf::RepeatedPtrField<std::string>* data
        = const_cast<dan::StrVec&>(pb).mutable_v();
    std::copy(data->begin(), data->end(), std::back_inserter(std));
}

IntVec replace_pb_to_std(const dan::IntVec& pb) {
    IntVec ret;
    replace_pb_to_std(pb, ret);
    return ret;
}

LongVec replace_pb_to_std(const dan::LongVec& pb) {
    LongVec ret;
    replace_pb_to_std(pb, ret);
    return ret;
}

StrVec replace_pb_to_std(const dan::StrVec& pb) {
    StrVec ret;
    replace_pb_to_std(pb, ret);
    return ret;
}

}  // namespace storage
