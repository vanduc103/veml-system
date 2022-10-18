#ifndef STORAGE_UTIL_PY_DATALOADER_H
#define STORAGE_UTIL_PY_DATALOADER_H

#include "common/def_const.h"
#include "storage/def_const.h"
#include "storage/catalog.h"

#include <pybind11/numpy.h>

namespace py = pybind11;

namespace storage { 

    struct EvalVecWrapper {
        EvalVecWrapper(EvalVec& data) : data_(data) {};
        EvalVec& data_;
    };

    struct IntVecWrapper {
        IntVecWrapper(IntVec& data) : data_(data) {};
        IntVec& data_;
    };

    struct LongVecWrapper {
        LongVecWrapper(LongVec& data) : data_(data) {};
        LongVec& data_;
    };

    struct FloatVecWrapper {
        FloatVecWrapper(FloatVec& data) : data_(data) {};
        FloatVec& data_;
    };

    struct DoubleVecWrapper {
        DoubleVecWrapper(DoubleVec& data) : data_(data) {};
        DoubleVec& data_;
    };

    struct MeshEvalWrapper {
        MeshEvalWrapper(std::vector<EvalVec>& data) : data_(data) {};
        std::vector<EvalVec>& data_;
    };

    class PyDataloader {
        public:
            PyDataloader() {}
            
            void addCol(IntVec& col) {
                EvalVec list;
                for(auto& i : col){
                    list.push_back(EvalValue(BigInt(i)));
                }
                col_content_.push_back(list);
            }
            
            void addCol(LongVec& col) {
                EvalVec list;
                for(auto& i : col){
                    list.push_back(EvalValue(BigInt(i)));
                }
                col_content_.push_back(list);
            }
            
            void addCol(DoubleVec& col) {
                EvalVec list;
                for(auto& i : col){
                    list.push_back(EvalValue(Double(i)));
                }
                col_content_.push_back(list);
            }
            
            void addCol(std::vector<FloatVec>& col) {
                EvalVec list;
                for(auto& i : col){
                    list.push_back(EvalValue(i));
                }
                col_content_.push_back(list);
            }

            void addCol(std::vector<LongVec>& col) {
                EvalVec list;
                for(auto& i : col){
                    list.push_back(EvalValue(i));
                }
                col_content_.push_back(list);
            }
            
            void addCol(std::vector<DoubleVec>& col) {
                EvalVec list;
                for(auto& i : col){
                    list.push_back(EvalValue(i));
                }
                col_content_.push_back(list);
            }

            void addCol(EvalVec& col) {
                col_content_.push_back(col);
            }

            void addBoolCol(std::vector<bool>& col) {
                EvalVec list;
                for(auto i : col){
                    list.push_back(EvalValue(Bool(i)));
                }
                col_content_.push_back(list);
            }

            void buildRowContent() {
                if(col_content_.empty() || !row_content_.empty()) return;
                uint32_t c = col_content_.size();
                size_t l = col_content_[0].size();
                row_content_.reserve(l);
                for(size_t i = 0; i < l; i++) {
                    EvalVec r;
                    r.reserve(c);
                    for(size_t j = 0; j < c; j++) {
                        r.push_back(col_content_[j][i]);
                    }
                    row_content_.push_back(r);
                }
            }

            std::shared_ptr<MeshEvalWrapper> getRowContent(){
                if(row_content_.empty()) buildRowContent();
                std::shared_ptr<MeshEvalWrapper> data = std::make_shared<MeshEvalWrapper>(row_content_);
                return data;
            }

            std::shared_ptr<EvalVecWrapper> getCol(int i) {
                std::shared_ptr<EvalVecWrapper> data = std::make_shared<EvalVecWrapper>(col_content_[i]);
                return data;
            }

        protected:
            std::vector<EvalVec> col_content_;
            std::vector<EvalVec> row_content_;
    };

    template <typename T>
    void assign(std::vector<T> v, py::array_t<T>& array){
        size_t l = array.size();
        const T* ptr = array.data();
        v.assign(ptr, ptr + l);
    }

    class ArrayUtil {
        public:
            ArrayUtil() {};
        
            void convert(py::array_t<int>& array) {
                assign(il_, array);
            }

            void convert(py::array_t<int64_t>& array) {
                assign(ll_, array);
            }

            void convert(py::array_t<float>& array) {
                assign(fl_, array);
            }

            void convert(py::array_t<double>& array) {
                assign(dl_, array);
            }

            void convertEval(py::array_t<int>& array) {
                if(!el_.empty()) {
                    std::cout << "Evalvec is not empty\n";
                    return;
                }
                size_t l = array.size();
                int* ptr = const_cast<int*>(array.data());
                size_t i = 0;
                el_.reserve(l);
                while(i < l) {
                    el_.push_back(EvalValue(BigInt(*ptr)));
                    ++ptr;
                    ++i;
                }
            }

            std::shared_ptr<IntVecWrapper> ones(size_t length) {
                il_.assign(length, 1);
                std::shared_ptr<IntVecWrapper> data = std::make_shared<IntVecWrapper>(il_);
                return data;
            }

            std::shared_ptr<IntVecWrapper> zeros(size_t length) {
                il_.assign(length, 0);
                std::shared_ptr<IntVecWrapper> data = std::make_shared<IntVecWrapper>(il_);
                return data;
            }

            std::shared_ptr<IntVecWrapper> getIntVec() {
                std::shared_ptr<IntVecWrapper> data = std::make_shared<IntVecWrapper>(il_);
                return data;
            }
            
            std::shared_ptr<LongVecWrapper> getLongVec() {
                std::shared_ptr<LongVecWrapper> data = std::make_shared<LongVecWrapper>(ll_);
                return data;
            }

            std::shared_ptr<FloatVecWrapper> getFloatVec() {
                std::shared_ptr<FloatVecWrapper> data = std::make_shared<FloatVecWrapper>(fl_);
                return data;
            }
            
            std::shared_ptr<DoubleVecWrapper> getDoubleVec() {
                std::shared_ptr<DoubleVecWrapper> data = std::make_shared<DoubleVecWrapper>(dl_);
                return data;
            }

            std::shared_ptr<EvalVecWrapper> getEvalVec() {
                std::shared_ptr<EvalVecWrapper> data = std::make_shared<EvalVecWrapper>(el_);
                return data;
            }

        protected:
            IntVec il_;
            LongVec ll_;
            FloatVec fl_;
            DoubleVec dl_;
            EvalVec el_;

    };
};

#endif