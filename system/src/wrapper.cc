// Project include
#include "common/def_const.h"

// Other include
#include <dlpack/dlpack.h>

extern "C" void generate_dlpack(void* ptr,
                        DLManagedTensor** ret,
                        int32_t num_dimension,
                        int64_t* shape,
                        uint8_t dtype,
                        uint8_t bits);

extern "C" void deleter(DLManagedTensor* tensor);

void generate_dlpack(void* ptr,
                    DLManagedTensor** ret,
                    int32_t num_dimension,
                    int64_t* shape,
                    uint8_t dtype,
                    uint8_t bits) {
    // generate dlmanaged tensor(wrapper of DLPack)
    *ret = new DLManagedTensor();
    (*ret)->deleter = deleter;
    (*ret)->manager_ctx = nullptr;

    // set data types
    DLDataTypeCode type = (dtype == 0)? kDLInt : kDLFloat;

    // set tensor values
    DLTensor& tensor = (*ret)->dl_tensor;
    tensor = {
        ptr,                        // DLTensor::data
        DLContext{kDLCPU, 0},       // DLTensor::ctx
        num_dimension,              // DLTensor::ndim
        DLDataType{type, bits, 1},  // DLTensor::dtype
        shape,                      // DLTensor::shape
        nullptr,                    // DLTensor::strides
        0                           // DLTensor::byte_offset
    }; 
}

void deleter(DLManagedTensor* tensor) {
    delete tensor;
}
