// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr
#ifndef UTIL_GEN_ZIPF_H_
#define UTIL_GEN_ZIPF_H_

// C & C++ system include
#include <cmath>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <random>
#include <chrono>
#include <map>
#include <thread>

/*
 * zipf distributed variables generator
 * N: total number of variables (= #output)
 * D: range of value (1 ~ D)
 * alpha: degree of skew (0 ~ 1)
 *
 * revise implementation of Kenneth J. Christensen
 *  (Univ of South Florida,http://www.csee.usf.edu/~christen)
 *  http://www.csee.usf.edu/~kchriste/tools/toolpage.html
 */
class Zipf {
  public:
    Zipf(int n, int d, double a, int dop = 32)
        : N(n), D(d), alpha(a), dop(dop) { 
        rand_val(1);
        gen();
    }
    void gen();
    void dump(std::string path);

    std::vector<int>& get_numbers()    { return numbers; }
    std::map<int, int>& get_freq_map() { return freq_map; }

    static void calculate(double alpha, double* arr, int start, int end) {
        int idx = 0;
        for(int i = start; i < end; i++, idx++) {
            arr[idx] = (1.0 / pow((double)start, alpha));
        }
    }

  private:
    double rand_val(int seed);

    int N;
    int D;
    double alpha;
    int dop;

    std::vector<int> numbers;
    std::map<int, int> freq_map;
};

void Zipf::gen() {
    // precalculate alpha exponential
    double* alpha_arr = new double[D];
    int per_thr = D / dop;
    std::vector<std::thread> threads;
    for (int i = 0; i < dop; i++) {
        if (i  == dop-1) {
            threads.push_back(std::thread(
                        calculate, alpha,
                        alpha_arr + i * per_thr,
                            1 + i * per_thr, D + 1));
        } else {
            threads.push_back(std::thread(
                        calculate, alpha,
                        alpha_arr + i * per_thr,
                            1 + i * per_thr,
                            1 + (i+1) * per_thr));
        }
    }

    for (auto& thr : threads) {
        thr.join();
    }

    // compute normalization constant
    double C = 0;
    for (int i = 1; i <= D; i++) {
        //C = C + (1.0 / pow((double)i, alpha));
        C = C + alpha_arr[i-1];
    }
    C = 1.0 / C;

    // generate skewed frequency
    std::vector<int> freq(D, 0);
    for (int i = 1; i <= N; i++) {
        double z = 0;
        while ((z==0) || (z==1)) {
            z = rand_val(0);
        }

        double sum_prob = 0;
        for (int j = 1; j <= D; j++) {
            //sum_prob = sum_prob + C / pow((double)j, alpha);
            sum_prob = sum_prob + C * alpha_arr[j-1];
            if (sum_prob >= z) {
                freq[j-1] += 1;
                break;
            }
        }
    }

    // get numbers to assign to distribution
    for (int i = 0; i < D; i++) {
        numbers.push_back(i);
    }
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::shuffle(numbers.begin(), numbers.end(), std::default_random_engine(seed));

    for (unsigned i = 0; i < numbers.size(); i++) {
        freq_map[numbers[i]] = freq[i];
    }

    delete[] alpha_arr;
}

void Zipf::dump(std::string path) {
    std::ofstream ofs(path.c_str(), std::ofstream::out);

    if (ofs.good()) {
        for (unsigned i = 0; i < numbers.size(); i++) {
            ofs << numbers[i] << '\n';
        }
    }
}

double Zipf::rand_val(int seed) {
    const long  a =      16807;  // Multiplier
    const long  m = 2147483647;  // Modulus
    const long  q =     127773;  // m div a
    const long  r =       2836;  // m mod a
    static long x;               // Random int value
    long        x_div_q;         // x divided by q
    long        x_mod_q;         // x modulo q
    long        x_new;           // New x value

    if (seed > 0) {
        x = seed;
        return(0.0);
    }

    // RNG using integer arithmetic
    x_div_q = x / q;
    x_mod_q = x % q;
    x_new = (a * x_mod_q) - (r * x_div_q);

    if (x_new > 0)
        x = x_new;
    else
        x = x_new + m;

    // Return a random value between 0.0 and 1.0
    return((double) x / m);
}

#endif // UTIL_GEN_ZIPF_H_
