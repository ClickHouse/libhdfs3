/********************************************************************
 * 2023 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "GF256.h"
#include "Exception.h"
#include "ExceptionInternal.h"

namespace Hdfs {
namespace Internal {

std::vector<int8_t> GF256::GF_BASE = {
    (int8_t) 0x01, (int8_t) 0x02, (int8_t) 0x04, (int8_t) 0x08, (int8_t) 0x10,
    (int8_t) 0x20, (int8_t) 0x40, (int8_t) 0x80, (int8_t) 0x1d, (int8_t) 0x3a,
    (int8_t) 0x74, (int8_t) 0xe8, (int8_t) 0xcd, (int8_t) 0x87, (int8_t) 0x13,
    (int8_t) 0x26, (int8_t) 0x4c, (int8_t) 0x98, (int8_t) 0x2d, (int8_t) 0x5a,
    (int8_t) 0xb4, (int8_t) 0x75, (int8_t) 0xea, (int8_t) 0xc9, (int8_t) 0x8f,
    (int8_t) 0x03, (int8_t) 0x06, (int8_t) 0x0c, (int8_t) 0x18, (int8_t) 0x30,
    (int8_t) 0x60, (int8_t) 0xc0, (int8_t) 0x9d, (int8_t) 0x27, (int8_t) 0x4e,
    (int8_t) 0x9c, (int8_t) 0x25, (int8_t) 0x4a, (int8_t) 0x94, (int8_t) 0x35,
    (int8_t) 0x6a, (int8_t) 0xd4, (int8_t) 0xb5, (int8_t) 0x77, (int8_t) 0xee,
    (int8_t) 0xc1, (int8_t) 0x9f, (int8_t) 0x23, (int8_t) 0x46, (int8_t) 0x8c,
    (int8_t) 0x05, (int8_t) 0x0a, (int8_t) 0x14, (int8_t) 0x28, (int8_t) 0x50,
    (int8_t) 0xa0, (int8_t) 0x5d, (int8_t) 0xba, (int8_t) 0x69, (int8_t) 0xd2,
    (int8_t) 0xb9, (int8_t) 0x6f, (int8_t) 0xde, (int8_t) 0xa1, (int8_t) 0x5f,
    (int8_t) 0xbe, (int8_t) 0x61, (int8_t) 0xc2, (int8_t) 0x99, (int8_t) 0x2f,
    (int8_t) 0x5e, (int8_t) 0xbc, (int8_t) 0x65, (int8_t) 0xca, (int8_t) 0x89,
    (int8_t) 0x0f, (int8_t) 0x1e, (int8_t) 0x3c, (int8_t) 0x78, (int8_t) 0xf0,
    (int8_t) 0xfd, (int8_t) 0xe7, (int8_t) 0xd3, (int8_t) 0xbb, (int8_t) 0x6b,
    (int8_t) 0xd6, (int8_t) 0xb1, (int8_t) 0x7f, (int8_t) 0xfe, (int8_t) 0xe1,
    (int8_t) 0xdf, (int8_t) 0xa3, (int8_t) 0x5b, (int8_t) 0xb6, (int8_t) 0x71,
    (int8_t) 0xe2, (int8_t) 0xd9, (int8_t) 0xaf, (int8_t) 0x43, (int8_t) 0x86,
    (int8_t) 0x11, (int8_t) 0x22, (int8_t) 0x44, (int8_t) 0x88, (int8_t) 0x0d,
    (int8_t) 0x1a, (int8_t) 0x34, (int8_t) 0x68, (int8_t) 0xd0, (int8_t) 0xbd,
    (int8_t) 0x67, (int8_t) 0xce, (int8_t) 0x81, (int8_t) 0x1f, (int8_t) 0x3e,
    (int8_t) 0x7c, (int8_t) 0xf8, (int8_t) 0xed, (int8_t) 0xc7, (int8_t) 0x93,
    (int8_t) 0x3b, (int8_t) 0x76, (int8_t) 0xec, (int8_t) 0xc5, (int8_t) 0x97,
    (int8_t) 0x33, (int8_t) 0x66, (int8_t) 0xcc, (int8_t) 0x85, (int8_t) 0x17,
    (int8_t) 0x2e, (int8_t) 0x5c, (int8_t) 0xb8, (int8_t) 0x6d, (int8_t) 0xda,
    (int8_t) 0xa9, (int8_t) 0x4f, (int8_t) 0x9e, (int8_t) 0x21, (int8_t) 0x42,
    (int8_t) 0x84, (int8_t) 0x15, (int8_t) 0x2a, (int8_t) 0x54, (int8_t) 0xa8,
    (int8_t) 0x4d, (int8_t) 0x9a, (int8_t) 0x29, (int8_t) 0x52, (int8_t) 0xa4,
    (int8_t) 0x55, (int8_t) 0xaa, (int8_t) 0x49, (int8_t) 0x92, (int8_t) 0x39,
    (int8_t) 0x72, (int8_t) 0xe4, (int8_t) 0xd5, (int8_t) 0xb7, (int8_t) 0x73,
    (int8_t) 0xe6, (int8_t) 0xd1, (int8_t) 0xbf, (int8_t) 0x63, (int8_t) 0xc6,
    (int8_t) 0x91, (int8_t) 0x3f, (int8_t) 0x7e, (int8_t) 0xfc, (int8_t) 0xe5,
    (int8_t) 0xd7, (int8_t) 0xb3, (int8_t) 0x7b, (int8_t) 0xf6, (int8_t) 0xf1,
    (int8_t) 0xff, (int8_t) 0xe3, (int8_t) 0xdb, (int8_t) 0xab, (int8_t) 0x4b,
    (int8_t) 0x96, (int8_t) 0x31, (int8_t) 0x62, (int8_t) 0xc4, (int8_t) 0x95,
    (int8_t) 0x37, (int8_t) 0x6e, (int8_t) 0xdc, (int8_t) 0xa5, (int8_t) 0x57,
    (int8_t) 0xae, (int8_t) 0x41, (int8_t) 0x82, (int8_t) 0x19, (int8_t) 0x32,
    (int8_t) 0x64, (int8_t) 0xc8, (int8_t) 0x8d, (int8_t) 0x07, (int8_t) 0x0e,
    (int8_t) 0x1c, (int8_t) 0x38, (int8_t) 0x70, (int8_t) 0xe0, (int8_t) 0xdd,
    (int8_t) 0xa7, (int8_t) 0x53, (int8_t) 0xa6, (int8_t) 0x51, (int8_t) 0xa2,
    (int8_t) 0x59, (int8_t) 0xb2, (int8_t) 0x79, (int8_t) 0xf2, (int8_t) 0xf9,
    (int8_t) 0xef, (int8_t) 0xc3, (int8_t) 0x9b, (int8_t) 0x2b, (int8_t) 0x56,
    (int8_t) 0xac, (int8_t) 0x45, (int8_t) 0x8a, (int8_t) 0x09, (int8_t) 0x12,
    (int8_t) 0x24, (int8_t) 0x48, (int8_t) 0x90, (int8_t) 0x3d, (int8_t) 0x7a,
    (int8_t) 0xf4, (int8_t) 0xf5, (int8_t) 0xf7, (int8_t) 0xf3, (int8_t) 0xfb,
    (int8_t) 0xeb, (int8_t) 0xcb, (int8_t) 0x8b, (int8_t) 0x0b, (int8_t) 0x16,
    (int8_t) 0x2c, (int8_t) 0x58, (int8_t) 0xb0, (int8_t) 0x7d, (int8_t) 0xfa,
    (int8_t) 0xe9, (int8_t) 0xcf, (int8_t) 0x83, (int8_t) 0x1b, (int8_t) 0x36,
    (int8_t) 0x6c, (int8_t) 0xd8, (int8_t) 0xad, (int8_t) 0x47, (int8_t) 0x8e,
    (int8_t) 0x01
};

std::vector<int8_t> GF256::GF_LOG_BASE = {
    (int8_t) 0x00, (int8_t) 0xff, (int8_t) 0x01, (int8_t) 0x19, (int8_t) 0x02,
    (int8_t) 0x32, (int8_t) 0x1a, (int8_t) 0xc6, (int8_t) 0x03, (int8_t) 0xdf,
    (int8_t) 0x33, (int8_t) 0xee, (int8_t) 0x1b, (int8_t) 0x68, (int8_t) 0xc7,
    (int8_t) 0x4b, (int8_t) 0x04, (int8_t) 0x64, (int8_t) 0xe0, (int8_t) 0x0e,
    (int8_t) 0x34, (int8_t) 0x8d, (int8_t) 0xef, (int8_t) 0x81, (int8_t) 0x1c,
    (int8_t) 0xc1, (int8_t) 0x69, (int8_t) 0xf8, (int8_t) 0xc8, (int8_t) 0x08,
    (int8_t) 0x4c, (int8_t) 0x71, (int8_t) 0x05, (int8_t) 0x8a, (int8_t) 0x65,
    (int8_t) 0x2f, (int8_t) 0xe1, (int8_t) 0x24, (int8_t) 0x0f, (int8_t) 0x21,
    (int8_t) 0x35, (int8_t) 0x93, (int8_t) 0x8e, (int8_t) 0xda, (int8_t) 0xf0,
    (int8_t) 0x12, (int8_t) 0x82, (int8_t) 0x45, (int8_t) 0x1d, (int8_t) 0xb5,
    (int8_t) 0xc2, (int8_t) 0x7d, (int8_t) 0x6a, (int8_t) 0x27, (int8_t) 0xf9,
    (int8_t) 0xb9, (int8_t) 0xc9, (int8_t) 0x9a, (int8_t) 0x09, (int8_t) 0x78,
    (int8_t) 0x4d, (int8_t) 0xe4, (int8_t) 0x72, (int8_t) 0xa6, (int8_t) 0x06,
    (int8_t) 0xbf, (int8_t) 0x8b, (int8_t) 0x62, (int8_t) 0x66, (int8_t) 0xdd,
    (int8_t) 0x30, (int8_t) 0xfd, (int8_t) 0xe2, (int8_t) 0x98, (int8_t) 0x25,
    (int8_t) 0xb3, (int8_t) 0x10, (int8_t) 0x91, (int8_t) 0x22, (int8_t) 0x88,
    (int8_t) 0x36, (int8_t) 0xd0, (int8_t) 0x94, (int8_t) 0xce, (int8_t) 0x8f,
    (int8_t) 0x96, (int8_t) 0xdb, (int8_t) 0xbd, (int8_t) 0xf1, (int8_t) 0xd2,
    (int8_t) 0x13, (int8_t) 0x5c, (int8_t) 0x83, (int8_t) 0x38, (int8_t) 0x46,
    (int8_t) 0x40, (int8_t) 0x1e, (int8_t) 0x42, (int8_t) 0xb6, (int8_t) 0xa3,
    (int8_t) 0xc3, (int8_t) 0x48, (int8_t) 0x7e, (int8_t) 0x6e, (int8_t) 0x6b,
    (int8_t) 0x3a, (int8_t) 0x28, (int8_t) 0x54, (int8_t) 0xfa, (int8_t) 0x85,
    (int8_t) 0xba, (int8_t) 0x3d, (int8_t) 0xca, (int8_t) 0x5e, (int8_t) 0x9b,
    (int8_t) 0x9f, (int8_t) 0x0a, (int8_t) 0x15, (int8_t) 0x79, (int8_t) 0x2b,
    (int8_t) 0x4e, (int8_t) 0xd4, (int8_t) 0xe5, (int8_t) 0xac, (int8_t) 0x73,
    (int8_t) 0xf3, (int8_t) 0xa7, (int8_t) 0x57, (int8_t) 0x07, (int8_t) 0x70,
    (int8_t) 0xc0, (int8_t) 0xf7, (int8_t) 0x8c, (int8_t) 0x80, (int8_t) 0x63,
    (int8_t) 0x0d, (int8_t) 0x67, (int8_t) 0x4a, (int8_t) 0xde, (int8_t) 0xed,
    (int8_t) 0x31, (int8_t) 0xc5, (int8_t) 0xfe, (int8_t) 0x18, (int8_t) 0xe3,
    (int8_t) 0xa5, (int8_t) 0x99, (int8_t) 0x77, (int8_t) 0x26, (int8_t) 0xb8,
    (int8_t) 0xb4, (int8_t) 0x7c, (int8_t) 0x11, (int8_t) 0x44, (int8_t) 0x92,
    (int8_t) 0xd9, (int8_t) 0x23, (int8_t) 0x20, (int8_t) 0x89, (int8_t) 0x2e,
    (int8_t) 0x37, (int8_t) 0x3f, (int8_t) 0xd1, (int8_t) 0x5b, (int8_t) 0x95,
    (int8_t) 0xbc, (int8_t) 0xcf, (int8_t) 0xcd, (int8_t) 0x90, (int8_t) 0x87,
    (int8_t) 0x97, (int8_t) 0xb2, (int8_t) 0xdc, (int8_t) 0xfc, (int8_t) 0xbe,
    (int8_t) 0x61, (int8_t) 0xf2, (int8_t) 0x56, (int8_t) 0xd3, (int8_t) 0xab,
    (int8_t) 0x14, (int8_t) 0x2a, (int8_t) 0x5d, (int8_t) 0x9e, (int8_t) 0x84,
    (int8_t) 0x3c, (int8_t) 0x39, (int8_t) 0x53, (int8_t) 0x47, (int8_t) 0x6d,
    (int8_t) 0x41, (int8_t) 0xa2, (int8_t) 0x1f, (int8_t) 0x2d, (int8_t) 0x43,
    (int8_t) 0xd8, (int8_t) 0xb7, (int8_t) 0x7b, (int8_t) 0xa4, (int8_t) 0x76,
    (int8_t) 0xc4, (int8_t) 0x17, (int8_t) 0x49, (int8_t) 0xec, (int8_t) 0x7f,
    (int8_t) 0x0c, (int8_t) 0x6f, (int8_t) 0xf6, (int8_t) 0x6c, (int8_t) 0xa1,
    (int8_t) 0x3b, (int8_t) 0x52, (int8_t) 0x29, (int8_t) 0x9d, (int8_t) 0x55,
    (int8_t) 0xaa, (int8_t) 0xfb, (int8_t) 0x60, (int8_t) 0x86, (int8_t) 0xb1,
    (int8_t) 0xbb, (int8_t) 0xcc, (int8_t) 0x3e, (int8_t) 0x5a, (int8_t) 0xcb,
    (int8_t) 0x59, (int8_t) 0x5f, (int8_t) 0xb0, (int8_t) 0x9c, (int8_t) 0xa9,
    (int8_t) 0xa0, (int8_t) 0x51, (int8_t) 0x0b, (int8_t) 0xf5, (int8_t) 0x16,
    (int8_t) 0xeb, (int8_t) 0x7a, (int8_t) 0x75, (int8_t) 0x2c, (int8_t) 0xd7,
    (int8_t) 0x4f, (int8_t) 0xae, (int8_t) 0xd5, (int8_t) 0xe9, (int8_t) 0xe6,
    (int8_t) 0xe7, (int8_t) 0xad, (int8_t) 0xe8, (int8_t) 0x74, (int8_t) 0xd6,
    (int8_t) 0xf4, (int8_t) 0xea, (int8_t) 0xa8, (int8_t) 0x50, (int8_t) 0x58,
    (int8_t) 0xaf
};

int8_t GF256::gfMul(int8_t a, int8_t b) {
    if ((a == 0) || (b == 0)) {
        return 0;
    }

    int tmp = (GF_LOG_BASE[a & 0xff] & 0xff) + (GF_LOG_BASE[b & 0xff] & 0xff);
    if (tmp > 254) {
        tmp -= 255;
    }

    return GF_BASE[tmp];
}

const std::vector<std::vector<int8_t>> & GF256::getInstance() {
    static GfMulTab gfMulTab;
    return gfMulTab.getInnerGfMulTab();
}

int8_t GF256::gfInv(int8_t a) {
    if (a == 0) {
        return 0;
    }

    return GF_BASE[(255 - GF_LOG_BASE[a & 0xff]) & 0xff];
}

/**
 * Invert a matrix assuming it's invertible.
 *
 * Ported from Intel ISA-L library.
 */
void GF256::gfInvertMatrix(std::vector<int8_t> & inMatrix,
                           std::vector<int8_t> & outMatrix, int n) {
    int8_t temp;

    // Set outMatrix[] to the identity matrix
    for (int i = 0; i < n * n; i++) {
        outMatrix[i] = 0;
    }

    for (int i = 0; i < n; i++) {
        outMatrix[i * n + i] = 1;
    }

    // Inverse
    for (int j, i = 0; i < n; i++) {
        // Check for 0 in pivot element
        if (inMatrix[i * n + i] == 0) {
            // Find a row with non-zero in current column and swap
            for (j = i + 1; j < n; j++) {
                if (inMatrix[j * n + i] != 0) {
                    break;
                }
            }
            if (j == n) [[unlikely]] {
                // Couldn't find means it's singular
                THROW(HdfsException ,"Not invertible");
            }

            for (int k = 0; k < n; k++) {
                // Swap rows i,j
                temp = inMatrix[i * n + k];
                inMatrix[i * n + k] = inMatrix[j * n + k];
                inMatrix[j * n + k] = temp;

                temp = outMatrix[i * n + k];
                outMatrix[i * n + k] = outMatrix[j * n + k];
                outMatrix[j * n + k] = temp;
            }
        }

        temp = gfInv(inMatrix[i * n + i]); // 1/pivot
        for (j = 0; j < n; j++) {
            // Scale row i by 1/pivot
            inMatrix[i * n + j] = gfMul(inMatrix[i * n + j], temp);
            outMatrix[i * n + j] = gfMul(outMatrix[i * n + j], temp);
        }

        for (j = 0; j < n; j++) {
            if (j == i) {
                continue;
            }

            temp = inMatrix[j * n + i];
            for (int k = 0; k < n; k++) {
                outMatrix[j * n + k] ^= gfMul(temp, outMatrix[i * n + k]);
                inMatrix[j * n + k] ^= gfMul(temp, inMatrix[i * n + k]);
            }
        }
    }
}

/**
 * Ported from Intel ISA-L library.
 *
 * Calculates const table gftbl in GF(2^8) from single input A
 * gftbl(A) = {A{00}, A{01}, A{02}, ... , A{0f} }, {A{00}, A{10}, A{20},
 * ... , A{f0} } -- from ISA-L implementation
 */
void GF256::gfVectMulInit(int8_t c, std::vector<int8_t> & tbl, int offset) {
    auto c2 = static_cast<int8_t>((c << 1) ^ ((c & 0x80) != 0 ? 0x1d : 0));
    auto c4 = static_cast<int8_t>((c2 << 1) ^ ((c2 & 0x80) != 0 ? 0x1d : 0));
    auto c8 = static_cast<int8_t>((c4 << 1) ^ ((c4 & 0x80) != 0 ? 0x1d : 0));

    int8_t c3, c5, c6, c7, c9, c10, c11, c12, c13, c14, c15;
    int8_t c17, c18, c19, c20, c21, c22, c23, c24, c25, c26,
        c27, c28, c29, c30, c31;

    c3 = static_cast<int8_t>(c2 ^ c);
    c5 = static_cast<int8_t>(c4 ^ c);
    c6 = static_cast<int8_t>(c4 ^ c2);
    c7 = static_cast<int8_t>(c4 ^ c3);

    c9 = static_cast<int8_t>(c8 ^ c);
    c10 = static_cast<int8_t>(c8 ^ c2);
    c11 = static_cast<int8_t>(c8 ^ c3);
    c12 = static_cast<int8_t>(c8 ^ c4);
    c13 = static_cast<int8_t>(c8 ^ c5);
    c14 = static_cast<int8_t>(c8 ^ c6);
    c15 = static_cast<int8_t>(c8 ^ c7);

    tbl[offset + 0] = 0;
    tbl[offset + 1] = c;
    tbl[offset + 2] = c2;
    tbl[offset + 3] = c3;
    tbl[offset + 4] = c4;
    tbl[offset + 5] = c5;
    tbl[offset + 6] = c6;
    tbl[offset + 7] = c7;
    tbl[offset + 8] = c8;
    tbl[offset + 9] = c9;
    tbl[offset + 10] = c10;
    tbl[offset + 11] = c11;
    tbl[offset + 12] = c12;
    tbl[offset + 13] = c13;
    tbl[offset + 14] = c14;
    tbl[offset + 15] = c15;

    c17 = static_cast<int8_t>((c8 << 1) ^ ((c8 & 0x80) != 0 ? 0x1d : 0));
    c18 = static_cast<int8_t>((c17 << 1) ^ ((c17 & 0x80) != 0 ? 0x1d : 0));
    c19 = static_cast<int8_t>(c18 ^ c17);
    c20 = static_cast<int8_t>((c18 << 1) ^ ((c18 & 0x80) != 0 ? 0x1d : 0));
    c21 = static_cast<int8_t>(c20 ^ c17);
    c22 = static_cast<int8_t>(c20 ^ c18);
    c23 = static_cast<int8_t>(c20 ^ c19);
    c24 = static_cast<int8_t>((c20 << 1) ^ ((c20 & 0x80) != 0 ? 0x1d : 0));
    c25 = static_cast<int8_t>(c24 ^ c17);
    c26 = static_cast<int8_t>(c24 ^ c18);
    c27 = static_cast<int8_t>(c24 ^ c19);
    c28 = static_cast<int8_t>(c24 ^ c20);
    c29 = static_cast<int8_t>(c24 ^ c21);
    c30 = static_cast<int8_t>(c24 ^ c22);
    c31 = static_cast<int8_t>(c24 ^ c23);

    tbl[offset + 16] = 0;
    tbl[offset + 17] = c17;
    tbl[offset + 18] = c18;
    tbl[offset + 19] = c19;
    tbl[offset + 20] = c20;
    tbl[offset + 21] = c21;
    tbl[offset + 22] = c22;
    tbl[offset + 23] = c23;
    tbl[offset + 24] = c24;
    tbl[offset + 25] = c25;
    tbl[offset + 26] = c26;
    tbl[offset + 27] = c27;
    tbl[offset + 28] = c28;
    tbl[offset + 29] = c29;
    tbl[offset + 30] = c30;
    tbl[offset + 31] = c31;
}

}
}
