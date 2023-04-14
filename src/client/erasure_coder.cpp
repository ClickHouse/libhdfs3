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

#include <string.h>

#include "erasure_code.h"
#include "erasure_coder.h"
#include "dump.h"
#include "Logger.h"

namespace Hdfs {
namespace Internal {

void initCoder(IsalCoder * pCoder, int numDataUnits, int numParityUnits, bool verbose) {
    pCoder->verbose = verbose;
    pCoder->numParityUnits = numParityUnits;
    pCoder->numDataUnits = numDataUnits;
    pCoder->numAllUnits = numDataUnits + numParityUnits;
}

static void initEncodeMatrix(int numDataUnits, int numParityUnits,
                             unsigned char * encodeMatrix) {
#ifdef HADOOP_ISAL_LIBRARY
    // Generate encode matrix, always invertible
    gf_gen_cauchy1_matrix(encodeMatrix,
                          numDataUnits + numParityUnits, numDataUnits);
#endif
}

void initEncoder(IsalEncoder * pCoder, int numDataUnits,
                 int numParityUnits, bool verbose) {
#ifdef HADOOP_ISAL_LIBRARY
    initCoder(&pCoder->coder, numDataUnits, numParityUnits, verbose);

    initEncodeMatrix(numDataUnits, numParityUnits, pCoder->encodeMatrix);

    // Generate gftbls from encode matrix
    ec_init_tables(numDataUnits, numParityUnits,
                   &pCoder->encodeMatrix[numDataUnits * numDataUnits],
                   pCoder->gftbls);

    if (pCoder->coder.verbose > 0) {
        dumpEncoder(pCoder);
    }
#endif
}

void initDecoder(IsalDecoder * pCoder, int numDataUnits,
                 int numParityUnits, bool verbose) {
    initCoder(&pCoder->coder, numDataUnits, numParityUnits, verbose);

    initEncodeMatrix(numDataUnits, numParityUnits, pCoder->encodeMatrix);
}

int innerEncode(IsalEncoder * pCoder, unsigned char ** dataUnits,
                unsigned char ** parityUnits, int chunkSize) {
#ifdef HADOOP_ISAL_LIBRARY
    int numDataUnits = pCoder->coder.numDataUnits;
    int numParityUnits = pCoder->coder.numParityUnits;

    ec_encode_data(chunkSize, numDataUnits, numParityUnits,
                   pCoder->gftbls, dataUnits, parityUnits);
#endif
    return 0;
}

// Return 1 when diff, 0 otherwise
static int compare(int * arr1, int len1, int * arr2, int len2) {
    int i;

    if (len1 == len2) {
        for (i = 0; i < len1; i++) {
            if (arr1[i] != arr2[i]) {
                return 1;
            }
        }
        return 0;
    }

    return 1;
}

static int processErasures(IsalDecoder * pCoder, unsigned char ** inputs,
                           int * erasedIndexes, int numErased) {
#ifdef HADOOP_ISAL_LIBRARY
    unsigned int r;
    int i, ret, index;
    int numDataUnits = pCoder->coder.numDataUnits;
    int isChanged = 0;

    for (i = 0, r = 0; i < numDataUnits; i++, r++) {
        while (inputs[r] == NULL) {
            r++;
        }

        if (pCoder->decodeIndex[i] != r) {
            pCoder->decodeIndex[i] = r;
            isChanged = 1;
        }
    }

    for (i = 0; i < numDataUnits; i++) {
        pCoder->realInputs[i] = inputs[pCoder->decodeIndex[i]];
    }

    if (isChanged == 0 &&
        compare(pCoder->erasedIndexes, pCoder->numErased,
        erasedIndexes, numErased) == 0) {
            return 0; // Optimization, nothing to do
    }

    clearDecoder(pCoder);

    for (i = 0; i < numErased; i++) {
        index = erasedIndexes[i];
        pCoder->erasedIndexes[i] = index;
        pCoder->erasureFlags[index] = 1;
        if (index < numDataUnits) {
            pCoder->numErasedDataUnits++;
        }
    }

    pCoder->numErased = numErased;

    ret = generateDecodeMatrix(pCoder);
    if (ret != 0) {
        LOG(LOG_ERROR, "Failed to generate decode matrix");
        return -1;
    }

    ec_init_tables(numDataUnits, pCoder->numErased,
                   pCoder->decodeMatrix, pCoder->gftbls);

    if (pCoder->coder.verbose > 0) {
        dumpDecoder(pCoder);
    }
#endif
    return 0;
}

int innerDecode(IsalDecoder * pCoder, unsigned char ** inputs,
                int * erasedIndexes, int numErased,
                unsigned char ** outputs, int chunkSize) {
#ifdef HADOOP_ISAL_LIBRARY
    int numDataUnits = pCoder->coder.numDataUnits;

    processErasures(pCoder, inputs, erasedIndexes, numErased);

    ec_encode_data(chunkSize, numDataUnits, pCoder->numErased,
                   pCoder->gftbls, pCoder->realInputs, outputs);
#endif
    return 0;
}

// Clear variables used per decode call
void clearDecoder(IsalDecoder * decoder) {
    decoder->numErasedDataUnits = 0;
    decoder->numErased = 0;
    memset(decoder->gftbls, 0, sizeof(decoder->gftbls));
    memset(decoder->decodeMatrix, 0, sizeof(decoder->decodeMatrix));
    memset(decoder->tmpMatrix, 0, sizeof(decoder->tmpMatrix));
    memset(decoder->invertMatrix, 0, sizeof(decoder->invertMatrix));
    memset(decoder->erasureFlags, 0, sizeof(decoder->erasureFlags));
    memset(decoder->erasedIndexes, 0, sizeof(decoder->erasedIndexes));
}

// Generate decode matrix from encode matrix
int generateDecodeMatrix(IsalDecoder * pCoder) {
#ifdef HADOOP_ISAL_LIBRARY
    int i, j, r, p;
    unsigned char s;
    int numDataUnits;

    numDataUnits = pCoder->coder.numDataUnits;

    // Construct matrix b by removing error rows
    for (i = 0; i < numDataUnits; i++) {
        r = pCoder->decodeIndex[i];
        for (j = 0; j < numDataUnits; j++) {
            pCoder->tmpMatrix[numDataUnits * i + j] =
                pCoder->encodeMatrix[numDataUnits * r + j];
        }
    }

    gf_invert_matrix(pCoder->tmpMatrix,
                     pCoder->invertMatrix, numDataUnits);

    for (i = 0; i < pCoder->numErasedDataUnits; i++) {
        for (j = 0; j < numDataUnits; j++) {
            pCoder->decodeMatrix[numDataUnits * i + j] =
                pCoder->invertMatrix[numDataUnits *
                pCoder->erasedIndexes[i] + j];
        }
    }

    for (p = pCoder->numErasedDataUnits; p < pCoder->numErased; p++) {
        for (i = 0; i < numDataUnits; i++) {
            s = 0;
            for (j = 0; j < numDataUnits; j++) {
                s ^= gf_mul(pCoder->invertMatrix[j * numDataUnits + i],
                    pCoder->encodeMatrix[numDataUnits *
                    pCoder->erasedIndexes[p] + j]);
            }

            pCoder->decodeMatrix[numDataUnits * p + i] = s;
        }
    }
#endif
    return 0;
}

}
}
