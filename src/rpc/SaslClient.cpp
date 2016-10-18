/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
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
#include <algorithm>
#include <cctype>

#include "Exception.h"
#include "ExceptionInternal.h"
#include "SaslClient.h"

#define SASL_SUCCESS 0

namespace Hdfs {
namespace Internal {

SaslClient::SaslClient(const RpcSaslProto_SaslAuth & auth, const Token & token,
                       const std::string & principal) :
     complete(false), changeLength(false),
     privacy(false), integrity(false),
     theAuth(auth), theToken(token), thePrincipal(principal)  {
    int rc;
    ctx = NULL;
    RpcAuth method = RpcAuth(RpcAuth::ParseMethod(auth.method()));
    rc = gsasl_init(&ctx);

    if (rc != GSASL_OK) {
        THROW(HdfsIOException, "cannot initialize libgsasl");
    }

    switch (method.getMethod()) {
    case AuthMethod::KERBEROS:
        initKerberos(auth, principal);
        break;

    case AuthMethod::TOKEN:
        initDigestMd5(auth, token);
        break;

    default:
        THROW(HdfsIOException, "unknown auth method.");
        break;
    }
}

SaslClient::~SaslClient() {
    if (session != NULL) {
        gsasl_finish(session);
        session = NULL;
    }

    if (ctx != NULL) {
        gsasl_done(ctx);
        ctx = NULL;
    }
}


void SaslClient::initKerberos(const RpcSaslProto_SaslAuth & auth,
                              const std::string & principal) {
    int rc;

    /* Create new authentication session. */
    if ((rc = gsasl_client_start(ctx, auth.mechanism().c_str(), &session)) != GSASL_OK) {
        THROW(HdfsIOException, "Cannot initialize client (%d): %s", rc,
              gsasl_strerror(rc));
    }

    gsasl_property_set(session, GSASL_SERVICE, auth.protocol().c_str());
    gsasl_property_set(session, GSASL_AUTHID, principal.c_str());
    gsasl_property_set(session, GSASL_HOSTNAME, auth.serverid().c_str());
}

std::string Base64Encode(const std::string & in) {
    char * temp;
    size_t len;
    std::string retval;
    int rc = gsasl_base64_to(in.c_str(), in.size(), &temp, &len);

    if (rc != GSASL_OK) {
        throw std::bad_alloc();
    }

    if (temp) {
        retval = temp;
        free(temp);
    }

    if (!temp || retval.length() != len) {
        THROW(HdfsIOException, "SaslClient: Failed to encode string to base64");
    }

    return retval;
}

void SaslClient::initDigestMd5(const RpcSaslProto_SaslAuth & auth,
                               const Token & token) {
    int rc;

    if ((rc = gsasl_client_start(ctx, auth.mechanism().c_str(), &session)) != GSASL_OK) {
        THROW(HdfsIOException, "Cannot initialize client (%d): %s", rc, gsasl_strerror(rc));
    }

    std::string password = Base64Encode(token.getPassword());
    std::string identifier = Base64Encode(token.getIdentifier());
    gsasl_property_set(session, GSASL_PASSWORD, password.c_str());
    gsasl_property_set(session, GSASL_AUTHID, identifier.c_str());
    gsasl_property_set(session, GSASL_HOSTNAME, auth.serverid().c_str());
    gsasl_property_set(session, GSASL_SERVICE, auth.protocol().c_str());
    changeLength = true;
}

int SaslClient::findPreferred(int possible) {
    if (possible & GSASL_QOP_AUTH)
        return GSASL_QOP_AUTH;
    if (possible & GSASL_QOP_AUTH_INT)
        return GSASL_QOP_AUTH_INT;
    if (possible & GSASL_QOP_AUTH_CONF)
        return GSASL_QOP_AUTH_CONF;
    return GSASL_QOP_AUTH;
}


std::string SaslClient::evaluateChallenge(const std::string & challenge) {
    int rc;
    char * output = NULL;
    size_t outputSize;
    std::string retval;
    rc = gsasl_step(session, challenge.data(), challenge.size(), &output,
                    &outputSize);
    RpcAuth method = RpcAuth(RpcAuth::ParseMethod(theAuth.method()));
    if (rc == GSASL_GSSAPI_INIT_SEC_CONTEXT_ERROR && method.getMethod() == AuthMethod::KERBEROS) {
        // Try again using principal instead
        gsasl_finish(session);
        initKerberos(theAuth, thePrincipal);
        gsasl_property_set(session, GSASL_GSSAPI_DISPLAY_NAME, thePrincipal.c_str());
        rc = gsasl_step(session, challenge.data(), challenge.size(), &output,
                    &outputSize);
    }

    if (rc == GSASL_NEEDS_MORE || rc == GSASL_OK) {
        retval.resize(outputSize);
        memcpy((void *)retval.data(), output, outputSize);

        if (output) {
            free(output);
        }
    } else {
        if (output) {
            free(output);
        }

        THROW(AccessControlException, "Failed to evaluate challenge: %s", gsasl_strerror(rc));
    }

    if (rc == GSASL_OK) {
        complete = true;
        int preferred = 0;
        if (method.getMethod() == AuthMethod::TOKEN) {
            const char *qop = gsasl_property_get (session, GSASL_QOP);
            if (qop)
                preferred = qop[0];
        }
        else if (challenge.length()) {
            std::string decoded = decode(challenge.c_str(), challenge.length());
            int qop = (int)decoded.c_str()[0];
            preferred = findPreferred(qop);
        }
        if (preferred & GSASL_QOP_AUTH_CONF) {
            privacy = true;
            integrity = true;
        } else if (preferred & GSASL_QOP_AUTH_INT) {
            integrity = true;
        }
    }

    return retval;
}

std::string SaslClient::encode(const char *input, size_t input_len) {
    std::string result;
    if ((!privacy && !integrity) || (!complete)) {
        result.resize(input_len);
        memcpy((void *)result.data(), input, input_len);
        return result;
    }

    char *output=NULL;
    size_t output_len;
    int rc = gsasl_encode(session, input, input_len, &output, &output_len);
    if (rc != GSASL_OK) {
        THROW(AccessControlException, "Failed to encode wrapped data: %s", gsasl_strerror(rc));
    }
    if (output_len) {
        if (output_len > 4 && changeLength) {
            result.resize(output_len-4);
            memcpy((void *)result.data(), output+4, output_len-4);
        } else {
            result.resize(output_len);
            memcpy((void *)result.data(), output, output_len);
        }
        free(output);
    }
    return result;
}

std::string  SaslClient::decode(const char *input, size_t input_len) {
    std::string result;
    if ((!privacy && !integrity) || (!complete)) {
        result.resize(input_len);
        memcpy((void *)result.data(), input, input_len);
        return result;
    }
    char *output=NULL;
    size_t output_len;
    std::string actualInput;
    if (changeLength) {
        actualInput.resize(input_len+4);
        actualInput[0] = (input_len>> 24) & 0xFF;
        actualInput[1] = (input_len >> 16) & 0xFF;
        actualInput[2] = (input_len >> 8) & 0xFF;
        actualInput[3] = input_len & 0xFF;
        memcpy((void *)(actualInput.data() + 4), input, input_len);
    } else {
        actualInput.resize(input_len);
        memcpy((void *)actualInput.data(), input, input_len);
    }
    int rc = gsasl_decode(session, actualInput.c_str(), actualInput.length(), &output, &output_len);
    if (rc != GSASL_OK) {
        THROW(AccessControlException, "Failed to decode wrapped data: %s", gsasl_strerror(rc));
    }
    if (output_len) {
        result.resize(output_len);
        memcpy((void *)result.data(), output, output_len);
        free(output);
    }

    return result;
}


bool SaslClient::isComplete() {
    return complete;
}

bool SaslClient::isPrivate() {
    return privacy;
}

bool SaslClient::isIntegrity() {
    return integrity;
}

}
}

