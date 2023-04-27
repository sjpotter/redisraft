/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"

#include <string.h>

/* Return expected length of a serialized integer value as decimal digits + 2 byte overhead */
int calcIntSerializedLen(size_t val)
{
    if (val < 10) {
        return 3;
    } else if (val < 100UL) {
        return 4;
    } else if (val < 1000UL) {
        return 5;
    } else if (val < 10000UL) {
        return 6;
    } else if (val < 100000UL) {
        return 7;
    } else if (val < 1000000UL) {
        return 8;
    } else if (val < 10000000UL) {
        return 9;
    } else if (val < 100000000UL) {
        return 10;
    } else if (val < 1000000000UL) {
        return 11;
    } else if (val < 10000000000UL) {
        return 12;
    } else if (val < 100000000000UL) {
        return 13;
    } else if (val < 1000000000000UL) {
        return 14;
    } else if (val < 10000000000000UL) {
        return 15;
    } else if (val < 100000000000000UL) {
        return 16;
    } else if (val < 1000000000000000UL) {
        return 17;
    } else if (val < 10000000000000000UL) {
        return 18;
    } else if (val < 100000000000000000UL) {
        return 19;
    } else if (val < 1000000000000000000UL) {
        return 20;
    } else if (val < 10000000000000000000UL) {
        return 21;
    } else {
        return 22;
    }
}

/* returns the expected length of a serialized RedisModuleString */
size_t calcSerializeStringSize(RedisModuleString *str)
{
    size_t len;
    if (str != NULL) {
        RedisModule_StringPtrLen(str, &len);
    } else {
        len = 0;
    }

    return calcIntSerializedLen(len) + len + 1;
}

/* decodes a serialized integer stored at buffer denoted by ptr
 * sz = remaining buffer size in bytes
 * expected_prefix = character value we expected to denote this serialization
 * val, pointer we return integer into
 *
 * return the amount of bytes consumed from the buffer or -1 upon error
 */
int decodeInteger(const char *ptr, size_t sz, char expect_prefix, size_t *val)
{
    size_t tmp = 0;
    int len = 1;

    if (sz < 3 || *ptr != expect_prefix) {
        return -1;
    }

    ptr++;
    sz--;
    while (*ptr != '\n') {
        if (*ptr < '0' || *ptr > '9') {
            return -1;
        }
        tmp *= 10;
        tmp += (*ptr - '0');

        ptr++;
        sz--;
        len++;

        if (!sz) {
            return -1;
        }
    }

    *val = tmp;

    return len + 1;
}

/* encodes an integer val into a buffer at ptr
 * prefix = character value we denote this serialization as
 * sz = remaining bytes in buffer
 *
 * returns the number of bytes consumed in the buffer by the serialization
 * or -1 if buffer wasn't large enough
 */
int encodeInteger(char prefix, char *ptr, size_t sz, unsigned long val)
{
    int n = snprintf(ptr, sz, "%c%lu\n", prefix, val);

    if (n >= (int) sz) {
        return -1;
    }
    return n;
}

/* decodes a RedisModuleString from the serialization buffer p
 * sz = remaining size of buffer
 * str = pointer to the RedisModuleString we will create so it can be returned to caller
 *
 * returns the number of bytes consumed from the buffer or -1 upon error.
 */
int decodeString(const char *p, size_t sz, RedisModuleString **str)
{
    int n;

    size_t len;

    n = decodeInteger(p, sz, '$', &len);
    if (n == -1) {
        return -1;
    }
    p += n;
    sz -= n;
    if (len >= sz) {
        return -1;
    }
    *str = RedisModule_CreateString(NULL, p, len);

    return (int) (n + len + 1);
}

/* encodes a RedisModuleString str into the serialization buffer p
 * sz = remaining size of buffer
 *
 * returns the number of bytes consumed from the buffer or -1 upon error
 */
int encodeString(char *p, size_t sz, RedisModuleString *str)
{
    size_t len;
    const char *e = NULL;
    int n;

    if (str == NULL) {
        len = 0;
        e = "";
    } else {
        e = RedisModule_StringPtrLen(str, &len);
    }

    n = encodeInteger('$', p, sz, len);
    if (n == -1) {
        return -1;
    }
    p += n;
    sz -= n;

    if (len >= sz) {
        return -1;
    }

    memcpy(p, e, len);
    p += len;
    *p = '\n';

    return (int) (n + len + 1);
}
