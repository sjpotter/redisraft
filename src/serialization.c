/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include <assert.h>
#include <string.h>
#include "redisraft.h"

/* RaftRedisCommand represents a single Redis command to execute.  Every Raft log entry
 * contains a serialized list of commands in Redis multi-bulk compatible encoding (using \n
 * rather than \r\n termination).
 * For example:
 * *1\n*3\n$3\nSET\n$3\nkey\n$5\nvalue
 */

RaftRedisCommand *RaftRedisCommandArrayExtend(RaftRedisCommandArray *target)
{
    if (target->size == target->len) {
        target->size++;
        target->commands = RedisModule_Realloc(target->commands, target->size * sizeof(RaftRedisCommand *));
    }

    target->commands[target->len] = RedisModule_Calloc(1, sizeof(RaftRedisCommand));
    target->len++;
    return target->commands[target->len - 1];
}

/* Concatenate the commands in the source array to the target array.  The source array
 * will remain empty when this operation is complete!
 */
void RaftRedisCommandArrayMove(RaftRedisCommandArray *target, RaftRedisCommandArray *source)
{
    int i;

    if (target->len + source->len > target->size) {
        target->size = target->len + source->len;
        target->commands = RedisModule_Realloc(target->commands, target->size * sizeof(RaftRedisCommand *));
    }

    for (i = 0; i < source->len; i++) {
        target->commands[target->len] = source->commands[i];
        target->len++;
    }

    source->len = 0;
    memset(source->commands, 0, sizeof(RaftRedisCommand *) * source->size);
}

/* Free a RaftRedisCommand */
void RaftRedisCommandFree(RaftRedisCommand *r)
{
    int i;

    if (r->argv) {
        for (i = 0; i < r->argc; i++) {
            RedisModule_FreeString(NULL, r->argv[i]);
        }
        RedisModule_Free(r->argv);
    }
    r->argc = 0;
}

void RaftRedisCommandArrayFree(RaftRedisCommandArray *array)
{
    int i;

    if (!array) {
        return;
    }

    if (array->commands) {
        for (i = 0; i < array->len; i++) {
            if (!array->commands[i]) {
                continue;
            }
            RaftRedisCommandFree(array->commands[i]);
            RedisModule_Free(array->commands[i]);
            array->commands[i] = NULL;
        }
        RedisModule_Free(array->commands);
        array->commands = NULL;
    }
    array->size = array->len = 0;
}


/* Return expected length of integer value as decimal digits + 2 byte overhead */
static int calcIntSerializedLen(size_t val)
{
    if (val < 10) return 3;
    if (val < 100) return 4;
    if (val < 1000) return 5;
    if (val < 10000) return 6;
    if (val < 100000) return 7;
    if (val < 1000000) return 8;
    if (val < 10000000) return 9;
    return 22;
}

static size_t calcSerializedSize(RaftRedisCommand *cmd)
{
    size_t sz = calcIntSerializedLen(cmd->argc + 1);
    int i;

    for (i = 0; i < cmd->argc; i++) {
        size_t len;
        RedisModule_StringPtrLen(cmd->argv[i], &len);
        sz += calcIntSerializedLen(len) + len + 1;
    }

    return sz;
}

static int encodeInteger(char prefix, char *ptr, size_t sz, unsigned long val)
{
    int n = snprintf(ptr, sz, "%c%lu\n", prefix, val);
    
    if (n >= (int) sz) {
        return -1;
    }
    return n;
}

/* Serialize a number of RaftRedisCommand into a Raft entry */
raft_entry_t *RaftRedisCommandArraySerialize(const RaftRedisCommandArray *source)
{
    size_t sz = calcIntSerializedLen(source->len);
    size_t len;
    int n, i, j;
    char *p;

    /* Compute sizes */
    for (i = 0; i < source->len; i++) {
        sz += calcSerializedSize(source->commands[i]);
    }

    /* Prepare entry */
    raft_entry_t *ety = raft_entry_new(sz);
    p = ety->data;

    /* Encode count */
    n = encodeInteger('*', p, sz, source->len);
    assert (n != -1);
    p += n; sz -= n;

    /* Encode entries */
    for (i = 0; i < source->len; i++) {
        RaftRedisCommand *src = source->commands[i];

        n = encodeInteger('*', p, sz, src->argc);
        assert(n != -1);
        p += n; sz -= n;

        for (j = 0; j < src->argc; j++) {
            const char *e = RedisModule_StringPtrLen(src->argv[j], &len);
            assert(sz > len);

            n = encodeInteger('$', p, sz, len);
            assert(n != -1);
            p += n; sz -= n;

            memcpy(p, e, len);
            p += len;
            *p = '\n';
            p++;
            sz -= (len + 1);
        }
    }

    return ety;
}

static int decodeInteger(const char *ptr, size_t sz, char expect_prefix, size_t *val)
{
    size_t tmp = 0;
    int len = 1;

    if (sz < 3 || *ptr != expect_prefix) {
        return -1;
    }

    ptr++; sz--;
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

    sz--;
    ptr++;
    *val = tmp;

    return len + 1;
}

size_t RaftRedisCommandDeserialize(RaftRedisCommand *target, const void *buf, size_t buf_size)
{
    const char *p = buf;
    int i, n;
    size_t len;

    /* Read argc */
    if ((n = decodeInteger(p, buf_size, '*', &len)) < 0 || !len) {
        return 0;
    }
    p += n; buf_size -= n;
    target->argc = len;
    target->argv = RedisModule_Calloc(len, sizeof(RedisModuleString *));

    /* Read args */
    for (i = 0; i < target->argc; i++) {
        if ((n = decodeInteger(p, buf_size, '$', &len)) < 0) {
            goto error;
        }
        p += n; buf_size -= n;
        if (buf_size <= len) {
            goto error;
        }

        target->argv[i] = RedisModule_CreateString(NULL, p, len);
        p += len + 1;
        buf_size -= (len + 1);
    }

    return p - (char*) buf;

error:
    RaftRedisCommandFree(target);
    return 0;
}

RRStatus RaftRedisCommandArrayDeserialize(RaftRedisCommandArray *target, const void *buf, size_t buf_size)
{
    const char *p = buf;
    size_t commands_num;
    int n;

    if (target->len) {
        RaftRedisCommandArrayFree(target);
    }

    /* Read multibulk count */
    if ((n = decodeInteger(p, buf_size, '*', &commands_num)) < 0 ||
            !commands_num) {
        return RR_ERROR;
    }
    p += n; buf_size -= n;

    /* Allocate array */
    target->len = target->size = commands_num;
    target->commands = RedisModule_Calloc(commands_num, sizeof(RaftRedisCommand*));
    for (size_t i = 0; i < commands_num; i++) {
        target->commands[i] = RedisModule_Calloc(1, sizeof(RaftRedisCommand));
        size_t len = RaftRedisCommandDeserialize(target->commands[i], p, buf_size);
        if (!len) {
            /* Error */
            RaftRedisCommandArrayFree(target);
            return RR_ERROR;
        }

        p += len;
        buf_size -= len;
    }

    return RR_OK;
}

/* serialize out keys in a RaftRedisCommand for locking */
raft_entry_t *RaftRedisLockKeysSerialize(RedisModuleString **argv, size_t argc)
{
    RedisModuleDict * keys = RedisModule_CreateDict(redis_raft.ctx);
    size_t total_key_size = 0;
    int num_keys = 0;

    for (size_t idx = 0; idx < argc; idx++) {
        size_t str_len;
        RedisModule_StringPtrLen(argv[idx], &str_len);

        if (RedisModule_DictSet(keys, argv[idx], NULL) == REDISMODULE_OK) {
            total_key_size += str_len + 1;
            num_keys++;
        }
    }

    raft_entry_t *ety = raft_entry_new(calcIntSerializedLen(num_keys) + total_key_size);
    char *p = ety->data;

    /* Encode number of keys */
    int n = encodeInteger('*', p, total_key_size, num_keys);
    assert (n != -1);
    p += n;

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(keys, "^", NULL, 0);
    char *key;
    size_t key_len;
    while ((key = RedisModule_DictNextC(iter, &key_len, NULL)) != NULL) {
        RedisModule_Assert(total_key_size >= (key_len + 1));
        memcpy(p, key, key_len);
        p += key_len;
        *p = '\0';
        p++;
        total_key_size -= (key_len + 1);
    }
    RedisModule_DictIteratorStop(iter);

    RedisModule_Assert(total_key_size == 0);
    RedisModule_FreeDict(redis_raft.ctx, keys);

    return ety;
}

RedisModuleString ** RaftRedisLockKeysDeserialize(const void *buf, size_t buf_size, size_t *num_keys)
{
    RedisModuleString **ret;

    const char *p = buf;
    int n;
    /* Read number of keys */
    if ((n = decodeInteger(p, buf_size, '*', num_keys)) < 0 || !num_keys) {
        return NULL;
    }
    p += n;

    ret = RedisModule_Alloc(sizeof(RedisModuleString *) * *num_keys);
    for (size_t i = 0; i < *num_keys; i++) {
        size_t str_len = strlen(p);
        ret[i] = RedisModule_CreateString(redis_raft.ctx, p, str_len);
        p += str_len + 1;
    }

    return ret;
}