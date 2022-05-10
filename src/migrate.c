#include <string.h>

#include "redisraft.h"

void raftAppendRaftDeleteEntry(RedisRaftCtx *rr, RaftReq *req)
{
    raft_entry_resp_t response;

    raft_entry_t *entry = RaftRedisLockKeysSerialize(req->r.migrate_keys.keys, req->r.migrate_keys.num_keys);
    entry->id = rand();
    entry->type = RAFT_LOGTYPE_DELETE_UNLOCK_KEYS;
    entryAttachRaftReq(rr, entry, req);

    int e = raft_recv_entry(rr->raft, entry, &response);
    if (e != 0) {
        replyRaftError(req->ctx, e);
        entryDetachRaftReq(rr, entry);
        raft_entry_release(entry);
        goto exit;
    }

    raft_entry_release(entry);

    /* Unless applied by raft_apply_all() (and freed by it), the request
     * is pending so we don't free it or unblock the client.
     */
    return;

    exit:
    RaftReqFree(req);
}

static void transferKeysResponse(redisAsyncContext *c, void *r, void *privdata)
{
    LOG_WARNING("calling transferKeysResponse");
    Connection *conn = privdata;
    JoinLinkState *state = ConnGetPrivateData(conn);
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);
    RaftReq *req = state->req;

    redisReply *reply = r;

    if (!reply) {
        LOG_WARNING("RAFT.IMPORT failed: connection dropped.");
        ConnMarkDisconnected(conn);
    } else if (reply->type == REDIS_REPLY_ERROR) {
        ConnAsyncTerminate(conn);

        LOG_WARNING("RAFT.IMPORT failed: %.*s", (int) reply->len, reply->str);
        RedisModule_ReplyWithError(req->ctx, "ERR: Migrate failed importing keys into remote cluster, try again");
        RaftReqFree(req);
    } else if (reply->type != REDIS_REPLY_STATUS || reply->len != 2 || strncmp(reply->str, "OK", 2)) {
        ConnAsyncTerminate(conn);

        /* FIXME: above should be changed to string eventually? */
        LOG_WARNING("RAFT.IMPORT unexpected response: type = %d (wanted %d), len = %ld, response = %.*s", reply->type, REDIS_REPLY_STATUS, reply->len, (int) reply->len, reply->str);
        RedisModule_ReplyWithError(req->ctx, "ERR: received unexpected response from remote cluster, see logs");
        RaftReqFree(req);
    } else {
        ConnAsyncTerminate(conn);
        raftAppendRaftDeleteEntry(rr, req);
    }

    redisAsyncDisconnect(c);
}

static void transferKeys(Connection *conn)
{
    LOG_WARNING("calling transferKeys");
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);
    JoinLinkState *state = ConnGetPrivateData(conn);
    RaftReq *req = state->req;

    /* Connection is not good?  Terminate and continue */
    if (!ConnIsConnected(conn)) {
        return;
    }

    ShardGroup * sg = getShardGroupById(rr, req->r.migrate_keys.shardGroupId);
    if (sg == NULL) {
        //FIXME: error
    }

    // raft.import term magic <key1_name> <key1_serialized> ... <keyn_name> <keyn_serialized>
    int argc = 3 + (req->r.migrate_keys.num_serialized_keys * 2);
    char **argv = RedisModule_Calloc(argc, sizeof(char *));
    size_t *argv_len = RedisModule_Calloc(argc, sizeof(size_t));

    argv[0] = RedisModule_Strdup("RAFT.IMPORT");
    argv_len[0] = strlen("RAFT.IMPORT");
    argv[1] = RedisModule_Alloc(32);
    int n = snprintf(argv[1], 64, "%ld", req->r.migrate_keys.migrate_term);
    argv_len[1] = n;
    argv[2] = RedisModule_Alloc(32);

    for (size_t i = 0; i < req->r.migrate_keys.num_keys; i++) {
        if (req->r.migrate_keys.keys_serialized[i] == NULL) {
            continue;
        }

        size_t key_len;
        const char * key = RedisModule_StringPtrLen(req->r.migrate_keys.keys[i], &key_len);
        argv[3 + (i*2)] = RedisModule_Strdup(key);
        argv_len[3+ (i*2)] = key_len;

        size_t str_len;
        const char * str = RedisModule_StringPtrLen(req->r.migrate_keys.keys_serialized[i], &str_len);
        argv[3 + (i*2) + 1] = RedisModule_Alloc(str_len);
        memcpy(argv[3 + (i*2) + 1], str, str_len);
        argv_len[3 + (i*2) + 1] = str_len;
    }

    if (redisAsyncCommandArgv(ConnGetRedisCtx(conn), transferKeysResponse, conn, argc, (const char **) argv, argv_len) != REDIS_OK) {
        redisAsyncDisconnect(ConnGetRedisCtx(conn));
        ConnMarkDisconnected(conn);
    }

    for(int i = 0; i < argc; i++) {
        RedisModule_Free(argv[i]);
    }
    RedisModule_Free(argv);
    RedisModule_Free(argv_len);
}


void MigrateKeys(RedisRaftCtx *rr, RaftReq *req)
{
    JoinLinkState *state = RedisModule_Calloc(1, sizeof(*state));
    state->type = "migrate";
    state->connect_callback = transferKeys;
    time(&(state->start));
    ShardGroup * sg = getShardGroupById(rr, req->r.migrate_keys.shardGroupId);
    if (sg == NULL) {
        RedisModule_ReplyWithError(req->ctx, "ERR couldn't resolve shardgroup id");
        goto exit;
    }
    req->r.migrate_keys.migrate_term = raft_get_current_term(rr->raft);

    for (size_t i = 0; i < req->r.migrate_keys.num_keys; i++) {
        RedisModuleString *key = req->r.migrate_keys.keys[i];

        if (RedisModule_KeyExists(req->ctx, key)) {
            req->r.migrate_keys.num_serialized_keys++;

            enterRedisModuleCall();
            RedisModuleCallReply *reply = RedisModule_Call(rr->ctx, "DUMP", "c", key);
            exitRedisModuleCall();

            if (reply && RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING) {
                req->r.migrate_keys.keys_serialized[i] = RedisModule_CreateStringFromCallReply(reply);
            } else {
                if (reply) {
                    LOG_WARNING("unexpected response type = %d", RedisModule_CallReplyType(reply));
                    RedisModule_FreeCallReply(reply);
                } else {
                    LOG_WARNING("didn't get a reply!");
                }
                RedisModule_ReplyWithError(req->ctx, "ERR see logs");
                goto exit;
            }
        }
    }

    /* nothing to migrate, return quickly */
    if (req->r.migrate_keys.num_serialized_keys == 0) {
        RedisModule_ReplyWithCString(req->ctx, "OK");
        goto exit;
    }

     for (unsigned int i = 0; i < sg->nodes_num; i++) {
        LOG_WARNING("MigrateKeys: adding %s:%d", sg->nodes[i].addr.host, sg->nodes[i].addr.port);
        NodeAddrListAddElement(&state->addr, &sg->nodes[i].addr);
    }
    state->req = req;

    state->conn = ConnCreate(rr, state, joinLinkIdleCallback, joinLinkFreeCallback);
    return;

exit:
    RaftReqFree(req);
}