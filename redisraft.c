#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/queue.h>
#include <assert.h>

#include <pthread.h>

#define REDISMODULE_EXPERIMENTAL_API
#include "redismodule.h"
#include "raft.h"

#include "uv.h"
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "hiredis/adapters/libuv.h"

#include "redisraft.h"

redis_raft_t redis_raft = { 0 };

static void serialize_argv(raft_entry_data_t *target, RedisModuleString **argv, int argc)
{
    size_t sz = sizeof(size_t) * (argc + 1);
    size_t len;
    int i;
    char *p;

    /* Compute sizes */
    for (i = 0; i < argc; i++) {
        RedisModule_StringPtrLen(argv[i], &len);
        sz += len;
    }

    /* Serialize argc */
    p = target->buf = RedisModule_Alloc(sz);
    target->len = sz;

    *(size_t *)p = argc;
    p += sizeof(size_t);

    /* Serialize argumnets */
    for (i = 0; i < argc; i++) {
        const char *d = RedisModule_StringPtrLen(argv[i], &len);
        *(size_t *)p = len;
        p += sizeof(size_t);
        memcpy(p, d, len);
        p += len;
    }
}

static int deserialize_argv(RedisModuleCtx *ctx, 
        RedisModuleString ***argv, raft_entry_data_t *source)
{
    char *p = source->buf;
    size_t argc = *(size_t *)p;
    p += sizeof(size_t);

    *argv = RedisModule_Calloc(argc, sizeof(RedisModuleString *));

    int i;
    for (i = 0; i < argc; i++) {
        size_t len = *(size_t *)p;
        p += sizeof(size_t);

        (*argv)[i] = RedisModule_CreateString(ctx, p, len);
        p += len;
    }

    return argc;
}

void execute_log_entry(raft_entry_t *entry)
{
    RedisModuleString **argv;
    int argc = deserialize_argv(redis_raft.ctx, &argv, &entry->data);

    size_t cmdlen;
    const char *cmd = RedisModule_StringPtrLen(argv[0], &cmdlen);

    RedisModule_ThreadSafeContextLock(redis_raft.ctx);
    RedisModuleCallReply *reply = RedisModule_Call(
            redis_raft.ctx, cmd, "v",
            &argv[1],
            argc - 1);
    RedisModule_ThreadSafeContextUnlock(redis_raft.ctx);
}

void execute_committed_req(raft_req_t *req)
{
    RedisModuleString *argv;
    int argc;

    size_t cmdlen;
    const char *cmd = RedisModule_StringPtrLen(req->r.raft.argv[0], &cmdlen);

    RedisModule_ThreadSafeContextLock(req->ctx);
    RedisModuleCallReply *reply = RedisModule_Call(
            req->ctx, cmd, "v",
            &req->r.raft.argv[1],
            req->r.raft.argc - 1);
    RedisModule_ThreadSafeContextUnlock(req->ctx);

    RedisModule_ReplyWithCallReply(req->ctx, reply);
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;
}

void iterate_cqueue(void)
{
    while (!STAILQ_EMPTY(&redis_raft.cqueue)) {
        raft_req_t *req = STAILQ_FIRST(&redis_raft.cqueue);
        if (!raft_msg_entry_response_committed(redis_raft.raft, &req->r.raft.response)) {
            return;
        }

        /* Execute and reply */
        execute_committed_req(req);
        STAILQ_REMOVE_HEAD(&redis_raft.cqueue, entries);
    }
}

void __redis_requestvote_callback(redisAsyncContext *c, void *r, void *privdata)
{
    node_t *node = privdata;
    redisReply *reply = r;
    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements != 2 ||
            reply->element[0]->type != REDIS_REPLY_INTEGER ||
            reply->element[1]->type != REDIS_REPLY_INTEGER) {
        LOG_NODE(node, "invalid RAFT.REQUESTVOTE reply\n");
        return;
    }

    msg_requestvote_response_t response = {
        .term = reply->element[0]->integer,
        .vote_granted = reply->element[1]->integer
    };

    raft_node_t *raft_node = raft_get_node(redis_raft.raft, node->id);
    assert(raft_node != NULL);

    int ret;
    if ((ret = raft_recv_requestvote_response(
            redis_raft.raft,
            raft_node,
            &response)) != 0) {
        LOG("raft_recv_requestvote_response failed => %d\n", ret);
    }
    LOG_NODE(node, "received requestvote response\n");
}


static int __raft_send_requestvote(raft_server_t *raft, void *user_data,
        raft_node_t *raft_node, msg_requestvote_t *msg)
{
    node_t *node = (node_t *) raft_node_get_udata(raft_node);

    /* RAFT.REQUESTVOTE <src_node_id> <term> <candidate_id> <last_log_idx> <last_log_term> */

    if (redisAsyncCommand(node->rc, __redis_requestvote_callback,
                node, "RAFT.REQUESTVOTE %d %d:%d:%d:%d",
                raft_get_nodeid(raft),
                msg->term,
                msg->candidate_id,
                msg->last_log_idx,
                msg->last_log_term) != REDIS_OK) {
        LOG_NODE(node, "failed requestvote");
    }

    return 0;
}

void __redis_appendentries_callback(redisAsyncContext *c, void *r, void *privdata)
{
    node_t *node = privdata;
    redisReply *reply = r;
    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements != 4 ||
            reply->element[0]->type != REDIS_REPLY_INTEGER ||
            reply->element[1]->type != REDIS_REPLY_INTEGER ||
            reply->element[2]->type != REDIS_REPLY_INTEGER ||
            reply->element[3]->type != REDIS_REPLY_INTEGER) {
        LOG_NODE(node, "invalid RAFT.APPENDENTRIES reply\n");
        return;
    }

    msg_appendentries_response_t response = {
        .term = reply->element[0]->integer,
        .success = reply->element[1]->integer,
        .current_idx = reply->element[2]->integer,
        .first_idx = reply->element[3]->integer
    };

    raft_node_t *raft_node = raft_get_node(redis_raft.raft, node->id);
    assert(raft_node != NULL);

    int ret;
    if ((ret = raft_recv_appendentries_response(
            redis_raft.raft,
            raft_node,
            &response)) != 0) {
        LOG_NODE(node, "raft_recv_appendentries_response failed => %d\n", ret);
    }
    LOG_NODE(node, "received appendentries response\n");

    /* Maybe we have pending stuff to apply now */
    iterate_cqueue(); 
}

static int __raft_send_appendentries(raft_server_t *raft, void *user_data,
        raft_node_t *raft_node, msg_appendentries_t *msg)
{
    node_t *node = (node_t *) raft_node_get_udata(raft_node);
    int argc = 4 + msg->n_entries * 2;
    char *argv[argc];
    size_t argvlen[argc];

    if (node->state != NODE_CONNECTED) {
        // TODO try to buffer
        LOG_NODE(node, "not connected");
        return 0;
    }

    argv[0] = "RAFT.APPENDENTRIES";
    argvlen[0] = strlen(argv[0]);
    argvlen[1] = asprintf(&argv[1], "%d", raft_get_nodeid(raft));
    argvlen[2] = asprintf(&argv[2], "%d:%d:%d:%d",
            msg->term, 
            msg->prev_log_idx,
            msg->prev_log_term,
            msg->leader_commit);
    argvlen[3] = asprintf(&argv[3], "%d", msg->n_entries);

    int i;
    for (i = 0; i < msg->n_entries; i++) {
        raft_entry_t *e = &msg->entries[i];
        argvlen[4 + i*2] = asprintf(&argv[4 + i*2], "%d:%d:%d", e->term, e->id, e->type);
        argvlen[5 + i*2] = e->data.len;
        argv[5 + i*2] = e->data.buf;
    }

    if (redisAsyncCommandArgv(node->rc, __redis_appendentries_callback,
                node, argc, (const char **)argv, argvlen) != REDIS_OK) {
        LOG_NODE(node, "failed appendentries");
    }
    return 0;
}

static int __raft_persist_vote(raft_server_t *raft, void *user_data, int vote)
{
    fprintf(stderr, "__raft_persist_vote %d\n", vote);
    return 0;
}

static int __raft_persist_term(raft_server_t *raft, void *user_data, int term, int vote)
{
    fprintf(stderr, "__raft_persist_term term=%d vote=%d\n", term, vote);
    return 0;
}

static void __raft_log(raft_server_t *raft, raft_node_t *node, void *user_data, const char *buf)
{
    fprintf(stderr, "[%d] raft log>> %s\n", raft_get_nodeid(raft), buf);
}

static int __raft_log_offer(raft_server_t *raft, void *user_data, raft_entry_t *entry, int entry_idx)
{
    fprintf(stderr, "[%d] log offer idx=%d\n", raft_get_nodeid(raft), entry_idx);
    return 0;
}

static int __raft_log_pop(raft_server_t *raft, void *user_data, raft_entry_t *entry, int entry_idx)
{
    fprintf(stderr, "[%d] log pop idx=%d\n", raft_get_nodeid(raft), entry_idx);
    return 0;
}

static int __raft_applylog(raft_server_t *raft, void *user_data, raft_entry_t *entry, int entry_idx)
{
    execute_log_entry(entry);
    return 0;
}

raft_cbs_t redis_raft_callbacks = {
    .send_requestvote = __raft_send_requestvote,
    .send_appendentries = __raft_send_appendentries,
    .persist_vote = __raft_persist_vote,
    .persist_term = __raft_persist_term,
    .log_offer = __raft_log_offer,
    .log_pop = __raft_log_pop,
    .log = __raft_log,
    .applylog = __raft_applylog,
};


/**********************************************************************/

static void free_request(raft_req_t *req)
{
    RedisModule_Free(req);
}

static void raft_req_free(raft_req_t *req)
{
    RedisModule_Free(req);
}

static raft_req_t *raft_req_init(RedisModuleCtx *ctx, enum raft_req_type type)
{
    raft_req_t *req = RedisModule_Alloc(sizeof(raft_req_t));
    if (ctx != NULL) {
        req->client = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
        req->ctx = RedisModule_GetThreadSafeContext(req->client);
    }
    req->type = type;
    
    return req;
}

static void enqueue_request(raft_req_t *req)
{
    STAILQ_INSERT_TAIL(&redis_raft.rqueue, req, entries);
    uv_async_send(&redis_raft.rqueue_sig);
}

static int enqueue_raft_command(RedisModuleCtx *ctx, int type,
        RedisModuleString **argv, int argc)
{
    raft_req_t *req = RedisModule_Alloc(sizeof(raft_req_t));
    req->client = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    req->ctx = RedisModule_GetThreadSafeContext(req->client);

    STAILQ_INSERT_TAIL(&redis_raft.rqueue, req, entries);
    uv_async_send(&redis_raft.rqueue_sig);

    return REDISMODULE_OK;
}

#define LOGLEVEL_DEBUG      "debug"
#define LOGLEVEL_VERBOSE    "verbose"
#define LOGLEVEL_NOTICE     "notice"
#define LOGLEVEL_WARNING    "warning"

#define VALID_NODE_ID(x)    ((x) > 0)

static int _cmd_raft_requestvote(raft_req_t *req)
{
    msg_requestvote_response_t response;

    if (raft_recv_requestvote(redis_raft.raft,
                raft_get_node(redis_raft.raft, req->r.requestvote.src_node_id),
                &req->r.requestvote.msg,
                &response) != 0) {
        RedisModule_ReplyWithError(req->ctx, "operation failed"); // TODO: Identify cases
        goto exit;
    }

    RedisModule_ReplyWithArray(req->ctx, 2);
    RedisModule_ReplyWithLongLong(req->ctx, response.term);
    RedisModule_ReplyWithLongLong(req->ctx, response.vote_granted);

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;

    return REDISMODULE_OK;
}


static int _cmd_raft_appendentries(raft_req_t *req)
{
    msg_appendentries_response_t response;

    if (raft_recv_appendentries(redis_raft.raft,
                raft_get_node(redis_raft.raft, req->r.appendentries.src_node_id),
                &req->r.appendentries.msg,
                &response) != 0) {
        RedisModule_ReplyWithError(req->ctx, "operation failed"); // TODO: Identify cases
        goto exit;
    }

    RedisModule_ReplyWithArray(req->ctx, 4);
    RedisModule_ReplyWithLongLong(req->ctx, response.term);
    RedisModule_ReplyWithLongLong(req->ctx, response.success);
    RedisModule_ReplyWithLongLong(req->ctx, response.current_idx);
    RedisModule_ReplyWithLongLong(req->ctx, response.first_idx);

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;

    return REDISMODULE_OK;
}

static int _cmd_raft_addnode(raft_req_t *req)
{
    node_t *node = node_init(req->r.addnode.id, &req->r.addnode.addr);

    /* Before attempting to connect, try to add the node */
    raft_node_t *raft_node;
    if (!(raft_node = raft_add_node(redis_raft.raft, node, node->id, 0))) {
        if (req->ctx) RedisModule_ReplyWithError(req->ctx, "node id exists");
        node_free(node);
        goto exit;
    }

    /* Connect */
    node_connect(node, &redis_raft);
    if (!req->ctx) return REDISMODULE_OK;

    RedisModule_ReplyWithSimpleString(req->ctx, "OK");

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;
    return REDISMODULE_OK;
}

static int _cmd_raft(raft_req_t *req)
{
    raft_node_t *leader = raft_get_current_leader_node(redis_raft.raft);
    if (!leader) {
        RedisModule_ReplyWithError(req->ctx, "-NOLEADER");
        goto exit;
    }
    if (raft_node_get_id(leader) != raft_get_nodeid(redis_raft.raft)) {
        node_t *l = raft_node_get_udata(leader);
        char *reply;
       
        asprintf(&reply, "LEADERIS %s:%u", l->addr.host, l->addr.port);

        RedisModule_ReplyWithError(req->ctx, reply);
        goto exit;
    }

    raft_entry_t entry = {
        .id = rand(),
        .type = RAFT_LOGTYPE_NORMAL,
    };

    serialize_argv(&entry.data, req->r.raft.argv, req->r.raft.argc);
    int e = raft_recv_entry(redis_raft.raft, &entry, &req->r.raft.response);
    if (e) {
        // todo handle errors
        RedisModule_Free(entry.data.buf);
        RedisModule_ReplyWithSimpleString(req->ctx, "ERROR");
        goto exit;
    }

    // We're now waiting 
    STAILQ_INSERT_TAIL(&redis_raft.cqueue, req, entries);

    return REDISMODULE_OK;

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;
    return REDISMODULE_OK;

}


static void handle_cmd(uv_async_t *handle)
{
    while (!STAILQ_EMPTY(&redis_raft.rqueue)) {
        raft_req_t *req = STAILQ_FIRST(&redis_raft.rqueue);
        raft_req_callbacks[req->type](req);
        STAILQ_REMOVE_HEAD(&redis_raft.rqueue, entries);
        raft_req_free(req);
    }
}

static void __raft_timer(uv_timer_t *handle)
{
    redis_raft_t *rr = (redis_raft_t *) uv_handle_get_data((uv_handle_t *) handle);

    raft_periodic(rr->raft, 500);
}

static void redis_raft_thread(void *arg)
{
    redis_raft_t *rr = (redis_raft_t *) arg;

    rr->loop = RedisModule_Alloc(sizeof(uv_loop_t));
    uv_loop_init(rr->loop);

    uv_async_init(rr->loop, &rr->rqueue_sig, handle_cmd);

    uv_timer_init(rr->loop, &rr->ptimer);
    uv_handle_set_data((uv_handle_t *) &rr->ptimer, rr);
    uv_timer_start(&rr->ptimer, __raft_timer, 5000, 500);

    uv_run(rr->loop, UV_RUN_DEFAULT);
}


int redis_raft_init(RedisModuleCtx *ctx, redis_raft_t *rr, int node_id)
{
    memset(rr, 0, sizeof(redis_raft_t));
    STAILQ_INIT(&rr->rqueue);
    STAILQ_INIT(&rr->cqueue);
    rr->ctx = RedisModule_GetThreadSafeContext(NULL);

    /* Initialize raft library */
    rr->raft = raft_new();
    if (!raft_add_node(rr->raft, NULL, node_id, 1)) {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Failed to initialize raft_node");
        return REDISMODULE_ERR;
    }
    raft_set_callbacks(rr->raft, &redis_raft_callbacks, NULL);

    /* Start RAFT thread */
    if (uv_thread_create(&rr->thread, redis_raft_thread, &redis_raft) < 0) {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Failed to initialize redis_raft thread");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

raft_req_callback_t raft_req_callbacks[] = {
    NULL,   /* 0 */
    _cmd_raft_addnode,
    _cmd_raft_appendentries,
    _cmd_raft_requestvote,
    _cmd_raft,
    NULL
};

/**********************************************************************/

int cmd_raft_addnode(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    redis_raft_t *rr = &redis_raft;

    if (argc != 3) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    /* Validate node id */
    long long node_id;
    if (RedisModule_StringToLongLong(argv[1], &node_id) != REDISMODULE_OK ||
        !VALID_NODE_ID(node_id)) {
            RedisModule_ReplyWithError(ctx, "invalid node id");
            return REDISMODULE_OK;
    }

    /* Parse address */
    node_addr_t node_addr;
    size_t node_addr_len;
    const char *node_addr_str = RedisModule_StringPtrLen(argv[2], &node_addr_len);
    if (!parse_node_addr(node_addr_str, node_addr_len, &node_addr)) {
        RedisModule_ReplyWithError(ctx, "invalid node address");
        return REDISMODULE_OK;
    }

    raft_req_t *req = raft_req_init(ctx, RAFT_REQ_ADDNODE);
    req->r.addnode.id = node_id;
    req->r.addnode.addr = node_addr;
    enqueue_request(req);

    return REDISMODULE_OK;
}

int cmd_raft_requestvote(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    redis_raft_t *rr = &redis_raft;

    /* RAFT.REQUESTVOTE <src_node_id> <term>:<candidate_id>:<last_log_idx>:<last_log_term> */
    if (argc != 3) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    raft_req_t *req = raft_req_init(ctx, RAFT_REQ_REQUESTVOTE);
    if (rmstring_to_int(argv[1], &req->r.requestvote.src_node_id) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "invalid source node id");
        goto error_cleanup;
    }

    size_t tmplen;
    const char *tmpstr = RedisModule_StringPtrLen(argv[2], &tmplen);
    if (sscanf(tmpstr, "%d:%d:%d:%d",
                &req->r.requestvote.msg.term,
                &req->r.requestvote.msg.candidate_id,
                &req->r.requestvote.msg.last_log_idx,
                &req->r.requestvote.msg.last_log_term) != 4) {
        RedisModule_ReplyWithError(ctx, "invalid message");
        goto error_cleanup;
    }

    enqueue_request(req);
    return REDISMODULE_OK;

error_cleanup:
    free_request(req);
    return REDISMODULE_OK;
}

int cmd_raft(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc < 2) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    raft_req_t *req = raft_req_init(ctx, RAFT_REQ_REDISCOMMAND);
    req->r.raft.argc = argc - 1;
    req->r.raft.argv = &argv[1];
    enqueue_request(req);

    return REDISMODULE_OK;
}

int cmd_raft_appendentries(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    redis_raft_t *rr = &redis_raft;

    /* RAFT.APPENDENTRIES <src_node_id> <term>:<prev_log_idx>:<prev_log_term>:<leader_commit>
     *      <n_entries> {<term:id:type> <entry>}...
     */

    if (argc < 4) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    long long n_entries;
    if (RedisModule_StringToLongLong(argv[3], &n_entries) != REDIS_OK) {
        RedisModule_ReplyWithError(ctx, "invalid n_entries value");
        return REDISMODULE_OK;
    }
    if (argc != 4 + 2 * n_entries) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    raft_req_t *req = raft_req_init(ctx, RAFT_REQ_APPENDENTRIES);
    if (rmstring_to_int(argv[1], &req->r.appendentries.src_node_id) == REDISMODULE_ERR) {
        RedisModule_ReplyWithError(ctx, "invalid source node id");
        goto error_cleanup;
    }

    int i;
    size_t tmplen;
    const char *tmpstr = RedisModule_StringPtrLen(argv[2], &tmplen);
    if (sscanf(tmpstr, "%d:%d:%d:%d",
                &req->r.appendentries.msg.term,
                &req->r.appendentries.msg.prev_log_idx,
                &req->r.appendentries.msg.prev_log_term,
                &req->r.appendentries.msg.leader_commit) != 4) {
        RedisModule_ReplyWithError(ctx, "invalid message");
        goto error_cleanup;
    }

    req->r.appendentries.msg.n_entries = n_entries;
    req->r.appendentries.msg.entries = RedisModule_Calloc(n_entries, sizeof(req->r.appendentries.msg.entries[0]));
    for (i = 0; i < n_entries; i++) {
        msg_entry_t *e = &req->r.appendentries.msg.entries[i];

        tmpstr = RedisModule_StringPtrLen(argv[4 + 2*i], &tmplen);
        if (sscanf(tmpstr, "%d:%d:%d",
                    &e->term,
                    &e->id,
                    &e->type) != 3) {
            RedisModule_ReplyWithError(ctx, "invalid entry");
            goto error_cleanup;
        }

        tmpstr = RedisModule_StringPtrLen(argv[5 + 2*i], &tmplen);
        e->data.buf = RedisModule_Alloc(tmplen);
        e->data.len = tmplen;
        memcpy(e->data.buf, tmpstr, tmplen);
    }

    enqueue_request(req);
    return REDISMODULE_OK;

error_cleanup:
    free_request(req);
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (RedisModule_Init(ctx, "redisraft", 1, REDISMODULE_APIVER_1) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    if (argc < 1) {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Missing node ID");
        return REDISMODULE_ERR;
    }
    
    long long id;
    if (RedisModule_StringToLongLong(argv[0], &id) != REDISMODULE_OK || id <= 0) {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Invalid node ID");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft",
                cmd_raft, "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.addnode",
                cmd_raft_addnode, "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.appendentries",
                cmd_raft_appendentries, "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.requestvote",
                cmd_raft_requestvote, "write", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (redis_raft_init(ctx, &redis_raft, id) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    /* Configure additional nodes -- TODO: replace with better syntax, error handling */
    sleep(1);
    int i;
    for (i = 1; i < argc; i++) {
        size_t tmplen;
        const char *tmpstr = RedisModule_StringPtrLen(argv[i], &tmplen);

        const char *colon = memchr(tmpstr, ':', tmplen);
        int node_id_len = colon - tmpstr;
        char node_id_str[node_id_len + 1];
        memcpy(node_id_str, tmpstr, node_id_len);
        node_id_str[node_id_len] = '\0';
        int node_id = strtoul(node_id_str, NULL, 10);

        node_addr_t node_addr;
        if (!parse_node_addr(colon + 1, tmplen - node_id_len - 1, &node_addr)) {
            return REDISMODULE_ERR;
        }

        raft_req_t *req = raft_req_init(NULL, RAFT_REQ_ADDNODE);
        req->r.addnode.id = node_id;
        req->r.addnode.addr = node_addr;
        enqueue_request(req);
    }

    return REDISMODULE_OK;
}
