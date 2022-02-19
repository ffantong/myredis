#include "redismodule.h"
//#include "server.h"
#include <unistd.h>

static char * getTypeName(int type) {
    switch (type) {
        case REDISMODULE_KEYTYPE_EMPTY:
            return "none";
        case REDISMODULE_KEYTYPE_STRING:
            return "string";
        case REDISMODULE_KEYTYPE_LIST:
            return "list";
        case REDISMODULE_KEYTYPE_HASH:
            return "hash";
        case REDISMODULE_KEYTYPE_SET:
            return "set";
        case REDISMODULE_KEYTYPE_ZSET:
            return "zset";
        default:
            return "none";
    }
}

int GetKeyType_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    int keyType = RedisModule_KeyType(key);
    RedisModule_ReplyWithSimpleString(ctx, getTypeName(keyType));
    return REDISMODULE_OK;
}

int GetPid_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_ReplyWithLongLong(ctx, getpid());
    return REDISMODULE_OK;
}

int ClusterCall_Command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "GET", "s", argv[1]);
    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        return RedisModule_ReplyWithCallReply(ctx, reply);
    }
    size_t len;
    const char *ptr = RedisModule_CallReplyStringPtr(reply,&len);

    RedisModule_ReplyWithStringBuffer(ctx, ptr, len);
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"helloworld",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"gettype",
                                  GetKeyType_Command, "readonly",
                                  0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"getpid",
                                  GetPid_Command, "readonly",
                                  0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"clusterget",
                                  ClusterCall_Command, "readonly",
                                  0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}