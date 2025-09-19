#include <libwebsockets.h>
#include <string.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <cjson/cJSON.h>

static int interrupted = 0;
static struct lws *client_wsi = NULL;

const char *symbols[] = {
    "BTC-USDT","ADA-USDT","ETH-USDT",
    "DOGE-USDT","XRP-USDT","SOL-USDT",
    "LTC-USDT","BNB-USDT"
};
const int num_symbols = 8;

static const char *sub_template =
    "{ \"id\":\"5323\", \"op\":\"subscribe\", \"args\":[ %s ] }";

static char subscribe_msg[2048];

// Σύνθεση JSON subscription string
void build_subscribe_message() {
    char args[1024] = "";
    for (int i=0; i<num_symbols; i++) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "{\"channel\":\"trades\",\"instId\":\"%s\"}%s",
                 symbols[i], (i < num_symbols-1) ? "," : "");
        strcat(args, buf);
    }
    snprintf(subscribe_msg, sizeof(subscribe_msg), sub_template, args);
}

// Callback για όλα τα websocket events
static int callback_ws(struct lws *wsi, enum lws_callback_reasons reason,
                       void *user, void *in, size_t len) {

    switch (reason) {

        case LWS_CALLBACK_CLIENT_ESTABLISHED:
            lwsl_user("Connected to server, sending subscribe...\n");
            lws_callback_on_writable(wsi);
            break;

        case LWS_CALLBACK_CLIENT_WRITEABLE: {
            unsigned char buf[LWS_PRE + 2048];
            size_t n = snprintf((char *)&buf[LWS_PRE], sizeof(buf)-LWS_PRE,
                                "%s", subscribe_msg);
            lws_write(wsi, &buf[LWS_PRE], n, LWS_WRITE_TEXT);
            lwsl_user("Subscribe message sent: %s\n", subscribe_msg);
            break;
        }

        case LWS_CALLBACK_CLIENT_RECEIVE: {
            // Λήψη μηνύματος
            char *msg = (char *)in;
            // printf("RX: %s\n", msg);

            // Parse JSON με cJSON
            cJSON *json = cJSON_Parse(msg);
            if (!json) break;

            cJSON *arg = cJSON_GetObjectItem(json, "arg");
            cJSON *channel = arg ? cJSON_GetObjectItem(arg, "channel") : NULL;

            if (channel && strcmp(channel->valuestring, "trades") == 0) {
                const char *symbol = cJSON_GetObjectItem(arg, "instId")->valuestring;
                cJSON *data = cJSON_GetObjectItem(json, "data");
                if (cJSON_IsArray(data)) {
                    cJSON *trade;
                    cJSON_ArrayForEach(trade, data) {
                        const char *tradeID = cJSON_GetObjectItem(trade, "tradeId")->valuestring;
                        const char *px = cJSON_GetObjectItem(trade, "px")->valuestring;
                        const char *sz = cJSON_GetObjectItem(trade, "sz")->valuestring;
                        const char *ts = cJSON_GetObjectItem(trade, "ts")->valuestring;

                        long long ts_exchange = atoll(ts);
                        long long ts_received = (long long)(time(NULL)) * 1000;
                        long long delay = ts_received - ts_exchange;

                        // Save σε αρχείο
                        char filename[128];
                        snprintf(filename, sizeof(filename), "trades/%s.csv", symbol);
                        FILE *f = fopen(filename, "a");
                        if (f) {
                            fprintf(f, "%s,%lld,%lld,%lld,%s,%s\n",
                                    tradeID, ts_exchange, ts_received, delay, px, sz);
                            fclose(f);
                        }
                    }
                }
            }
            cJSON_Delete(json);
            break;
        }

        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
            lwsl_err("Connection error\n");
            interrupted = 1;
            break;

        case LWS_CALLBACK_CLOSED:
            lwsl_notice("Connection closed\n");
            interrupted = 1;
            break;

        default:
            break;
    }
    return 0;
}

// Πρωτόκολλο
static struct lws_protocols protocols[] = {
    { "example-protocol", callback_ws, 0, 0, },
    { NULL, NULL, 0, 0 }
};

int main(void) {
    struct lws_context_creation_info info;
    memset(&info, 0, sizeof info);
    info.port = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;

    struct lws_context *context = lws_create_context(&info);
    if (!context) {
        fprintf(stderr, "lws init failed\n");
        return -1;
    }

    build_subscribe_message();

    struct lws_client_connect_info ccinfo = {0};
    ccinfo.context = context;
    ccinfo.address = "ws.okx.com";
    ccinfo.port = 8443;
    ccinfo.path = "/ws/v5/public";
    ccinfo.host = lws_canonical_hostname(context);
    ccinfo.origin = "origin";
    ccinfo.protocol = protocols[0].name;
    ccinfo.ssl_connection = LCCSCF_USE_SSL;

    client_wsi = lws_client_connect_via_info(&ccinfo);

    while (!interrupted)
        lws_service(context, 1000);

    lws_context_destroy(context);
    return 0;
}
