#ifndef LATENCY_LOG_H
#define LATENCY_LOG_H

#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

void latency_log_init(const char *path);
void latency_log_set_deadline(struct timespec deadline);
void latency_log_write(const char *task, struct timespec actual);
void latency_log_close(void);

#ifdef __cplusplus
}
#endif

#endif
