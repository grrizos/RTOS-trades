// latency_log.c
#include "latency_log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>


static FILE *g_logfile = NULL;

// store the next deadline
static struct timespec g_deadline = {0,0};

// initialization 
void latency_log_init(const char *path) {
    g_logfile = fopen(path, "w");
    if (!g_logfile) {
        perror("latency_log_init: fopen");
        exit(EXIT_FAILURE);
    }
    // write header
    fprintf(g_logfile, "timestamp_ns,work,latency_us,cpu_idle_percent\n");
    fflush(g_logfile);
}

// set deadline 
void latency_log_set_deadline(struct timespec deadline) {
    g_deadline = deadline;
}

// log metrics 
void latency_log_write(const char *task, struct timespec actual) {
    if (!g_logfile) return;

    // latency in microseconds
    long long ns_gap =
        (actual.tv_sec - g_deadline.tv_sec) * 1000000000LL +
        (actual.tv_nsec - g_deadline.tv_nsec);
    double latency_us = ns_gap / 1000.0;

    // CPU idle measurement
    static unsigned long long prev_idle = 0, prev_total = 0;
    FILE *fp = fopen("/proc/stat", "r");
    if (!fp) return;

    char label[16];
    unsigned long long user, nice, sys, idle, iowait, irq, softirq, steal;
    if (fscanf(fp, "%15s %llu %llu %llu %llu %llu %llu %llu %llu",
               label, &user, &nice, &sys, &idle,
               &iowait, &irq, &softirq, &steal) != 9) {
        fclose(fp);
        return;
    }
    fclose(fp);

    unsigned long long idle_time = idle + iowait;
    unsigned long long total_time = user + nice + sys + idle + iowait +
                                    irq + softirq + steal;

    double idle_pct = 0.0;
    if (prev_total > 0) {
        unsigned long long delta_total = total_time - prev_total;
        unsigned long long delta_idle = idle_time - prev_idle;
        if (delta_total > 0) {
            idle_pct = (100.0 * delta_idle) / delta_total;
        }
    }
    prev_total = total_time;
    prev_idle = idle_time;

    // current time 
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    long long now_ns = (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;

    // write row
    fprintf(g_logfile, "%lld,%s,%.3f,%.2f\n", now_ns, task, latency_us, idle_pct);
    fflush(g_logfile);
}

// --- close logger ---
void latency_log_close(void) {
    if (g_logfile) {
        fclose(g_logfile);
        g_logfile = NULL;
    }
}
