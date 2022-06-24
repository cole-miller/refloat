struct LogHandle;

struct LogHandle *log_open(int);
void log_close(struct LogHandle *);
