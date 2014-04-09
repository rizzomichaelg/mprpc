#include "logger.hh"

std::ostream& operator<<(std::ostream& out, const timeval& tv) {
    char buf[40];
    int x = sprintf(buf, "%ld.%06ld", (long) tv.tv_sec, (long) tv.tv_usec);
    out.write(buf, x);
    return out;
}
