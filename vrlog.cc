#include "vrlog.hh"

std::ostream& operator<<(std::ostream& str, const Vrlogitem& x) {
    if (x.is_real())
        return str << x.request << "@" << x.viewno;
    else
        return str << "~empty~";
}
