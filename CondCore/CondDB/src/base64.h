#include <string>
#include "CondCore/CondDB/interface/Binary.h"

std::string base64_encode(const cond::Binary& blob);
std::string base64_decode(std::string const& s);
