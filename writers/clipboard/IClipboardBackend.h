#ifndef TESTEXPORT_CLIPBOARD_IBACKEND_H
#define TESTEXPORT_CLIPBOARD_IBACKEND_H

#include <memory>
#include <string>

class IClipboardBackend {
public:
  virtual ~IClipboardBackend() = default;
  virtual bool SetPlainTextUtf8(const std::string &text, std::string *err) = 0;
};

std::unique_ptr<IClipboardBackend> CreateClipboardBackend(std::string *err);

#endif
