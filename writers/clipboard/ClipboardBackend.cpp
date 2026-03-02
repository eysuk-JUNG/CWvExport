#include "IClipboardBackend.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#if defined(_WIN32)
#include <windows.h>
#endif

namespace {
#if defined(_WIN32)
class ClipboardBackendWin final : public IClipboardBackend {
public:
  bool SetPlainTextUtf8(const std::string &text, std::string *err) override {
    const int needed_wchars =
        MultiByteToWideChar(CP_UTF8, 0, text.c_str(), static_cast<int>(text.size()), nullptr, 0);
    if (needed_wchars < 0) {
      *err = "MultiByteToWideChar size query failed.";
      return false;
    }

    std::vector<wchar_t> wtext(static_cast<size_t>(needed_wchars) + 1, L'\0');
    if (needed_wchars > 0) {
      const int converted = MultiByteToWideChar(CP_UTF8, 0, text.c_str(),
                                                static_cast<int>(text.size()), wtext.data(),
                                                needed_wchars);
      if (converted != needed_wchars) {
        *err = "MultiByteToWideChar conversion failed.";
        return false;
      }
    }

    constexpr int kOpenRetries = 50;
    bool opened = false;
    for (int i = 0; i < kOpenRetries; ++i) {
      if (OpenClipboard(nullptr) != 0) {
        opened = true;
        break;
      }
      Sleep(10);
    }
    if (!opened) {
      *err = "OpenClipboard failed after retries. GetLastError=" +
             std::to_string(static_cast<unsigned long>(GetLastError()));
      return false;
    }

    HGLOBAL mem = nullptr;
    bool ok = false;
    do {
      if (EmptyClipboard() == 0) {
        *err = "EmptyClipboard failed. GetLastError=" +
               std::to_string(static_cast<unsigned long>(GetLastError()));
        break;
      }

      const SIZE_T bytes = sizeof(wchar_t) * wtext.size();
      mem = GlobalAlloc(GMEM_MOVEABLE, bytes);
      if (mem == nullptr) {
        *err = "GlobalAlloc failed for clipboard buffer.";
        break;
      }

      void *locked = GlobalLock(mem);
      if (locked == nullptr) {
        *err = "GlobalLock failed for clipboard buffer.";
        break;
      }
      std::memcpy(locked, wtext.data(), bytes);
      GlobalUnlock(mem);

      if (SetClipboardData(CF_UNICODETEXT, mem) == nullptr) {
        *err = "SetClipboardData(CF_UNICODETEXT) failed. GetLastError=" +
               std::to_string(static_cast<unsigned long>(GetLastError()));
        break;
      }

      mem = nullptr; // ownership transferred to system
      ok = true;
    } while (false);

    CloseClipboard();
    if (mem != nullptr) {
      GlobalFree(mem);
    }
    return ok;
  }
};
#else
bool run_clip_cmd(const char *cmd, const std::string &text, std::string *err) {
  FILE *pipe = popen(cmd, "w");
  if (pipe == nullptr) {
    *err = std::string("failed to run clipboard command: ") + cmd;
    return false;
  }
  const size_t wrote = std::fwrite(text.data(), 1, text.size(), pipe);
  const int close_rc = pclose(pipe);
  if (wrote != text.size()) {
    *err = std::string("failed writing clipboard input to command: ") + cmd;
    return false;
  }
  if (close_rc != 0) {
    *err = std::string("clipboard command failed: ") + cmd;
    return false;
  }
  return true;
}

class ClipboardBackendPosix final : public IClipboardBackend {
public:
  bool SetPlainTextUtf8(const std::string &text, std::string *err) override {
#if defined(__APPLE__)
    return run_clip_cmd("pbcopy", text, err);
#elif defined(__linux__)
    std::string ignored;
    if (run_clip_cmd("wl-copy", text, &ignored)) {
      return true;
    }
    if (run_clip_cmd("xclip -selection clipboard", text, &ignored)) {
      return true;
    }
    if (run_clip_cmd("xsel --clipboard --input", text, &ignored)) {
      return true;
    }
    *err = "no supported clipboard command found (tried wl-copy, xclip, xsel).";
    return false;
#else
    (void)text;
    *err = "clipboard backend is not implemented for this OS.";
    return false;
#endif
  }
};
#endif
} // namespace

std::unique_ptr<IClipboardBackend> CreateClipboardBackend(std::string *err) {
#if defined(_WIN32)
  (void)err;
  return std::make_unique<ClipboardBackendWin>();
#elif defined(__APPLE__) || defined(__linux__)
  (void)err;
  return std::make_unique<ClipboardBackendPosix>();
#else
  *err = "clipboard backend is not available on this platform.";
  return nullptr;
#endif
}
