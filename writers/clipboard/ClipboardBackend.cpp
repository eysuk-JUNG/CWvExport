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
    if (needed_wchars <= 0 && !text.empty()) {
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
#include <unistd.h>
#include <sys/wait.h>

bool run_clip_cmd_argv(const std::vector<const char *> &argv, const std::string &text,
                       std::string *err) {
  if (argv.empty() || argv[0] == nullptr) {
    *err = "invalid clipboard command.";
    return false;
  }

  int pipefd[2] = {-1, -1};
  if (pipe(pipefd) != 0) {
    *err = "pipe() failed for clipboard command.";
    return false;
  }

  const pid_t pid = fork();
  if (pid < 0) {
    close(pipefd[0]);
    close(pipefd[1]);
    *err = "fork() failed for clipboard command.";
    return false;
  }

  if (pid == 0) {
    close(pipefd[1]);
    if (dup2(pipefd[0], STDIN_FILENO) < 0) {
      _exit(127);
    }
    close(pipefd[0]);
    execvp(argv[0], const_cast<char *const *>(argv.data()));
    _exit(127);
  }

  close(pipefd[0]);
  size_t offset = 0;
  while (offset < text.size()) {
    const ssize_t written = write(pipefd[1], text.data() + offset, text.size() - offset);
    if (written <= 0) {
      close(pipefd[1]);
      int status = 0;
      waitpid(pid, &status, 0);
      *err = std::string("failed writing clipboard input to command: ") + argv[0];
      return false;
    }
    offset += static_cast<size_t>(written);
  }
  close(pipefd[1]);

  int status = 0;
  if (waitpid(pid, &status, 0) < 0) {
    *err = std::string("waitpid failed for clipboard command: ") + argv[0];
    return false;
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    *err = std::string("clipboard command failed: ") + argv[0];
    return false;
  }
  return true;
}

bool run_clip_cmd(const char *cmd, const std::string &text, std::string *err) {
  if (std::strcmp(cmd, "pbcopy") == 0) {
    std::vector<const char *> argv = {"pbcopy", nullptr};
    return run_clip_cmd_argv(argv, text, err);
  }
  if (std::strcmp(cmd, "wl-copy") == 0) {
    std::vector<const char *> argv = {"wl-copy", nullptr};
    return run_clip_cmd_argv(argv, text, err);
  }
  if (std::strcmp(cmd, "xclip -selection clipboard") == 0) {
    std::vector<const char *> argv = {"xclip", "-selection", "clipboard", nullptr};
    return run_clip_cmd_argv(argv, text, err);
  }
  if (std::strcmp(cmd, "xsel --clipboard --input") == 0) {
    std::vector<const char *> argv = {"xsel", "--clipboard", "--input", nullptr};
    return run_clip_cmd_argv(argv, text, err);
  }
  *err = std::string("unsupported clipboard command pattern: ") + cmd;
  return false;
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
