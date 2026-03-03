/*
 * Copyright (c) WareValley Co., Ltd.
 * All rights reserved.
 *
 * This software ("CWvXlib") and associated documentation files (the "Software")
 * are the proprietary property of WareValley Co., Ltd.
 *
 * You may not use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, in whole or in part, without the express written
 * permission of WareValley Co., Ltd.
 */
/* cxlsx.h - Public API for cxlsx, a lightweight C library for Excel (.xlsx) */
#ifndef CXLSX_H
#define CXLSX_H

#include "version.h"
#include <stddef.h>
#include <stdint.h>

#ifdef _WIN32
#if defined(CXLSX_STATIC)
#define CXLSX_API
#elif defined(CXLSX_EXPORTS)
#define CXLSX_API __declspec(dllexport)
#else
#define CXLSX_API __declspec(dllimport)
#endif
#elif defined(__GNUC__) || defined(__clang__)
#define CXLSX_API __attribute__((visibility("default")))
#else
#define CXLSX_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* License activation - must be called before any other API.
 * Returns 0 on success, -1 on invalid key, -2 on expired, -3 on bad signature.
 * All creation APIs will return NULL/error until a valid license is activated.
 */
CXLSX_API int cxlsx_init(const char *license_key);

/* License status query */
typedef struct {
  const char *customer_id; /* e.g. "TEST-USER", NULL if inactive */
  const char *expiry;      /* e.g. "2099-12-31" or "0000-00-00" (permanent) */
  int active;              /* 1 = active, 0 = inactive */
} cxlsx_license_info;

CXLSX_API int cxlsx_license_active(void);
CXLSX_API int cxlsx_license_get_info(cxlsx_license_info *info);

/* Version */
CXLSX_API const char *cxlsx_version(void);

/* Error codes */
#define CXLSX_OK 0
#define CXLSX_ERROR -1
#define CXLSX_ERROR_MEMORY -2
#define CXLSX_ERROR_FILE -3
#define CXLSX_ERROR_ZIP -4
#define CXLSX_ERROR_PARAM -5
#define CXLSX_ERROR_LICENSE -6
#define CXLSX_ERROR_PASSWORD -7
#define CXLSX_ERROR_LIMIT -8

/* Warning codes (positive, non-fatal) */
#define CXLSX_WARN_TRUNCATED 1 /* string was truncated to Excel limit */

/* Convert error code to human-readable string */
CXLSX_API const char *cxlsx_strerror(int err);

/* Horizontal alignment */
#define CXLSX_ALIGN_NONE 0
#define CXLSX_ALIGN_LEFT 1
#define CXLSX_ALIGN_CENTER 2
#define CXLSX_ALIGN_RIGHT 3
#define CXLSX_ALIGN_FILL 4
#define CXLSX_ALIGN_JUSTIFY 5
#define CXLSX_ALIGN_CENTER_ACROSS 6

/* Vertical alignment */
#define CXLSX_VALIGN_NONE 0
#define CXLSX_VALIGN_TOP 1
#define CXLSX_VALIGN_CENTER 2
#define CXLSX_VALIGN_BOTTOM 3

/* Border styles */
#define CXLSX_BORDER_NONE 0
#define CXLSX_BORDER_THIN 1
#define CXLSX_BORDER_MEDIUM 2
#define CXLSX_BORDER_THICK 3
#define CXLSX_BORDER_DOUBLE 4
#define CXLSX_BORDER_DASHED 5
#define CXLSX_BORDER_DOTTED 6

/* Predefined colors (RGB) */
#define CXLSX_COLOR_BLACK 0x000000
#define CXLSX_COLOR_WHITE 0xFFFFFF
#define CXLSX_COLOR_RED 0xFF0000
#define CXLSX_COLOR_GREEN 0x00FF00
#define CXLSX_COLOR_BLUE 0x0000FF
#define CXLSX_COLOR_YELLOW 0xFFFF00
#define CXLSX_COLOR_GRAY 0x808080

/* --- Workbook API --- */
typedef struct cxlsx_workbook cxlsx_workbook;
typedef struct cxlsx_worksheet cxlsx_worksheet;
typedef struct cxlsx_format cxlsx_format;

typedef struct cxlsx_datetime {
  int year;
  int month;
  int day;
  int hour;
  int min;
  double sec;
} cxlsx_datetime;

CXLSX_API cxlsx_workbook *cxlsx_workbook_new(const char *filename);

CXLSX_API cxlsx_worksheet *cxlsx_workbook_add_worksheet(cxlsx_workbook *wb,
                                                        const char *name);

CXLSX_API cxlsx_format *cxlsx_workbook_add_format(cxlsx_workbook *wb);

CXLSX_API int cxlsx_workbook_close(cxlsx_workbook *wb);

/* Set file encryption password (must be called before close) */
CXLSX_API void cxlsx_workbook_set_password(cxlsx_workbook *wb,
                                           const char *password);

/* Protect workbook structure (prevent adding/removing/renaming sheets) */
CXLSX_API void cxlsx_workbook_protect(cxlsx_workbook *wb, const char *password);

/* Unprotect workbook structure; returns CXLSX_OK if password matches */
CXLSX_API int cxlsx_workbook_unprotect(cxlsx_workbook *wb,
                                       const char *password);

/* Document properties (written to docProps/core.xml) */
typedef struct cxlsx_doc_properties {
  const char *title;    /* dc:title */
  const char *subject;  /* dc:subject */
  const char *author;   /* dc:creator (default: "cxlsx") */
  const char *keywords; /* cp:keywords */
  const char *category; /* cp:category */
  const char *comments; /* dc:description */
} cxlsx_doc_properties;

/* Set document metadata; NULL fields are ignored */
CXLSX_API void cxlsx_workbook_set_properties(cxlsx_workbook *wb,
                                             const cxlsx_doc_properties *props);

CXLSX_API int cxlsx_worksheet_write_string(cxlsx_worksheet *ws, int row,
                                           int col, const char *value,
                                           cxlsx_format *fmt);

CXLSX_API int cxlsx_worksheet_write_number(cxlsx_worksheet *ws, int row,
                                           int col, double value,
                                           cxlsx_format *fmt);

CXLSX_API int cxlsx_worksheet_write_formula(cxlsx_worksheet *ws, int row,
                                            int col, const char *formula,
                                            cxlsx_format *fmt);

CXLSX_API int cxlsx_worksheet_write_datetime(cxlsx_worksheet *ws, int row,
                                             int col, const cxlsx_datetime *dt,
                                             cxlsx_format *fmt);

CXLSX_API int cxlsx_worksheet_write_blank(cxlsx_worksheet *ws, int row, int col,
                                          cxlsx_format *fmt);

CXLSX_API int cxlsx_worksheet_merge_range(cxlsx_worksheet *ws, int first_row,
                                          int first_col, int last_row,
                                          int last_col, const char *value,
                                          cxlsx_format *fmt);

CXLSX_API void cxlsx_worksheet_set_column_width(cxlsx_worksheet *ws,
                                                int first_col, int last_col,
                                                double width);

CXLSX_API void cxlsx_worksheet_set_row_height(cxlsx_worksheet *ws, int row,
                                              double height);

CXLSX_API void cxlsx_worksheet_freeze_panes(cxlsx_worksheet *ws, int row,
                                            int col);

/* Set sheet tab color (ARGB, e.g. CXLSX_COLOR_BLUE or 0xFF4472C4) */
CXLSX_API void cxlsx_worksheet_set_tab_color(cxlsx_worksheet *ws,
                                             uint32_t argb_color);

/* Hide columns from first_col to last_col (0-based) */
CXLSX_API void cxlsx_worksheet_set_column_hidden(cxlsx_worksheet *ws,
                                                 int first_col, int last_col);

/* Hide a single row (0-based) */
CXLSX_API void cxlsx_worksheet_set_row_hidden(cxlsx_worksheet *ws, int row);

/* Set autofilter dropdown on a cell range (0-based row/col).
 * Typically the range covers the header row through the last data row. */
CXLSX_API void cxlsx_worksheet_autofilter(cxlsx_worksheet *ws, int first_row,
                                          int first_col, int last_row,
                                          int last_col);

CXLSX_API int cxlsx_worksheet_insert_image(cxlsx_worksheet *ws, int row,
                                           int col, const char *filename);

/* Write a string with a hyperlink URL to cell (row, col).
 * 'text' is the display text; if NULL, the URL itself is used.
 * 'url' is the link target (http://, https://, mailto:, etc.). */
CXLSX_API int cxlsx_worksheet_write_url(cxlsx_worksheet *ws, int row, int col,
                                        const char *url, const char *text,
                                        cxlsx_format *fmt);

/* Protect sheet (prevent editing locked cells) */
CXLSX_API void cxlsx_worksheet_protect(cxlsx_worksheet *ws,
                                       const char *password);

/* Unprotect sheet; returns CXLSX_OK if password matches */
CXLSX_API int cxlsx_worksheet_unprotect(cxlsx_worksheet *ws,
                                        const char *password);

/* Set header/footer strings for printing.
 * Use Excel format codes: &L (left), &C (center), &R (right),
 * &P (page number), &N (total pages), &D (date), &T (time),
 * &F (filename), &A (sheet name). */
CXLSX_API void cxlsx_worksheet_set_header(cxlsx_worksheet *ws,
                                          const char *header);
CXLSX_API void cxlsx_worksheet_set_footer(cxlsx_worksheet *ws,
                                          const char *footer);

typedef enum {
  CXLSX_COL_STRING = 0,
  CXLSX_COL_NUMBER = 1,
  CXLSX_COL_DATE = 2
} cxlsx_col_type;

typedef struct {
  const char *name;
  cxlsx_col_type type;
  double width;
} cxlsx_col_def;

typedef struct {
  cxlsx_col_type type;
  union {
    const char *str;
    double number;
    double date;
  } v;
} cxlsx_cell_value;

typedef struct cxlsx_stream cxlsx_stream;

CXLSX_API cxlsx_stream *cxlsx_stream_new(const char *filename,
                                         int rows_per_sheet);

CXLSX_API int cxlsx_stream_set_columns(cxlsx_stream *s,
                                       const cxlsx_col_def *cols,
                                       int col_count);

CXLSX_API int cxlsx_stream_write_rows(cxlsx_stream *s,
                                      const cxlsx_cell_value *values,
                                      int row_count);

CXLSX_API int cxlsx_stream_close(cxlsx_stream *s);

/* Set file encryption password for streaming writer (call before close) */
CXLSX_API void cxlsx_stream_set_password(cxlsx_stream *s, const char *password);

/* Set custom sheet name prefix for streaming writer.
 * Sheets are named as "prefix1", "prefix2", etc.
 * Default prefix is "Sheet" (→ Sheet1, Sheet2, ...).
 * Must be called before writing any rows. */
CXLSX_API void cxlsx_stream_set_sheet_name(cxlsx_stream *s,
                                           const char *name_prefix);

/* Set header/footer strings for all sheets in streaming writer.
 * Same format codes as cxlsx_worksheet_set_header/footer. */
CXLSX_API void cxlsx_stream_set_header(cxlsx_stream *s, const char *header);
CXLSX_API void cxlsx_stream_set_footer(cxlsx_stream *s, const char *footer);

/* --- Format API --- */
CXLSX_API void cxlsx_format_set_bold(cxlsx_format *fmt);
CXLSX_API void cxlsx_format_set_italic(cxlsx_format *fmt);
CXLSX_API void cxlsx_format_set_underline(cxlsx_format *fmt);
CXLSX_API void cxlsx_format_set_font_size(cxlsx_format *fmt, double size);
CXLSX_API void cxlsx_format_set_font_name(cxlsx_format *fmt, const char *name);
CXLSX_API void cxlsx_format_set_font_color(cxlsx_format *fmt, uint32_t color);
CXLSX_API void cxlsx_format_set_bg_color(cxlsx_format *fmt, uint32_t color);
CXLSX_API void cxlsx_format_set_fg_color(cxlsx_format *fmt, uint32_t color);
CXLSX_API void cxlsx_format_set_num_format(cxlsx_format *fmt,
                                           const char *num_fmt);
CXLSX_API void cxlsx_format_set_border(cxlsx_format *fmt, int style);
CXLSX_API void cxlsx_format_set_align(cxlsx_format *fmt, int align);
CXLSX_API void cxlsx_format_set_valign(cxlsx_format *fmt, int valign);
CXLSX_API void cxlsx_format_set_text_wrap(cxlsx_format *fmt);
CXLSX_API void cxlsx_format_set_locked(cxlsx_format *fmt, int locked);
CXLSX_API void cxlsx_format_set_hidden(cxlsx_format *fmt, int hidden);

/* --- Reader API --- */
typedef enum {
  CXLSX_CELL_NONE = 0,
  CXLSX_CELL_STRING,
  CXLSX_CELL_NUMBER,
  CXLSX_CELL_FORMULA,
  CXLSX_CELL_DATETIME,
  CXLSX_CELL_BLANK
} cxlsx_cell_type;

typedef struct cxlsx_reader cxlsx_reader;

typedef struct cxlsx_read_cell {
  int row;
  int col;
  cxlsx_cell_type type;
  const char *str_val;
  double num_val;
} cxlsx_read_cell;

/* Open an XLSX file. Pass password for encrypted files, or NULL/"" for normal.

 * Auto-detects CFB (encrypted) vs ZIP (normal) format. */

CXLSX_API cxlsx_reader *cxlsx_reader_open(const char *filename,

                                          const char *password);

CXLSX_API int cxlsx_reader_sheet_count(cxlsx_reader *r);

CXLSX_API const char *cxlsx_reader_sheet_name(cxlsx_reader *r, int index);

CXLSX_API int cxlsx_reader_open_sheet(cxlsx_reader *r, int index);

CXLSX_API int cxlsx_reader_row_count(cxlsx_reader *r);

CXLSX_API int cxlsx_reader_col_count(cxlsx_reader *r);

CXLSX_API int cxlsx_reader_get_cell(cxlsx_reader *r, int row, int col,
                                    cxlsx_read_cell *cell);

/* Cell format info returned by cxlsx_reader_get_cell_format */
typedef struct cxlsx_cell_format {
  int bold;
  int italic;
  int underline;
  double font_size;    /* 0 = default */
  char font_name[64];  /* "" = default */
  uint32_t font_color; /* 0 = default (black) */
  uint32_t bg_color;   /* 0 = no fill */
  char num_format[64]; /* "" = General */
  int border_style;    /* CXLSX_BORDER_* */
  int align;           /* CXLSX_ALIGN_* */
  int valign;          /* CXLSX_VALIGN_* */
  int text_wrap;
  int locked; /* -1 = default */
  int hidden;
} cxlsx_cell_format;

/* Get cell format info. Returns CXLSX_OK on success.
 * Fields default to 0/"" for unstyled cells. */
CXLSX_API int cxlsx_reader_get_cell_format(cxlsx_reader *r, int row, int col,
                                           cxlsx_cell_format *fmt);

CXLSX_API void cxlsx_reader_close(cxlsx_reader *r);

#define CXLSX_SREADER_EOF 1

typedef struct cxlsx_sreader cxlsx_sreader;

CXLSX_API cxlsx_sreader *cxlsx_sreader_open(const char *filename);

CXLSX_API int cxlsx_sreader_next_row(cxlsx_sreader *sr);

CXLSX_API const cxlsx_read_cell *cxlsx_sreader_get_row(cxlsx_sreader *sr,
                                                       int *n);

CXLSX_API long long cxlsx_sreader_row_index(cxlsx_sreader *sr);

CXLSX_API int cxlsx_sreader_sheet_index(cxlsx_sreader *sr);

CXLSX_API const char *cxlsx_sreader_sheet_name(cxlsx_sreader *sr);

CXLSX_API int cxlsx_sreader_sheet_count(cxlsx_sreader *sr);

/* Get last streaming-reader error code.
 * - If sr != NULL: returns current reader instance error (or CXLSX_OK).
 * - If sr == NULL: returns last open-time error from cxlsx_sreader_open(). */
CXLSX_API int cxlsx_sreader_last_error(cxlsx_sreader *sr);

CXLSX_API void cxlsx_sreader_close(cxlsx_sreader *sr);

/* --- Win32 wide-char API variants --- */
#ifdef _WIN32
#include <wchar.h>

CXLSX_API cxlsx_workbook *cxlsx_workbook_new_w(const wchar_t *filename);
CXLSX_API cxlsx_worksheet *cxlsx_workbook_add_worksheet_w(cxlsx_workbook *wb,
                                                          const wchar_t *name);
CXLSX_API int cxlsx_worksheet_write_string_w(cxlsx_worksheet *ws, int row,
                                             int col, const wchar_t *value,
                                             cxlsx_format *fmt);
CXLSX_API int cxlsx_worksheet_write_formula_w(cxlsx_worksheet *ws, int row,
                                              int col, const wchar_t *formula,
                                              cxlsx_format *fmt);
CXLSX_API int cxlsx_worksheet_merge_range_w(cxlsx_worksheet *ws, int first_row,
                                            int first_col, int last_row,
                                            int last_col, const wchar_t *value,
                                            cxlsx_format *fmt);
CXLSX_API int cxlsx_worksheet_insert_image_w(cxlsx_worksheet *ws, int row,
                                             int col, const wchar_t *filename);
CXLSX_API int cxlsx_worksheet_write_url_w(cxlsx_worksheet *ws, int row, int col,
                                          const wchar_t *url,
                                          const wchar_t *text,
                                          cxlsx_format *fmt);
CXLSX_API void cxlsx_format_set_font_name_w(cxlsx_format *fmt,
                                            const wchar_t *name);
CXLSX_API void cxlsx_format_set_num_format_w(cxlsx_format *fmt,
                                             const wchar_t *num_fmt);

CXLSX_API cxlsx_stream *cxlsx_stream_new_w(const wchar_t *filename,
                                           int rows_per_sheet);

CXLSX_API cxlsx_reader *cxlsx_reader_open_w(const wchar_t *filename,

                                            const wchar_t *password);

CXLSX_API cxlsx_sreader *cxlsx_sreader_open_w(const wchar_t *filename);

/* Auto-redirect to wide-char variants when UNICODE is defined */
#if defined(UNICODE) && !defined(CXLSX_EXPORTS) && !defined(CXLSX_STATIC)
#define cxlsx_workbook_new cxlsx_workbook_new_w
#define cxlsx_workbook_add_worksheet cxlsx_workbook_add_worksheet_w
#define cxlsx_worksheet_write_string cxlsx_worksheet_write_string_w
#define cxlsx_worksheet_write_formula cxlsx_worksheet_write_formula_w
#define cxlsx_worksheet_merge_range cxlsx_worksheet_merge_range_w
#define cxlsx_worksheet_insert_image cxlsx_worksheet_insert_image_w
#define cxlsx_worksheet_write_url cxlsx_worksheet_write_url_w
#define cxlsx_format_set_font_name cxlsx_format_set_font_name_w
#define cxlsx_format_set_num_format cxlsx_format_set_num_format_w
#define cxlsx_stream_new cxlsx_stream_new_w
#define cxlsx_reader_open cxlsx_reader_open_w
#define cxlsx_sreader_open cxlsx_sreader_open_w
#endif

#endif

#ifdef __cplusplus
}
#endif

#endif /* CXLSX_H */
