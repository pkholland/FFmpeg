/*
 * AWS S3 protocol
 *
 * Copyright (c) 2023 Paul Holland
 *
 * based on libavformat/http.c, Copyright (c) 2000, 2001 Fabrice Bellard
 *
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

#include "config.h"
#include "config_components.h"

#include "libavutil/avstring.h"
#include "libavutil/opt.h"
#include "avformat.h"
#include "url.h"
#include "s3_proto.h"
#include <fcntl.h>

typedef struct S3Context {
    const AVClass *class;
    s3_proto *s3;
    int seekable;           /**< Control seekability, 0 = disable, 1 = enable, -1 = probe. */
} S3Context;

#define OFFSET(x) offsetof(S3Context, x)
#define D AV_OPT_FLAG_DECODING_PARAM
#define E AV_OPT_FLAG_ENCODING_PARAM

static const AVOption s3_options[] = {
    { "seekable", "control seekability of connection", OFFSET(seekable), AV_OPT_TYPE_BOOL, { .i64 = -1 }, -1, 1, D },
    { NULL }
};

static int s3_open(URLContext *h, const char *uri, int flags)
{
    int access;

    S3Context* ctx = h->priv_data;
    if (flags & AVIO_FLAG_WRITE && flags & AVIO_FLAG_READ) {
        access = O_CREAT | O_RDWR;
    } else if (flags & AVIO_FLAG_WRITE) {
        access = O_CREAT | O_WRONLY;
    } else {
        access = O_RDONLY;
    }
    ctx->s3 = s3_proto_new(uri, access);
    if (ctx->s3 == 0) {
        if (flags & AVIO_FLAG_READ) {
            return AVERROR(ENOENT);
        }
        else {
            return AVERROR(EACCES);
        }
    }
    return 0;
}

static int s3_read(URLContext *h, uint8_t *buf, int size)
{
    S3Context* ctx = h->priv_data;
    return s3_proto_read(ctx->s3, buf, size);
}

static int s3_write(URLContext *h, const uint8_t *buf, int size)
{
    S3Context* ctx = h->priv_data;
    return s3_proto_write(ctx->s3, buf, size);
}

static int64_t s3_seek(URLContext *h, int64_t pos, int whence)
{
    S3Context* ctx = h->priv_data;
    int64_t ret;

    if (whence == AVSEEK_SIZE) {
        ret = s3_proto_size(ctx->s3);
    }
    else {
        ret = s3_proto_seek(ctx->s3, pos, whence);
    }
    return ret < 0 ? AVERROR(errno) : ret;
}

static int s3_close(URLContext *h)
{
    S3Context* ctx = h->priv_data;
    return s3_proto_close_and_delete(ctx->s3);
}

static const AVClass s3_class = {
    .class_name = "s3",
    .item_name  = av_default_item_name,
    .option     = s3_options,
    .version    = LIBAVUTIL_VERSION_INT,
};

const URLProtocol ff_s3_protocol = {
    .name              = "s3",
    .url_open          = s3_open,
    .url_read          = s3_read,
    .url_write         = s3_write,
    .url_seek          = s3_seek,
    .url_close         = s3_close,
    .priv_data_size    = sizeof(S3Context),
    .priv_data_class   = &s3_class,
    .flags             = URL_PROTOCOL_FLAG_NETWORK,
    .default_whitelist = "file,crypto,data"
};
