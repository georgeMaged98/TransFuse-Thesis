#define FUSE_USE_VERSION 31

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <assert.h>
#include <map>
#include <string>

/*
 * Command line options
 *
 * We can't set default values for the char* fields here because
 * fuse_opt_parse would attempt to free() them when the user specifies
 * different values on the command line.
 */
static struct options {
    const char *filename;
    const char *contents;
    int show_help;
    std::map<std::string, std::string> files; // Map for storing file contents
} options;

#define OPTION(t, p)                           \
    { t, offsetof(struct options, p), 1 }
static const struct fuse_opt option_spec[] = {
    OPTION("--name=%s", filename),
    OPTION("--contents=%s", contents),
    OPTION("-h", show_help),
    OPTION("--help", show_help),
    FUSE_OPT_END
};

static void *hello_init(struct fuse_conn_info *conn,
                        struct fuse_config *cfg) {
    (void) conn;
    cfg->kernel_cache = 1;
    return NULL;
}

static int hello_getattr(const char *path, struct stat *stbuf,
                         struct fuse_file_info *fi) {
    (void) fi;
    int res = 0;

    memset(stbuf, 0, sizeof(struct stat));
    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
    } else if (strcmp(path + 1, options.filename) == 0) {
        stbuf->st_mode = S_IFREG | 0444;
        stbuf->st_nlink = 1;
        stbuf->st_size = strlen(options.contents);
    } else if (options.files.find(path + 1) != options.files.end()) {
        stbuf->st_mode = S_IFREG | 0666; // Read/write permissions for new files
        stbuf->st_nlink = 1;
        stbuf->st_size = options.files[path + 1].size();
    } else {
        res = -ENOENT;
    }

    return res;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi,
                         enum fuse_readdir_flags flags) {
    (void) offset;
    (void) fi;
    (void) flags;

    if (strcmp(path, "/") != 0)
        return -ENOENT;

    filler(buf, ".", NULL, 0, static_cast<fuse_fill_dir_flags>(0));
    filler(buf, "..", NULL, 0, static_cast<fuse_fill_dir_flags>(0));
    filler(buf, options.filename, NULL, 0, static_cast<fuse_fill_dir_flags>(0));
    for (const auto &file : options.files) {
        filler(buf, file.first.c_str(), NULL, 0, static_cast<fuse_fill_dir_flags>(0));
    }

    return 0;
}

static int hello_open(const char *path, struct fuse_file_info *fi) {
    if (strcmp(path + 1, options.filename) != 0 && options.files.find(path + 1) == options.files.end())
        return -ENOENT;

    if ((fi->flags & O_ACCMODE) != O_RDONLY && options.files.find(path + 1) == options.files.end())
        return -EACCES;

    return 0;
}

static int hello_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi) {
    size_t len;
    (void) fi;
    if (strcmp(path + 1, options.filename) == 0) {
        len = strlen(options.contents);
        if (offset < len) {
            if (offset + size > len)
                size = len - offset;
            memcpy(buf, options.contents + offset, size);
        } else
            size = 0;
        return size;
    } else if (options.files.find(path + 1) != options.files.end()) {
        const std::string &content = options.files[path + 1];
        len = content.size();
        if (offset < len) {
            if (offset + size > len)
                size = len - offset;
            memcpy(buf, content.c_str() + offset, size);
        } else
            size = 0;
        return size;
    }
    return -ENOENT;
}

static int hello_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    (void) mode; // Mode is not used in this simple implementation
    options.files[path + 1] = ""; // Create an empty file with the given path
    fi->fh = 0; // Use file handle 0 for simplicity
    return 0;
}

static int hello_utimens(const char *path, const struct timespec ts[2], struct fuse_file_info *fi) {
    (void) fi;
    (void) ts;

    if (options.files.find(path + 1) == options.files.end() && strcmp(path + 1, options.filename) != 0) {
        return -ENOENT;
    }

    // Normally, you would update the timestamp here. In this example, we'll just return success.
    return 0;
}

static const struct fuse_operations hello_oper = {
    .getattr = hello_getattr,
    .open = hello_open,
    .read = hello_read,
    .readdir = hello_readdir,
    .init = hello_init,
    .create = hello_create,
    .utimens = hello_utimens,
};

static void show_help(const char *progname) {
    printf("usage: %s [options] <mountpoint>\n\n", progname);
    printf("File-system specific options:\n"
        "    --name=<s>          Name of the \"hello\" file\n"
        "                        (default: \"hello\")\n"
        "    --contents=<s>      Contents \"hello\" file\n"
        "                        (default \"Hello, World!\\n\")\n"
        "\n");
}

// int main(int argc, char *argv[]) {
//     int ret;
//     struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
//
//     /* Set defaults -- we have to use strdup so that
//        fuse_opt_parse can free the defaults if other
//        values are specified */
//     options.filename = strdup("hello");
//     options.contents = strdup("Hello World!\n");
//
//     /* Parse options */
//     if (fuse_opt_parse(&args, &options, option_spec, NULL) == -1)
//         return 1;
//
//     /* When --help is specified, first print our own file-system
//        specific help text, then signal fuse_main to show
//        additional help (by adding `--help` to the options again)
//        without usage: line (by setting argv[0] to the empty
//        string) */
//     if (options.show_help) {
//         show_help(argv[0]);
//         assert(fuse_opt_add_arg(&args, "--help") == 0);
//         args.argv[0][0] = '\0';
//     }
//
//     ret = fuse_main(args.argc, args.argv, &hello_oper, NULL);
//     fuse_opt_free_args(&args);
//     return ret;
// }
