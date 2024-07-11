#define FUSE_USE_VERSION 31

#include <fuse3/fuse.h>
#include <fuse3/fuse_opt.h>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>


// static fuse_fill_dir_flags fill_dir_plus = 0;

/*
 *  Operation callback functions
 */

static void *transfuse_init(struct fuse_conn_info *conn, struct fuse_config *cfg) {
    (void) conn;
    cfg->use_ino = 1;

    /* parallel_direct_writes feature depends on direct_io features.
       To make parallel_direct_writes valid, need either set cfg->direct_io
       in current function (recommended in high level API) or set fi->direct_io
       in xmp_create() or xmp_open(). */
    // cfg->direct_io = 1;
    cfg->parallel_direct_writes = 1;

    /* Pick up changes from lower filesystem right away. This is
       also necessary for better hardlink support. When the kernel
       calls the unlink() handler, it does not know the inode of
       the to-be-removed entry and can therefore not invalidate
       the cache of the associated inode - resulting in an
       incorrect st_nlink value being reported for any remaining
       hardlinks to this inode. */
    cfg->entry_timeout = 0;
    cfg->attr_timeout = 0;
    cfg->negative_timeout = 0;

    return NULL;
}

static int transfuse_access(const char *path, int mask) {
    printf("--> transfuse_access() has been invoked and the path is: %s\n", path);

    int res;

    res = access(path, mask);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_open(const char *path, fuse_file_info *fi) {
    printf("--> transfuse_open() and the path is: %s\n", path);
    int res;

    res = open(path, fi->flags);
    if (res == -1)
        return -errno;

    /* Enable direct_io when open has flags O_DIRECT to enjoy the feature
    parallel_direct_writes (i.e., to get a shared lock, not exclusive lock,
    for writes to the same file). */
    if (fi->flags & O_DIRECT) {
        fi->direct_io = 1;
        fi->parallel_direct_writes = 1;
    }

    fi->fh = res;
    return 0;
}

static int transfuse_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi) {
    // printf(" --> transfuse_getattr() is invoked  and path is: %s; \n", path);
    (void) fi;
    int res;

    res = lstat(path, stbuf);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                             const off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
    DIR *dp;
    struct dirent *de;

    (void) offset;
    (void) fi;
    (void) flags;

    dp = opendir(path);
    if (dp == NULL)
        return -errno;

    while ((de = readdir(dp)) != NULL) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = de->d_ino;
        st.st_mode = de->d_type << 12;
        // if (filler(buf, de->d_name, &st, 0, fill_dir_plus))
        //     break;
    }

    closedir(dp);
    return 0;
}

static int transfuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    printf(" --> transfuse_read() and the read path is %s\n", path);
    int fd;
    int res;

    if (fi == NULL)
        fd = open(path, O_RDONLY);
    else
        fd = fi->fh;

    if (fd == -1)
        return -errno;

    res = pread(fd, buf, size, offset);
    if (res == -1)
        res = -errno;

    if (fi == NULL)
        close(fd);
    return res;
}

static int transfuse_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    printf(" --> transfuse_write() and the write path is  %s\n", path);
    int fd;
    int res;

    (void) fi;
    if (fi == NULL)
        fd = open(path, O_WRONLY);
    else
        fd = fi->fh;

    if (fd == -1)
        return -errno;

    res = pwrite(fd, buf, size, offset);
    if (res == -1)
        res = -errno;

    if (fi == NULL)
        close(fd);
    return res;
}

static int transfuse_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    printf(" --> transfuse_create() is invoked and path is: %s with mode: %o and flags: %x\n", path, mode, fi->flags);

    int res;
    path = "./testfile";
    // Create the file with the specified mode
    res = open(path, fi->flags, mode);
    if (res == -1) {
        fprintf(stderr, "Error creating file %s: %s\n", path, strerror(errno));
        return -errno;
    }
    // Set the file descriptor for the file
    fi->fh = res;

    // Change the ownership of the file to the current user
    uid_t uid = getuid();
    gid_t gid = getgid();

    printf("Setting ownership to UID: %d, GID: %d\n", uid, gid);

    if (fchown(res, uid, gid) == -1) {
        close(res);
        return -errno;
    }

    return 0;
}

static int transfuse_chmod(const char *path, mode_t mode,
                           struct fuse_file_info *fi) {
    printf(" --> transfuse_chmod() and path is: %s: ", path);
    (void) fi;
    int res;

    printf("test 1");
    res = chmod(path, mode);
    if (res == -1) {
        printf("test error");
        return -errno;
    }
    printf("test 2");
    return 0;
}

static int transfuse_chown(const char *path, uid_t uid, gid_t gid,
                           struct fuse_file_info *fi) {
    (void) fi;
    int res;

    res = lchown(path, uid, gid);
    if (res == -1)
        return -errno;

    return 0;
}

static constexpr struct fuse_operations transfuse_oper = {
    .getattr = transfuse_getattr,
    .chmod = transfuse_chmod,
    .chown = transfuse_chown,
    .open = transfuse_open,
    .read = transfuse_read,
    .write = transfuse_write,
    .readdir = transfuse_readdir,
    .init = transfuse_init,
    .access = transfuse_access,
    .create = transfuse_create
};

static void show_help(const char *progname) {
    printf("usage: %s [options]\n\n", progname);
    printf("options:\n"
        "      --version\n"
        "      --help\n"
        "\n");
}

// int main(int argc, char *argv[])
// {
//     enum { MAX_ARGS = 10 };
//     int i, new_argc;
//     char *new_argv[MAX_ARGS];
//     struct fuse_args args = FUSE_ARGS_INIT(0, NULL);
//
//     umask(0);
//
//     /* Process the "--plus" option apart */
//     for (i = 0, new_argc = 0; (i < argc) && (new_argc < MAX_ARGS); i++) {
//         if (strcmp(argv[i], "--plus") == 0) {
//             // fill_dir_plus = FUSE_FILL_DIR_PLUS;
//         } else {
//             new_argv[new_argc++] = argv[i];
//         }
//     }
//
//     // Initialize fuse_args with the new arguments
//     fuse_opt_add_arg(&args, new_argv[0]);  // Add program name first
//     for (i = 1; i < new_argc; i++) {
//         fuse_opt_add_arg(&args, new_argv[i]);
//     }
//
//     // Add necessary mount options
//     fuse_opt_add_arg(&args, "-o");
//     fuse_opt_add_arg(&args, "allow_other,default_permissions");
//
//     // Add uid and gid options to ensure correct ownership
//     char uid_option[20];
//     char gid_option[20];
//     snprintf(uid_option, sizeof(uid_option), "uid=%d", getuid());
//     snprintf(gid_option, sizeof(gid_option), "gid=%d", getgid());
//     fuse_opt_add_arg(&args, "-o");
//     fuse_opt_add_arg(&args, uid_option);
//     fuse_opt_add_arg(&args, "-o");
//     fuse_opt_add_arg(&args, gid_option);
//
//     // Run the FUSE main loop with the modified arguments
//     int ret = fuse_main(args.argc, args.argv, &transfuse_oper, NULL);
//
//     // Free the allocated arguments
//     fuse_opt_free_args(&args);
//
//     return ret;
// }