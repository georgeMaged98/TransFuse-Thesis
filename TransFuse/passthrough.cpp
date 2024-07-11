//
// Created by george-elfayoumi on 6/25/24.
//
/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  Copyright (C) 2011       Sebastian Pipping <sebastian@pipping.org>

  This program can be distributed under the terms of the GNU GPLv2.
  See the file COPYING.
*/

/** @file
 *
 * This file system mirrors the existing file system hierarchy of the
 * system, starting at the root file system. This is implemented by
 * just "passing through" all requests to the corresponding user-space
 * libc functions. Its performance is terrible.
 *
 * Compile with
 *
 *     gcc -Wall passthrough.c `pkg-config fuse3 --cflags --libs` -o passthrough
 *
 * ## Source code ##
 * \include passthrough.c
 */


#define FUSE_USE_VERSION 31

#define _GNU_SOURCE

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif

#include <fuse.h>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <cerrno>
#include <iostream>

#ifdef __FreeBSD__
#include <sys/socket.h>
#include <sys/un.h>
#endif
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include "passthrough_helpers.h"

fuse_fill_dir_flags fill_dir_plus = FUSE_FILL_DIR_PLUS;

static const char *fuse_root_dir = "/media/george-elfayoumi/Academic/transfuse_fs";


using namespace std;

static void *transfuse_init(struct fuse_conn_info *conn,
                            struct fuse_config *cfg) {
    (void) conn;
    cfg->use_ino = 1;

    /* parallel_direct_writes feature depends on direct_io features.
       To make parallel_direct_writes valid, need either set cfg->direct_io
       in current function (recommended in high level API) or set fi->direct_io
       in transfuse_create() or transfuse_open(). */
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
    printf(" --> transfuse_init() \n");

    return NULL;
}

// fuse_operations::getattr: This callback retrieves file attributes,
// which can be relevant when memory mapping a file to determine its size and permissions.
static int transfuse_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi) {
    (void) fi; // Unused parameter
    int res;
    char fpath[PATH_MAX];

    // Construct the full path by combining the FUSE root directory with the relative path
    snprintf(fpath, sizeof(fpath), "%s%s", fuse_root_dir, path);

    // Log the constructed path for debugging
    printf(" --> transfuse_getattr() path: %s -> fpath: %s\n", path, fpath);

    // Get file attributes using lstat (or stat if you don't need symbolic link information)
    res = lstat(fpath, stbuf);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_access(const char *path, int mask) {
    int res;
    printf(" --> transfuse_access() and path is %s: \n ", path);

    res = access(path, mask);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_readlink(const char *path, char *buf, size_t size) {
    int res;

    res = readlink(path, buf, size - 1);
    if (res == -1)
        return -errno;

    buf[res] = '\0';
    return 0;
}


static int transfuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                             off_t offset, struct fuse_file_info *fi,
                             enum fuse_readdir_flags flags) {
    DIR *dp;
    struct dirent *de;
    char fpath[PATH_MAX];

    (void) offset;
    (void) fi;
    (void) flags;

    // Construct the full path by combining the FUSE root directory with the relative path
    snprintf(fpath, sizeof(fpath), "%s%s", fuse_root_dir, path);

    // Log the constructed path for debugging
    printf("--> transfuse_readdir() and  path: %s -> fpath: %s\n", path, fpath);

    dp = opendir(fpath);
    if (dp == NULL)
        return -errno;

    while ((de = readdir(dp)) != NULL) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = de->d_ino;
        st.st_mode = de->d_type << 12;
        if (filler(buf, de->d_name, &st, 0, static_cast<fuse_fill_dir_flags>(0)))
            break;
    }

    closedir(dp);
    return 0;
}

static int transfuse_mknod(const char *path, mode_t mode, dev_t rdev) {
    int res;
    char fpath[PATH_MAX];
    snprintf(fpath, sizeof(fpath), "%s%s", fuse_root_dir, path);

    // Log the constructed path for debugging
    printf(" --> transfuse_mknod() path: %s -> fpath: %s\n", path, fpath);

    res = mknod_wrapper(AT_FDCWD, fpath, NULL, mode, rdev);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_mkdir(const char *path, mode_t mode) {
    int res;

    res = mkdir(path, mode);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_unlink(const char *path) {
    int res;
    char fpath[PATH_MAX];

    // Construct the full path by combining the FUSE root directory with the relative path
    snprintf(fpath, sizeof(fpath), "%s%s", fuse_root_dir, path);

    // Log the constructed path for debugging
    printf(" --> transfuse_unlink() and path is %s -> fpath: %s\n", path, fpath);

    res = unlink(fpath);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_rmdir(const char *path) {
    int res;

    res = rmdir(path);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_symlink(const char *from, const char *to) {
    int res;

    res = symlink(from, to);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_rename(const char *from, const char *to, unsigned int flags) {
    int res;

    if (flags)
        return -EINVAL;

    res = rename(from, to);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_link(const char *from, const char *to) {
    int res;

    res = link(from, to);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_chmod(const char *path, mode_t mode,
                           struct fuse_file_info *fi) {
    (void) fi;
    int res;
    char fpath[PATH_MAX];

    // Construct the full path by combining the FUSE root directory with the relative path
    snprintf(fpath, sizeof(fpath), "%s%s", fuse_root_dir, path);

    // Log the constructed path for debugging
    printf(" --> transfuse_chmod() path: %s -> fpath: %s\n", path, fpath);

    res = chmod(fpath, mode);

    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_chown(const char *path, uid_t uid, gid_t gid,
                           struct fuse_file_info *fi) {
    (void) fi;
    int res;
    char fpath[PATH_MAX];

    // Construct the full path by combining the FUSE root directory with the relative path
    snprintf(fpath, sizeof(fpath), "%s%s", fuse_root_dir, path);

    // Log the constructed path for debugging
    printf(" --> transfuse_chown() path: %s -> fpath: %s\n", path, fpath);

    res = lchown(fpath, uid, gid);
    if (res == -1)
        return -errno;

    return 0;
}

static int transfuse_truncate(const char *path, off_t size,
                              struct fuse_file_info *fi) {
    int res;
    char fpath[PATH_MAX];
    snprintf(fpath, sizeof(fpath), "%s%s", fuse_root_dir, path);

    // Log the constructed path for debugging
    printf(" --> transfuse_truncate() path: %s -> fpath: %s\n", path, fpath);

    if (fi == NULL || fi->fh == 0) {
        // Use truncate if file info is NULL or file handle is invalid
        res = truncate(fpath, size);
    } else {
        // Use ftruncate if file is already open
        res = ftruncate(fi->fh, size);
    }
    if (res == -1)
        return -errno;

    return 0;
}

#ifdef HAVE_UTIMENSAT
static int transfuse_utimens(const char *path, const struct timespec ts[2],
		       struct fuse_file_info *fi)
{
	(void) fi;
	int res;

	/* don't use utime/utimes since they follow symlinks */
	res = utimensat(0, path, ts, AT_SYMLINK_NOFOLLOW);
	if (res == -1)
		return -errno;

	return 0;
}
#endif


static int transfuse_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    int fd;
    char fpath[PATH_MAX];

    // Construct the full path by combining the FUSE root directory with the relative path
    snprintf(fpath, sizeof(fpath), "%s%s", fuse_root_dir, path);

    printf(" --> transfuse_create() path: %s -> fpath: %s\n", path, fpath);

    // Open the file with the appropriate flags
    fd = open(fpath, fi->flags, mode);
    if (fd == -1)
        return -errno;

    fi->fh = fd;
    return 0;
}

static int transfuse_open(const char *path, struct fuse_file_info *fi) {
    int res;
    printf(" --> transfuse_open() and path is %s: \n ", path);

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


static int transfuse_read(const char *path, char *buf, size_t size, off_t offset,
                          struct fuse_file_info *fi) {
    int fd;
    int res;
    char fpath[PATH_MAX];
    // Construct the full path by combining the FUSE root directory with the relative path
    snprintf(fpath, sizeof(fpath), "%s%s", fuse_root_dir, path);
    // Log the constructed path for debugging
    printf(" --> transfuse_read() and path is %s -> fpath: %s\n", path, fpath);

    // Open the file if the file info is NULL
    if (fi == NULL || fi->fh == 0) {
        fd = open(fpath, O_RDONLY);
        if (fd == -1) {
            perror("Error opening file");
            return -errno;
        }
    } else {
        fd = fi->fh;
    }

    // Perform the read operation
    res = pread(fd, buf, size, offset);
    if (res == -1) {
        perror("Error reading file");
        res = -errno;
    }

    // Close the file if it was opened in this function
    if (fi == NULL || fi->fh == 0) {
        close(fd);
    }

    return res;
}


static int transfuse_write(const char *path, const char *buf, size_t size,
                           off_t offset, struct fuse_file_info *fi) {
    int fd;
    int res;
    char fpath[PATH_MAX];

    // Construct the full path by combining the FUSE root directory with the relative path
    snprintf(fpath, sizeof(fpath), "%s%s", fuse_root_dir, path);

    // Log the constructed path for debugging
    printf(" --> transfuse_write() and path is %s -> fpath: %s\n", path, fpath);

    // Open the file if the file info is NULL
    if (fi == NULL || fi->fh == 0) {
        fd = open(fpath, O_WRONLY);
        if (fd == -1) {
            perror("Error opening file");
            return -errno;
        }
    } else {
        fd = fi->fh;
    }

    // Perform the write operation
    res = pwrite(fd, buf, size, offset);
    if (res == -1) {
        perror("Error writing to file");
        res = -errno;
    }

    // Close the file if it was opened in this function
    if (fi == NULL || fi->fh == 0) {
        close(fd);
    }

    return res;
}


static int transfuse_statfs(const char *path, struct statvfs *stbuf) {
    int res;

    res = statvfs(path, stbuf);
    if (res == -1)
        return -errno;

    return 0;
}

// fuse_operations::release: Release an open file
// Release is called when there are no more references to an open file: all file descriptors are closed and all memory mappings are unmapped.
// For every open() call there will be exactly one release() call with the same flags and file handle.
// It is possible to have a file opened more than once, in which case only the last release will mean, that no more reads/writes will happen on the file.
// The return value of release is ignored.
static int transfuse_release(const char *path, struct fuse_file_info *fi) {
    (void) path;
    printf(" --> transfuse_release() and path is: %s \n", path);

    close(fi->fh);
    return 0;
}

static int transfuse_fsync(const char *path, int isdatasync,
                           struct fuse_file_info *fi) {
    /* Just a stub.	 This method is optional and can safely be left
       unimplemented */

    (void) path;
    (void) isdatasync;
    (void) fi;
    return 0;
}

#ifdef HAVE_POSIX_FALLOCATE
static int transfuse_fallocate(const char *path, int mode,
			off_t offset, off_t length, struct fuse_file_info *fi)
{
	int fd;
	int res;

	(void) fi;

	if (mode)
		return -EOPNOTSUPP;

	if(fi == NULL)
		fd = open(path, O_WRONLY);
	else
		fd = fi->fh;

	if (fd == -1)
		return -errno;

	res = -posix_fallocate(fd, offset, length);

	if(fi == NULL)
		close(fd);
	return res;
}
#endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int transfuse_setxattr(const char *path, const char *name, const char *value,
			size_t size, int flags)
{
	int res = lsetxattr(path, name, value, size, flags);
	if (res == -1)
		return -errno;
	return 0;
}

static int transfuse_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
	int res = lgetxattr(path, name, value, size);
	if (res == -1)
		return -errno;
	return res;
}

static int transfuse_listxattr(const char *path, char *list, size_t size)
{
	int res = llistxattr(path, list, size);
	if (res == -1)
		return -errno;
	return res;
}

static int transfuse_removexattr(const char *path, const char *name)
{
	int res = lremovexattr(path, name);
	if (res == -1)
		return -errno;
	return 0;
}
#endif /* HAVE_SETXATTR */

#ifdef HAVE_COPY_FILE_RANGE
static ssize_t transfuse_copy_file_range(const char *path_in,
				   struct fuse_file_info *fi_in,
				   off_t offset_in, const char *path_out,
				   struct fuse_file_info *fi_out,
				   off_t offset_out, size_t len, int flags)
{
	int fd_in, fd_out;
	ssize_t res;

	if(fi_in == NULL)
		fd_in = open(path_in, O_RDONLY);
	else
		fd_in = fi_in->fh;

	if (fd_in == -1)
		return -errno;

	if(fi_out == NULL)
		fd_out = open(path_out, O_WRONLY);
	else
		fd_out = fi_out->fh;

	if (fd_out == -1) {
		close(fd_in);
		return -errno;
	}

	res = copy_file_range(fd_in, &offset_in, fd_out, &offset_out, len,
			      flags);
	if (res == -1)
		res = -errno;

	if (fi_out == NULL)
		close(fd_out);
	if (fi_in == NULL)
		close(fd_in);

	return res;
}
#endif

static off_t transfuse_lseek(const char *path, off_t off, int whence, struct fuse_file_info *fi) {
    int fd;
    off_t res;

    if (fi == NULL)
        fd = open(path, O_RDONLY);
    else
        fd = fi->fh;

    if (fd == -1)
        return -errno;

    res = lseek(fd, off, whence);
    if (res == -1)
        res = -errno;

    if (fi == NULL)
        close(fd);
    return res;
}

static const struct fuse_operations transfuse_oper = {
    .getattr = transfuse_getattr,
    //     .readlink = transfuse_readlink,
    .mknod = transfuse_mknod,
    //     .mkdir = transfuse_mkdir,
    .unlink = transfuse_unlink,
    //     .rmdir = transfuse_rmdir,
    //     .symlink = transfuse_symlink,
    //     .rename = transfuse_rename,
    //     .link = transfuse_link,
    .chmod = transfuse_chmod,
    .chown = transfuse_chown,
    .truncate = transfuse_truncate,
#ifdef HAVE_UTIMENSAT
    .utimens        = transfuse_utimens,
#endif
    //     .open = transfuse_open,
    .read = transfuse_read,
    .write = transfuse_write,
    //     .statfs = transfuse_statfs,
    .release = transfuse_release,
    //     .fsync = transfuse_fsync,
    // #ifdef HAVE_POSIX_FALLOCATE
    //     .fallocate      = transfuse_fallocate,
    // #endif
#ifdef HAVE_SETXATTR
     .setxattr       = transfuse_setxattr,
     .getxattr       = transfuse_getxattr,
     // .listxattr      = transfuse_listxattr,
     // .removexattr    = transfuse_removexattr,
#endif
    .readdir = transfuse_readdir,
    .init = transfuse_init,
    //     .access = transfuse_access,
    .create = transfuse_create,
    // #ifdef HAVE_COPY_FILE_RANGE
    //     .copy_file_range = transfuse_copy_file_range,
    // #endif
    //     .lseek = transfuse_lseek,
};

int main(int argc, char *argv[]) {
    enum { MAX_ARGS = 10 };
    int i, new_argc;
    char *new_argv[MAX_ARGS];

    umask(0);
    /* Process the "--plus" option apart */
    for (i = 0, new_argc = 0; (i < argc) && (new_argc < MAX_ARGS); i++) {
        if (!strcmp(argv[i], "--plus")) {
            fill_dir_plus = FUSE_FILL_DIR_PLUS;
        } else {
            new_argv[new_argc++] = argv[i];
        }
    }
    return fuse_main(new_argc, new_argv, &transfuse_oper, nullptr);
}
