# TransFuse-Thesis



## Abstract

Database Management System (DBMS) have always wanted strict control over the
transfer of pages from disk to memory and vice versa, and that’s why they utilize a
buffer manager to achieve spatial and temporal control. However, the Operating System
(OS) offers an alternative approach using memory-mapped file I/O via mmap(), which
relies on the OS to manage paging transparently. There are well-known problems
introduced by delegating memory management to the OS page cache, especially
concerning the safety of transactions due to the lack of control over when pages can
be flushed to permanent storage. This allows uncommitted changes to persist to disk
and leaves the database inconsistent. Existing solutions, such as OS copy-on-write and
shadow paging, address these issues to some extent, but they have limitations. In this
thesis, we propose a new solution leveraging Filesystem in Userspace (FUSE) which
allows for building a custom implementation of the filesystem in userspace rather than
in kernel. FUSE intercepts write requests made by mmap(), validates them, and ensures
that only valid writes persist to permanent storage. If an invalid write is detected,
the database application is notified via Unix-based inter-process communication, and
the database re-marks this page as dirty so that it can be re-scheduled for eviction
later. We show that our solution guarantees the correctness and safety of database
transactions while using FUSE and MMap for file I/O. Our results demonstrate that
the solution performs better than a buffer manager when the dataset size is small and
fits entirely in memory. However, there is room for improvement in terms of scalability
and performance as the amount of data exceeds available memory, which we leave as
future work.






Link to Thesis: https://drive.google.com/file/d/1wXNzrvloxtrAdo17_9GgnYyX7XYR252C/view?usp=drive_link
