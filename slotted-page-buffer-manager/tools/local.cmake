# ---------------------------------------------------------------------------
# MODERNDBS
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

set(TOOLS_SRC tools/database_wrapper.cc tools/test_512M.cc tools/test_1GB.cc tools/test_4GB.cc)

# ---------------------------------------------------------------------------
# Executables
# ---------------------------------------------------------------------------

add_executable(database_wrapper tools/database_wrapper.cc)
target_link_libraries(database_wrapper moderndbs Threads::Threads)


add_executable(test512M tools/test_512M.cc)
target_link_libraries(test512M moderndbs Threads::Threads)

add_executable(test1GB tools/test_1GB.cc)
target_link_libraries(test1GB moderndbs Threads::Threads)

add_executable(test4GB tools/test_4GB.cc)
target_link_libraries(test4GB moderndbs Threads::Threads)

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_clang_tidy_target(lint_tools "${TOOLS_SRC}")
list(APPEND lint_targets lint_tools)
