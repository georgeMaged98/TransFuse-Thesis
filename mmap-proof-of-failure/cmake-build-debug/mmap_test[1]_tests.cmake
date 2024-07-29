add_test([=[HelloTest.BasicAssertions]=]  /Users/georgemaged/Academic/TUM/Thesis/proof_of_failure/cmake-build-debug/mmap_test [==[--gtest_filter=HelloTest.BasicAssertions]==] --gtest_also_run_disabled_tests)
set_tests_properties([=[HelloTest.BasicAssertions]=]  PROPERTIES WORKING_DIRECTORY /Users/georgemaged/Academic/TUM/Thesis/proof_of_failure/cmake-build-debug SKIP_REGULAR_EXPRESSION [==[\[  SKIPPED \]]==])
set(  mmap_test_TESTS HelloTest.BasicAssertions)
