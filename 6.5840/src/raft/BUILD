go_library(
    name = "raft",
    srcs = [
        "config.go",
        "persister.go",
        "raft.go",
        "util.go",
    ],
)

go_test(
    name = "raft_test",
    srcs = ["test_test.go"],
    library = ":raft",
)
