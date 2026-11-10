# HK COS multipart 实测（2026-11-09）

主机 `vwork-dev` → `cellhive-e2e-hk-1376795072.cos.ap-hongkong.myqcloud.com`，现场 DNS 为 `169.254.0.47`。CPU 0–3、GOMAXPROCS=2；共享开发主机有其他服务及构建负载，不是隔离性能环境。

相同 256MiB 随机文件，SHA-256 `ec68ec48057e2c15f31da12a1587f38a13d0537636b20389509332f97d242a61`。每个配置独立测试进程、一个上传任务、串行分片。三轮顺序为 5/16/32、32/5/16、16/32/5。通过 Go overlay 仅替换分片默认常量，执行生产 `createMultipart` 路径；没有修改生产默认值或增加运行时选项。

计时含 initiate、读取源文件、全部分片上传和 complete，不含源文件生成、预先 SHA、下载校验及清理。每轮重新 GET 全对象核验长度与 SHA-256，然后 Delete；9/9 测试通过，删除请求全部成功。RSS 是整个测试进程的最大 RSS（含校验），不是仅分片分配量。

| 分片 | 三轮上传秒数 | 中位秒数 | 中位吞吐 MiB/s | 进程峰值 RSS MiB |
| --- | --- | --- | --- | --- |
| 5MiB | 10.085 / 6.771 / 7.891 | 7.891 | 32.44 | 24.25 |
| 16MiB | 8.805 / 6.618 / 9.971 | 8.805 | 29.07 | 35.12 |
| 32MiB | 6.493 / 6.180 / 5.744 | 6.180 | 41.42 | 50.75 |

32MiB 在三轮均快于另外两档；测试后用户确认采用 32MiB，默认分片已更新；单次 PUT / multipart 切换阈值仍为 5MiB。仅三轮且有共享负载，不能保证相同比例出现在生产；本测试未覆盖上传并发、10GiB 文件、冷恢复或小对象单次 PUT。小于 multipart 阈值的对象应独立考虑，不应仅凭大对象结果改变小对象路径。

源码：`internal/bucket/cos_multipart_benchmark_test.go`。复测设置 `CELLHIVE_MULTIPART_BENCH_FILE` 指向相同本地文件，以及现有 COS 环境变量，运行 `go test ./internal/bucket -run '^TestCOSMultipartThroughput$' -v -count=1`。默认会跳过，测试限定指定 HK 桶。各分片变体使用 overlay 改 `nativeMultipartPartBytes` 后 `go test -c`，不修改工作树源码。

原始日志、overlay、运行脚本及二进制：`/tmp/cos-part-bench/`。测试生成对象均为独立 `cells/multipart-benchmark/__bench__/<时间戳>/ltx/...`，未触及现有 cell 数据。
