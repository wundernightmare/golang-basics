ReadTimeout=2s DialTimeout=2s, ctx deadline (when used)=5s, pool=1

| ctx | fault | op | api | result | x ReadTimeout | error / where it is blocked | after heal |
|---|---|---|---|---|---|---|---|
| none | ch-pause | insert-big | native | **HUNG >30s** | 15x | waiting inside http.Transport.RoundTrip; writing HTTP request body: socket Write blocked (ResponseHeaderTimeout not armed yet) | returned 0.1s after heal: <nil> |
| none | ch-pause | insert-big | std | **HUNG >30s** | 15x | waiting inside http.Transport.RoundTrip; writing HTTP request body: socket Write blocked (ResponseHeaderTimeout not armed yet) | returned 0.1s after heal: <nil> |
| none | ch-pause | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28233?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | ch-pause | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28241?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | ch-pause | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28232?client_protocol_version=544… |  |
| none | ch-pause | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28240?client_protocol_version=544… |  |
| none | ch-pause | select-stream | native | **HUNG >30s** | 15x | github.com/ClickHouse/clickhouse-go/v2.(*rows).Next <- main.(*nativeClient).stream <- main.runOp <- main.runCase.func6; reading HTTP response body (not covered by ReadTimeout) | returned 2.6s after heal: <nil> |
| none | ch-pause | select-stream | std | **HUNG >30s** | 15x | github.com/ClickHouse/clickhouse-go/v2.(*rows).Next <- github.com/ClickHouse/clickhouse-go/v2.(*stdRows).Next <- database/sql.(*Rows).nextLocked <- database/sql.(*Rows).Next.func1; reading HTTP response body (not covered by ReadTimeout) | returned 4.4s after heal: <nil> |
| none | nft-drop | insert-big | native | **HUNG >45s** | 23x | waiting inside http.Transport.RoundTrip; writing HTTP request body: socket Write blocked (ResponseHeaderTimeout not armed yet) | returned 7.5s after heal: batch sendStreamQuery: Post "http://default@127.0.0.1:28128?client_protocol_version=54460&database=default&default_format=Native&query=INSERT+INTO+chaos.sink+%28s%29+FORMAT+Native": write tcp 127.0.0.1:51674->127.0.0.1:28128: use of closed network connection |
| none | nft-drop | insert-big | std | **HUNG >45s** | 23x | waiting inside http.Transport.RoundTrip; writing HTTP request body: socket Write blocked (ResponseHeaderTimeout not armed yet) | returned 7.5s after heal: driver: bad connection |
| none | nft-drop | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28126?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | nft-drop | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28192?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | nft-drop | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28125?client_protocol_version=544… |  |
| none | nft-drop | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28191?client_protocol_version=544… |  |
| none | nft-drop | select-stream | native | **HUNG >45s** | 23x | github.com/ClickHouse/clickhouse-go/v2.(*rows).Next <- main.(*nativeClient).stream <- main.runOp <- main.runCase.func6; reading HTTP response body (not covered by ReadTimeout) | returned 7.4s after heal: readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 43436: read full: read: unexpected EOF |
| none | nft-drop | select-stream | std | **HUNG >45s** | 23x | github.com/ClickHouse/clickhouse-go/v2.(*rows).Next <- github.com/ClickHouse/clickhouse-go/v2.(*stdRows).Next <- database/sql.(*Rows).nextLocked <- database/sql.(*Rows).Next.func1; reading HTTP response body (not covered by ReadTimeout) | returned 7.6s after heal: readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 49568: read full: read: unexpected EOF |
| none | nft-drop-in | insert-big | native | **HUNG >45s** | 23x | waiting inside http.Transport.RoundTrip; writing HTTP request body: socket Write blocked (ResponseHeaderTimeout not armed yet) | returned 7.6s after heal: batch sendStreamQuery: [HTTP 503] code: 209, message: Timeout exceeded while reading from socket (peer: [::ffff:10.89.0.8]:36848, local: [::ffff:10.89.0.6]:8123, 30000 ms). (SOCKET_TIMEOUT) (version 25.8.33.6 (official build)) |
| none | nft-drop-in | insert-big | std | **HUNG >45s** | 23x | waiting inside http.Transport.RoundTrip; writing HTTP request body: socket Write blocked (ResponseHeaderTimeout not armed yet) | returned 7.7s after heal: batch sendStreamQuery: [HTTP 503] code: 209, message: Timeout exceeded while reading from socket (peer: [::ffff:10.89.0.8]:45306, local: [::ffff:10.89.0.6]:8123, 30000 ms). (SOCKET_TIMEOUT) (version 25.8.33.6 (official build)) |
| none | nft-drop-in | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28130?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | nft-drop-in | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28196?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | nft-drop-in | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28129?client_protocol_version=544… |  |
| none | nft-drop-in | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28195?client_protocol_version=544… |  |
| none | nft-drop-in | select-stream | native | **HUNG >45s** | 23x | github.com/ClickHouse/clickhouse-go/v2.(*rows).Next <- main.(*nativeClient).stream <- main.runOp <- main.runCase.func6; reading HTTP response body (not covered by ReadTimeout) | returned 7.6s after heal: readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 49568: read full: read: unexpected EOF |
| none | nft-drop-in | select-stream | std | **HUNG >45s** | 23x | github.com/ClickHouse/clickhouse-go/v2.(*rows).Next <- github.com/ClickHouse/clickhouse-go/v2.(*stdRows).Next <- database/sql.(*Rows).nextLocked <- database/sql.(*Rows).Next.func1; reading HTTP response body (not covered by ReadTimeout) | returned 7.8s after heal: readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 52634: read full: read: unexpected EOF |
| none | none | insert-big | native | 0.2s | 0x | ok (no error) |  |
| none | none | insert-big | std | 0.1s | 0x | ok (no error) |  |
| none | none | ping-idle | native | 0.0s | 0x | ok (no error) |  |
| none | none | ping-idle | std | 0.0s | 0x | ok (no error) |  |
| none | none | ping-new | native | 0.0s | 0x | ok (no error) |  |
| none | none | ping-new | std | 0.0s | 0x | ok (no error) |  |
| none | none | select-slow-body | native | 6.0s | 3x | ok (no error) |  |
| none | none | select-slow-body | std | 6.0s | 3x | ok (no error) |  |
| none | none | select-slow-headers | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28104?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | none | select-slow-headers | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28168?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | toxi-bandwidth | insert-big | native | **HUNG >45s** | 23x | waiting inside http.Transport.RoundTrip; writing HTTP request body: socket Write blocked (ResponseHeaderTimeout not armed yet) | returned 0.1s after heal: <nil> |
| none | toxi-bandwidth | insert-big | std | **HUNG >45s** | 23x | waiting inside http.Transport.RoundTrip; writing HTTP request body: socket Write blocked (ResponseHeaderTimeout not armed yet) | returned 0.0s after heal: <nil> |
| none | toxi-bandwidth | ping-idle | native | 1.0s | 0x | ok (no error) |  |
| none | toxi-bandwidth | ping-idle | std | 1.0s | 0x | ok (no error) |  |
| none | toxi-bandwidth | ping-new | native | 2.1s | 1x | ok (no error) |  |
| none | toxi-bandwidth | ping-new | std | 2.1s | 1x | ok (no error) |  |
| none | toxi-bandwidth | select-stream | native | **HUNG >45s** | 23x | github.com/ClickHouse/clickhouse-go/v2.(*rows).Next <- main.(*nativeClient).stream <- main.runOp <- main.runCase.func6; reading HTTP response body (not covered by ReadTimeout) | returned 0.0s after heal: readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 9709: read full: read: unexpected EOF |
| none | toxi-bandwidth | select-stream | std | **HUNG >45s** | 23x | github.com/ClickHouse/clickhouse-go/v2.(*rows).Next <- github.com/ClickHouse/clickhouse-go/v2.(*stdRows).Next <- database/sql.(*Rows).nextLocked <- database/sql.(*Rows).Next.func1; reading HTTP response body (not covered by ReadTimeout) | returned 0.0s after heal: readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 1533: read full: read: unexpected EOF |
| none | toxi-down | insert-big | native | 0.2s | 0x | batch sendStreamQuery: Post "http://default@127.0.0.1:28124?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | toxi-down | insert-big | std | 0.2s | 0x | driver: bad connection |  |
| none | toxi-down | ping-idle | native | 0.2s | 0x | sendQuery: Post "http://default@127.0.0.1:28122?client_protocol_version=54460&database=default&default_format=Native": read tcp 127.0.0.1:48… |  |
| none | toxi-down | ping-idle | std | 0.2s | 0x | sendQuery: Post "http://default@127.0.0.1:28188?client_protocol_version=54460&database=default&default_format=Native": read tcp 127.0.0.1:54… |  |
| none | toxi-down | ping-new | native | 0.2s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28121?client_protocol_version=544… |  |
| none | toxi-down | ping-new | std | 0.2s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28187?client_protocol_version=544… |  |
| none | toxi-down | select-stream | native | 0.0s | 0x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 5872: read full: read: unexpected EOF |  |
| none | toxi-down | select-stream | std | 0.0s | 0x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 13770: read full: read: unexpected EOF |  |
| none | toxi-reset | insert-big | native | 0.1s | 0x | batch sendStreamQuery: Post "http://default@127.0.0.1:28120?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | toxi-reset | insert-big | std | 0.1s | 0x | driver: bad connection |  |
| none | toxi-reset | ping-idle | native | 0.0s | 0x | sendQuery: Post "http://default@127.0.0.1:28118?client_protocol_version=54460&database=default&default_format=Native": EOF |  |
| none | toxi-reset | ping-idle | std | 0.0s | 0x | sendQuery: Post "http://default@127.0.0.1:28184?client_protocol_version=54460&database=default&default_format=Native": EOF |  |
| none | toxi-reset | ping-new | native | 0.0s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28117?client_protocol_version=544… |  |
| none | toxi-reset | ping-new | std | 0.0s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28183?client_protocol_version=544… |  |
| none | toxi-reset | select-stream | native | 0.0s | 0x | readData stream: block decode for exception: read: read: unexpected EOF |  |
| none | toxi-reset | select-stream | std | 0.0s | 0x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 14671: read full: read: unexpected EOF |  |
| none | toxi-timeout | insert-big | native | 2.2s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28105?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | toxi-timeout | insert-big | std | 2.1s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28174?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | toxi-timeout | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28111?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | toxi-timeout | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28172?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | toxi-timeout | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28113?client_protocol_version=544… |  |
| none | toxi-timeout | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28171?client_protocol_version=544… |  |
| none | toxi-timeout | select-stream | native | 11.6s | 6x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 5598: read full: read: unexpected EOF |  |
| none | toxi-timeout | select-stream | std | 11.2s | 6x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 14637: read full: read: unexpected EOF |  |
| none | toxi-timeout-down | insert-big | native | 2.3s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28109?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | toxi-timeout-down | insert-big | std | 2.1s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28178?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | toxi-timeout-down | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28106?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | toxi-timeout-down | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28176?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | toxi-timeout-down | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28107?client_protocol_version=544… |  |
| none | toxi-timeout-down | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28175?client_protocol_version=544… |  |
| none | toxi-timeout-down | select-stream | native | 11.6s | 6x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 4823: read full: read: unexpected EOF |  |
| none | toxi-timeout-down | select-stream | std | 11.1s | 6x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 14239: read full: read: unexpected EOF |  |
| deadline | ch-pause | insert-big | native | 4.9s | 2x | batch sendStreamQuery: Post "http://default@127.0.0.1:28239?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | ch-pause | insert-big | std | 4.8s | 2x | batch sendStreamQuery: Post "http://default@127.0.0.1:28247?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | ch-pause | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28237?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | ch-pause | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28245?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | ch-pause | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28236?client_protocol_version=544… |  |
| deadline | ch-pause | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28244?client_protocol_version=544… |  |
| deadline | ch-pause | select-stream | native | 4.8s | 2x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 61832: read full: read: context deadline exceeded |  |
| deadline | ch-pause | select-stream | std | 4.8s | 2x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 60810: read full: read: context deadline exceeded |  |
| deadline | nft-drop | insert-big | native | 4.9s | 2x | batch sendStreamQuery: Post "http://default@127.0.0.1:28161?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | nft-drop | insert-big | std | 4.9s | 2x | batch sendStreamQuery: Post "http://default@127.0.0.1:28227?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | nft-drop | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28159?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | nft-drop | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28225?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | nft-drop | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28158?client_protocol_version=544… |  |
| deadline | nft-drop | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28224?client_protocol_version=544… |  |
| deadline | nft-drop | select-stream | native | 4.8s | 2x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 29046: read full: read: context deadline exceeded |  |
| deadline | nft-drop | select-stream | std | 4.8s | 2x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 20534: read full: read: context deadline exceeded |  |
| deadline | nft-drop-in | insert-big | native | 4.9s | 2x | batch sendStreamQuery: Post "http://default@127.0.0.1:28165?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | nft-drop-in | insert-big | std | 4.9s | 2x | batch sendStreamQuery: Post "http://default@127.0.0.1:28231?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | nft-drop-in | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28163?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | nft-drop-in | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28229?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | nft-drop-in | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28162?client_protocol_version=544… |  |
| deadline | nft-drop-in | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28228?client_protocol_version=544… |  |
| deadline | nft-drop-in | select-stream | native | 4.8s | 2x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 25622: read full: read: context deadline exceeded |  |
| deadline | nft-drop-in | select-stream | std | 4.8s | 2x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 39746: read full: read: context deadline exceeded |  |
| deadline | none | insert-big | native | 0.1s | 0x | ok (no error) |  |
| deadline | none | insert-big | std | 0.1s | 0x | ok (no error) |  |
| deadline | none | ping-idle | native | 0.0s | 0x | ok (no error) |  |
| deadline | none | ping-idle | std | 0.0s | 0x | ok (no error) |  |
| deadline | none | ping-new | native | 0.0s | 0x | ok (no error) |  |
| deadline | none | ping-new | std | 0.0s | 0x | ok (no error) |  |
| deadline | none | select-slow-body | native | 5.0s | 3x | readData stream: block decode for exception: read: read: context deadline exceeded |  |
| deadline | none | select-slow-body | std | 5.0s | 3x | readData stream: block decode for exception: read: read: context deadline exceeded |  |
| deadline | none | select-slow-headers | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28135?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | none | select-slow-headers | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28201?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | toxi-bandwidth | insert-big | native | 4.9s | 2x | batch sendStreamQuery: Post "http://default@127.0.0.1:28149?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | toxi-bandwidth | insert-big | std | 4.9s | 2x | batch sendStreamQuery: Post "http://default@127.0.0.1:28215?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | toxi-bandwidth | ping-idle | native | 1.0s | 0x | ok (no error) |  |
| deadline | toxi-bandwidth | ping-idle | std | 1.0s | 0x | ok (no error) |  |
| deadline | toxi-bandwidth | ping-new | native | 2.1s | 1x | ok (no error) |  |
| deadline | toxi-bandwidth | ping-new | std | 2.1s | 1x | ok (no error) |  |
| deadline | toxi-bandwidth | select-stream | native | 4.8s | 2x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 8004: read full: read: context deadline exceeded |  |
| deadline | toxi-bandwidth | select-stream | std | 4.8s | 2x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 15170: read full: read: context deadline exceeded |  |
| deadline | toxi-down | insert-big | native | 0.2s | 0x | batch sendStreamQuery: Post "http://default@127.0.0.1:28157?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | toxi-down | insert-big | std | 0.2s | 0x | driver: bad connection |  |
| deadline | toxi-down | ping-idle | native | 0.2s | 0x | sendQuery: Post "http://default@127.0.0.1:28155?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | toxi-down | ping-idle | std | 0.2s | 0x | sendQuery: Post "http://default@127.0.0.1:28221?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | toxi-down | ping-new | native | 0.2s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28154?client_protocol_version=544… |  |
| deadline | toxi-down | ping-new | std | 0.2s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28220?client_protocol_version=544… |  |
| deadline | toxi-down | select-stream | native | 0.0s | 0x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 3034: read full: read: unexpected EOF |  |
| deadline | toxi-down | select-stream | std | 0.0s | 0x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 17062: read full: read: unexpected EOF |  |
| deadline | toxi-reset | insert-big | native | 0.0s | 0x | batch sendStreamQuery: Post "http://default@127.0.0.1:28153?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | toxi-reset | insert-big | std | 0.1s | 0x | driver: bad connection |  |
| deadline | toxi-reset | ping-idle | native | 0.0s | 0x | sendQuery: Post "http://default@127.0.0.1:28151?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | toxi-reset | ping-idle | std | 0.0s | 0x | sendQuery: Post "http://default@127.0.0.1:28217?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | toxi-reset | ping-new | native | 0.0s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28150?client_protocol_version=544… |  |
| deadline | toxi-reset | ping-new | std | 0.0s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28216?client_protocol_version=544… |  |
| deadline | toxi-reset | select-stream | native | 0.0s | 0x | readData stream: block decode for exception: read: read: unexpected EOF |  |
| deadline | toxi-reset | select-stream | std | 0.0s | 0x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 16315: read full: read: unexpected EOF |  |
| deadline | toxi-timeout | insert-big | native | 2.1s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28141?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | toxi-timeout | insert-big | std | 2.1s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28207?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | toxi-timeout | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28139?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | toxi-timeout | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28205?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | toxi-timeout | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28138?client_protocol_version=544… |  |
| deadline | toxi-timeout | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28204?client_protocol_version=544… |  |
| deadline | toxi-timeout | select-stream | native | 4.8s | 2x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 7346: read full: read: context deadline exceeded |  |
| deadline | toxi-timeout | select-stream | std | 4.8s | 2x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 13151: read full: read: context deadline exceeded |  |
| deadline | toxi-timeout-down | insert-big | native | 2.1s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28145?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | toxi-timeout-down | insert-big | std | 2.1s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28211?client_protocol_version=54460&database=default&default_format=Native&max_executi… |  |
| deadline | toxi-timeout-down | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28143?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | toxi-timeout-down | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28209?client_protocol_version=54460&database=default&default_format=Native&max_execution_time=9": … |  |
| deadline | toxi-timeout-down | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28142?client_protocol_version=544… |  |
| deadline | toxi-timeout-down | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28208?client_protocol_version=544… |  |
| deadline | toxi-timeout-down | select-stream | native | 4.8s | 2x | readData stream: block decode for exception: read: read: context deadline exceeded |  |
| deadline | toxi-timeout-down | select-stream | std | 4.8s | 2x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 14061: read full: read: context deadline exceeded |  |
