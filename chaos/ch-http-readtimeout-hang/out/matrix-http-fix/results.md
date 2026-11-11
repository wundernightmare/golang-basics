protocol=http fix=true ReadTimeout=2s DialTimeout=2s, ctx deadline (when used)=5s, pool=1

| ctx | fault | op | api | result | x ReadTimeout | error / where it is blocked | after heal |
|---|---|---|---|---|---|---|---|
| none | ch-pause | insert-big | native | 1.8s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28169?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | ch-pause | insert-big | std | 1.9s | 1x | driver: bad connection |  |
| none | ch-pause | ping-idle | native | 1.9s | 1x | sendQuery: Post "http://default@127.0.0.1:28167?client_protocol_version=54460&database=default&default_format=Native": read tcp 127.0.0.1:45… |  |
| none | ch-pause | ping-idle | std | 1.9s | 1x | sendQuery: Post "http://default@127.0.0.1:28171?client_protocol_version=54460&database=default&default_format=Native": read tcp 127.0.0.1:53… |  |
| none | ch-pause | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28166?client_protocol_version=544… |  |
| none | ch-pause | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28170?client_protocol_version=544… |  |
| none | ch-pause | select-stream | native | 2.1s | 1x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 61832: read full: read: read tcp 127.0.0.1:48596->1… |  |
| none | ch-pause | select-stream | std | 2.0s | 1x | readData stream: block decode for exception: read: read: read tcp 127.0.0.1:53242->127.0.0.1:28172: i/o timeout |  |
| none | nft-drop | insert-big | native | 1.8s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28128?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | nft-drop | insert-big | std | 1.8s | 1x | driver: bad connection |  |
| none | nft-drop | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28126?client_protocol_version=54460&database=default&default_format=Native": read tcp 127.0.0.1:60… |  |
| none | nft-drop | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28159?client_protocol_version=54460&database=default&default_format=Native": read tcp 127.0.0.1:53… |  |
| none | nft-drop | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28125?client_protocol_version=544… |  |
| none | nft-drop | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28158?client_protocol_version=544… |  |
| none | nft-drop | select-stream | native | 2.0s | 1x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 35486: read full: read: read tcp 127.0.0.1:39158->1… |  |
| none | nft-drop | select-stream | std | 2.0s | 1x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 20610: read full: read: read tcp 127.0.0.1:33002->1… |  |
| none | nft-drop-in | insert-big | native | 1.8s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28132?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | nft-drop-in | insert-big | std | 1.9s | 1x | driver: bad connection |  |
| none | nft-drop-in | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28130?client_protocol_version=54460&database=default&default_format=Native": read tcp 127.0.0.1:32… |  |
| none | nft-drop-in | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28163?client_protocol_version=54460&database=default&default_format=Native": read tcp 127.0.0.1:40… |  |
| none | nft-drop-in | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28129?client_protocol_version=544… |  |
| none | nft-drop-in | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28162?client_protocol_version=544… |  |
| none | nft-drop-in | select-stream | native | 2.0s | 1x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 25564: read full: read: read tcp 127.0.0.1:53036->1… |  |
| none | nft-drop-in | select-stream | std | 2.0s | 1x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 32163: read full: read: read tcp 127.0.0.1:44678->1… |  |
| none | none | insert-big | native | 0.2s | 0x | ok (no error) |  |
| none | none | insert-big | std | 0.1s | 0x | ok (no error) |  |
| none | none | ping-idle | native | 0.0s | 0x | ok (no error) |  |
| none | none | ping-idle | std | 0.0s | 0x | ok (no error) |  |
| none | none | ping-new | native | 0.0s | 0x | ok (no error) |  |
| none | none | ping-new | std | 0.0s | 0x | ok (no error) |  |
| none | none | select-slow-body | native | 2.0s | 1x | readData stream: block decode for exception: read: read: read tcp 127.0.0.1:41476->127.0.0.1:28108: i/o timeout |  |
| none | none | select-slow-body | std | 2.0s | 1x | readData stream: block decode for exception: read: read: read tcp 127.0.0.1:44884->127.0.0.1:28136: i/o timeout |  |
| none | none | select-slow-headers | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28104?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | none | select-slow-headers | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28135?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | toxi-bandwidth | insert-big | native | 1.9s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28116?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | toxi-bandwidth | insert-big | std | 1.9s | 1x | driver: bad connection |  |
| none | toxi-bandwidth | ping-idle | native | 1.0s | 0x | ok (no error) |  |
| none | toxi-bandwidth | ping-idle | std | 1.0s | 0x | ok (no error) |  |
| none | toxi-bandwidth | ping-new | native | 2.1s | 1x | ok (no error) |  |
| none | toxi-bandwidth | ping-new | std | 2.1s | 1x | ok (no error) |  |
| none | toxi-bandwidth | select-stream | native | **HUNG >45s** | 23x | github.com/ClickHouse/clickhouse-go/v2.(*rows).Next <- main.(*nativeClient).stream <- main.runOp <- main.runCase.func6; reading HTTP response body (not covered by ReadTimeout) | returned 0.0s after heal: readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 5621: read full: read: unexpected EOF |
| none | toxi-bandwidth | select-stream | std | **HUNG >45s** | 23x | github.com/ClickHouse/clickhouse-go/v2.(*rows).Next <- github.com/ClickHouse/clickhouse-go/v2.(*stdRows).Next <- database/sql.(*Rows).nextLocked <- database/sql.(*Rows).Next.func1; reading HTTP response body (not covered by ReadTimeout) | returned 0.0s after heal: readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 20952: read full: read: unexpected EOF |
| none | toxi-down | insert-big | native | 0.2s | 0x | batch sendStreamQuery: Post "http://default@127.0.0.1:28124?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | toxi-down | insert-big | std | 0.2s | 0x | driver: bad connection |  |
| none | toxi-down | ping-idle | native | 0.2s | 0x | sendQuery: Post "http://default@127.0.0.1:28122?client_protocol_version=54460&database=default&default_format=Native": read tcp 127.0.0.1:41… |  |
| none | toxi-down | ping-idle | std | 0.2s | 0x | sendQuery: Post "http://default@127.0.0.1:28155?client_protocol_version=54460&database=default&default_format=Native": read tcp 127.0.0.1:50… |  |
| none | toxi-down | ping-new | native | 0.2s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28121?client_protocol_version=544… |  |
| none | toxi-down | ping-new | std | 0.2s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28154?client_protocol_version=544… |  |
| none | toxi-down | select-stream | native | 0.0s | 0x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 319: read full: read: unexpected EOF |  |
| none | toxi-down | select-stream | std | 0.0s | 0x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 12668: read full: read: unexpected EOF |  |
| none | toxi-reset | insert-big | native | 0.1s | 0x | batch sendStreamQuery: Post "http://default@127.0.0.1:28120?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | toxi-reset | insert-big | std | 0.1s | 0x | driver: bad connection |  |
| none | toxi-reset | ping-idle | native | 0.0s | 0x | sendQuery: Post "http://default@127.0.0.1:28118?client_protocol_version=54460&database=default&default_format=Native": EOF |  |
| none | toxi-reset | ping-idle | std | 0.0s | 0x | sendQuery: Post "http://default@127.0.0.1:28151?client_protocol_version=54460&database=default&default_format=Native": EOF |  |
| none | toxi-reset | ping-new | native | 0.0s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28117?client_protocol_version=544… |  |
| none | toxi-reset | ping-new | std | 0.0s | 0x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28150?client_protocol_version=544… |  |
| none | toxi-reset | select-stream | native | 0.0s | 0x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 5300: read full: read: unexpected EOF |  |
| none | toxi-reset | select-stream | std | 0.0s | 0x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 16501: read full: read: unexpected EOF |  |
| none | toxi-timeout | insert-big | native | 1.8s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28105?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | toxi-timeout | insert-big | std | 1.9s | 1x | driver: bad connection |  |
| none | toxi-timeout | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28106?client_protocol_version=54460&database=default&default_format=Native": read tcp 127.0.0.1:36… |  |
| none | toxi-timeout | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28139?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | toxi-timeout | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28111?client_protocol_version=544… |  |
| none | toxi-timeout | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28138?client_protocol_version=544… |  |
| none | toxi-timeout | select-stream | native | 2.0s | 1x | readData stream: block decode for exception: read: read: read tcp 127.0.0.1:41528->127.0.0.1:28110: i/o timeout |  |
| none | toxi-timeout | select-stream | std | 2.0s | 1x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 14135: read full: read: read tcp 127.0.0.1:50446->1… |  |
| none | toxi-timeout-down | insert-big | native | 1.8s | 1x | batch sendStreamQuery: Post "http://default@127.0.0.1:28112?client_protocol_version=54460&database=default&default_format=Native&query=INSER… |  |
| none | toxi-timeout-down | insert-big | std | 1.9s | 1x | driver: bad connection |  |
| none | toxi-timeout-down | ping-idle | native | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28107?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | toxi-timeout-down | ping-idle | std | 2.0s | 1x | sendQuery: Post "http://default@127.0.0.1:28143?client_protocol_version=54460&database=default&default_format=Native": net/http: timeout awa… |  |
| none | toxi-timeout-down | ping-new | native | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28113?client_protocol_version=544… |  |
| none | toxi-timeout-down | ping-new | std | 2.0s | 1x | failed to query server hello: failed to query server hello info: sendQuery: Post "http://default@127.0.0.1:28142?client_protocol_version=544… |  |
| none | toxi-timeout-down | select-stream | native | 2.0s | 1x | readData stream: block decode for exception: read: read: read tcp 127.0.0.1:60242->127.0.0.1:28109: i/o timeout |  |
| none | toxi-timeout-down | select-stream | std | 2.0s | 1x | readData stream: block decode for exception: clickhouse [Decode]: randomString(1024) row 13087: read full: read: read tcp 127.0.0.1:38314->1… |  |
