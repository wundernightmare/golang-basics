protocol=native ReadTimeout=2s DialTimeout=2s, ctx deadline (when used)=5s, pool=1

| ctx | fault | op | api | result | x ReadTimeout | error / where it is blocked | after heal |
|---|---|---|---|---|---|---|---|
| none | ch-pause | insert-big | native | **HUNG >30s** | 15x | socket Write blocked: clickhouse-go sets a read deadline only, no write deadline | returned 0.1s after heal: <nil> |
| none | ch-pause | insert-big | std | **HUNG >30s** | 15x | socket Write blocked: clickhouse-go sets a read deadline only, no write deadline | returned 0.1s after heal: <nil> |
| none | ch-pause | ping-idle | native | 2.0s | 1x | ping: failed to read packet from 127.0.0.1:28233 (conn_id=1, age=2s): read: read tcp 127.0.0.1:52142->127.0.0.1:28233: i/o timeout |  |
| none | ch-pause | ping-idle | std | 2.0s | 1x | ping: failed to read packet from 127.0.0.1:28241 (conn_id=1, age=2s): read: read tcp 127.0.0.1:54246->127.0.0.1:28241: i/o timeout |  |
| none | ch-pause | ping-new | native | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28232 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:54680->127.0.0.1:28232: i/o tim… |  |
| none | ch-pause | ping-new | std | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28240 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:36424->127.0.0.1:28240: i/o tim… |  |
| none | ch-pause | select-stream | native | 1.7s | 1x | read data: failed to decode block from 127.0.0.1:28234 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 51612: rea… |  |
| none | ch-pause | select-stream | std | 1.7s | 1x | read data: failed to decode block from 127.0.0.1:28242 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 50590: rea… |  |
| none | nft-drop | insert-big | native | **HUNG >45s** | 23x | socket Write blocked: clickhouse-go sets a read deadline only, no write deadline | returned 7.4s after heal: <nil> |
| none | nft-drop | insert-big | std | **HUNG >45s** | 23x | socket Write blocked: clickhouse-go sets a read deadline only, no write deadline | returned 7.4s after heal: <nil> |
| none | nft-drop | ping-idle | native | 2.0s | 1x | ping: failed to read packet from 127.0.0.1:28126 (conn_id=1, age=2s): read: read tcp 127.0.0.1:40396->127.0.0.1:28126: i/o timeout |  |
| none | nft-drop | ping-idle | std | 2.0s | 1x | ping: failed to read packet from 127.0.0.1:28192 (conn_id=1, age=2s): read: read tcp 127.0.0.1:40716->127.0.0.1:28192: i/o timeout |  |
| none | nft-drop | ping-new | native | 2.0s | 1x | dial tcp 127.0.0.1:28125: i/o timeout |  |
| none | nft-drop | ping-new | std | 2.0s | 1x | dial tcp 127.0.0.1:28191: i/o timeout |  |
| none | nft-drop | select-stream | native | 1.7s | 1x | read data: failed to decode block from 127.0.0.1:28127 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 25849: rea… |  |
| none | nft-drop | select-stream | std | 1.7s | 1x | read data: failed to decode block from 127.0.0.1:28193 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 40002: rea… |  |
| none | nft-drop-in | insert-big | native | **HUNG >45s** | 23x | socket Write blocked: clickhouse-go sets a read deadline only, no write deadline | returned 7.4s after heal: <nil> |
| none | nft-drop-in | insert-big | std | **HUNG >45s** | 23x | socket Write blocked: clickhouse-go sets a read deadline only, no write deadline | returned 7.6s after heal: <nil> |
| none | nft-drop-in | ping-idle | native | 2.0s | 1x | ping: failed to read packet from 127.0.0.1:28130 (conn_id=1, age=2s): read: read tcp 127.0.0.1:43122->127.0.0.1:28130: i/o timeout |  |
| none | nft-drop-in | ping-idle | std | 2.0s | 1x | ping: failed to read packet from 127.0.0.1:28196 (conn_id=1, age=2s): read: read tcp 127.0.0.1:50732->127.0.0.1:28196: i/o timeout |  |
| none | nft-drop-in | ping-new | native | 2.0s | 1x | dial tcp 127.0.0.1:28129: i/o timeout |  |
| none | nft-drop-in | ping-new | std | 2.0s | 1x | dial tcp 127.0.0.1:28195: i/o timeout |  |
| none | nft-drop-in | select-stream | native | 1.7s | 1x | read data: failed to decode block from 127.0.0.1:28131 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 16489: rea… |  |
| none | nft-drop-in | select-stream | std | 1.8s | 1x | read data: failed to decode block from 127.0.0.1:28197 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 32012: rea… |  |
| none | none | insert-big | native | 0.3s | 0x | ok (no error) |  |
| none | none | insert-big | std | 0.1s | 0x | ok (no error) |  |
| none | none | ping-idle | native | 0.0s | 0x | ok (no error) |  |
| none | none | ping-idle | std | 0.0s | 0x | ok (no error) |  |
| none | none | ping-new | native | 0.0s | 0x | ok (no error) |  |
| none | none | ping-new | std | 0.0s | 0x | ok (no error) |  |
| none | none | select-slow-body | native | 2.0s | 1x | query processing: failed to read packet from 127.0.0.1:28104 (conn_id=1): read: read tcp 127.0.0.1:38142->127.0.0.1:28104: i/o timeout |  |
| none | none | select-slow-body | std | 2.0s | 1x | query processing: failed to read packet from 127.0.0.1:28169 (conn_id=1): read: read tcp 127.0.0.1:54766->127.0.0.1:28169: i/o timeout |  |
| none | none | select-slow-headers | native | 2.0s | 1x | query processing: failed to read packet from 127.0.0.1:28103 (conn_id=1): read: read tcp 127.0.0.1:39290->127.0.0.1:28103: i/o timeout |  |
| none | none | select-slow-headers | std | 2.0s | 1x | query processing: failed to read packet from 127.0.0.1:28168 (conn_id=1): read: read tcp 127.0.0.1:37726->127.0.0.1:28168: i/o timeout |  |
| none | toxi-bandwidth | insert-big | native | **HUNG >45s** | 23x | socket Write blocked: clickhouse-go sets a read deadline only, no write deadline | returned 0.0s after heal: <nil> |
| none | toxi-bandwidth | insert-big | std | **HUNG >45s** | 23x | socket Write blocked: clickhouse-go sets a read deadline only, no write deadline | returned 0.0s after heal: <nil> |
| none | toxi-bandwidth | ping-idle | native | 0.0s | 0x | ok (no error) |  |
| none | toxi-bandwidth | ping-idle | std | 0.0s | 0x | ok (no error) |  |
| none | toxi-bandwidth | ping-new | native | 0.1s | 0x | ok (no error) |  |
| none | toxi-bandwidth | ping-new | std | 0.1s | 0x | ok (no error) |  |
| none | toxi-bandwidth | select-stream | native | 1.5s | 1x | read data: failed to decode block from 127.0.0.1:28114 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 5136: read… |  |
| none | toxi-bandwidth | select-stream | std | 1.8s | 1x | read data: failed to decode block from 127.0.0.1:28181 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 17378: rea… |  |
| none | toxi-down | insert-big | native | 0.1s | 0x | send data: failed to flush partial block (conn_id=1, col=0): write: write tcp 127.0.0.1:47278->127.0.0.1:28124: write: broken pipe |  |
| none | toxi-down | insert-big | std | 0.1s | 0x | driver: bad connection |  |
| none | toxi-down | ping-idle | native | 0.2s | 0x | handshake: failed to read packet from 127.0.0.1:28122 (conn_id=2, auth_db=default): read: read tcp 127.0.0.1:52238->127.0.0.1:28122: read: c… |  |
| none | toxi-down | ping-idle | std | 0.2s | 0x | handshake: failed to read packet from 127.0.0.1:28188 (conn_id=2, auth_db=default): read: read tcp 127.0.0.1:47640->127.0.0.1:28188: read: c… |  |
| none | toxi-down | ping-new | native | 0.2s | 0x | handshake: failed to read packet from 127.0.0.1:28121 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:36572->127.0.0.1:28121: read: c… |  |
| none | toxi-down | ping-new | std | 0.2s | 0x | handshake: failed to read packet from 127.0.0.1:28187 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:51410->127.0.0.1:28187: read: c… |  |
| none | toxi-down | select-stream | native | 0.0s | 0x | read data: failed to decode block from 127.0.0.1:28123 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 5362: read… |  |
| none | toxi-down | select-stream | std | 0.0s | 0x | read data: failed to decode block from 127.0.0.1:28189 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 22455: rea… |  |
| none | toxi-reset | insert-big | native | 0.1s | 0x | send data: failed to flush partial block (conn_id=1, col=0): write: write tcp 127.0.0.1:57998->127.0.0.1:28120: write: broken pipe |  |
| none | toxi-reset | insert-big | std | 0.1s | 0x | driver: bad connection |  |
| none | toxi-reset | ping-idle | native | 0.0s | 0x | ping: failed to read packet from 127.0.0.1:28118 (conn_id=1, age=0s): read: EOF |  |
| none | toxi-reset | ping-idle | std | 0.0s | 0x | ping: failed to read packet from 127.0.0.1:28184 (conn_id=1, age=0s): read: EOF |  |
| none | toxi-reset | ping-new | native | 0.0s | 0x | handshake: failed to read packet from 127.0.0.1:28117 (conn_id=1, auth_db=default): read: EOF |  |
| none | toxi-reset | ping-new | std | 0.0s | 0x | handshake: failed to read packet from 127.0.0.1:28183 (conn_id=1, auth_db=default): read: EOF |  |
| none | toxi-reset | select-stream | native | 0.0s | 0x | read data: failed to decode block from 127.0.0.1:28119 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 5618: read… |  |
| none | toxi-reset | select-stream | std | 0.0s | 0x | read data: failed to decode block from 127.0.0.1:28185 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 18125: rea… |  |
| none | toxi-timeout | insert-big | native | 2.2s | 1x | query processing: failed to read packet from 127.0.0.1:28105 (conn_id=1): read: read tcp 127.0.0.1:43988->127.0.0.1:28105: i/o timeout |  |
| none | toxi-timeout | insert-big | std | 2.1s | 1x | driver: bad connection |  |
| none | toxi-timeout | ping-idle | native | 2.0s | 1x | ping: failed to read packet from 127.0.0.1:28109 (conn_id=1, age=2s): read: read tcp 127.0.0.1:47752->127.0.0.1:28109: i/o timeout |  |
| none | toxi-timeout | ping-idle | std | 2.0s | 1x | ping: failed to read packet from 127.0.0.1:28172 (conn_id=1, age=2s): read: read tcp 127.0.0.1:35248->127.0.0.1:28172: i/o timeout |  |
| none | toxi-timeout | ping-new | native | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28110 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:41578->127.0.0.1:28110: i/o tim… |  |
| none | toxi-timeout | ping-new | std | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28171 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:50598->127.0.0.1:28171: i/o tim… |  |
| none | toxi-timeout | select-stream | native | 1.5s | 1x | read data: failed to decode block from 127.0.0.1:28111 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 5991: read… |  |
| none | toxi-timeout | select-stream | std | 1.8s | 1x | read data: failed to decode block from 127.0.0.1:28173 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 14820: rea… |  |
| none | toxi-timeout-down | insert-big | native | 2.3s | 1x | query processing: failed to read packet from 127.0.0.1:28112 (conn_id=1): read: read tcp 127.0.0.1:41814->127.0.0.1:28112: i/o timeout |  |
| none | toxi-timeout-down | insert-big | std | 2.1s | 1x | driver: bad connection |  |
| none | toxi-timeout-down | ping-idle | native | 2.0s | 1x | ping: failed to read packet from 127.0.0.1:28106 (conn_id=1, age=2s): read: read tcp 127.0.0.1:35484->127.0.0.1:28106: i/o timeout |  |
| none | toxi-timeout-down | ping-idle | std | 2.0s | 1x | ping: failed to read packet from 127.0.0.1:28176 (conn_id=1, age=2s): read: read tcp 127.0.0.1:32890->127.0.0.1:28176: i/o timeout |  |
| none | toxi-timeout-down | ping-new | native | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28113 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:55528->127.0.0.1:28113: i/o tim… |  |
| none | toxi-timeout-down | ping-new | std | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28175 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:56194->127.0.0.1:28175: i/o tim… |  |
| none | toxi-timeout-down | select-stream | native | 1.5s | 1x | read data: failed to decode block from 127.0.0.1:28108 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 5835: read… |  |
| none | toxi-timeout-down | select-stream | std | 1.8s | 1x | read data: failed to decode block from 127.0.0.1:28177 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 20561: rea… |  |
| deadline | ch-pause | insert-big | native | 4.8s | 2x | context deadline exceeded |  |
| deadline | ch-pause | insert-big | std | 4.8s | 2x | context deadline exceeded |  |
| deadline | ch-pause | ping-idle | native | 5.0s | 3x | ping: failed to read packet from 127.0.0.1:28237 (conn_id=1, age=5s): read: read tcp 127.0.0.1:34524->127.0.0.1:28237: i/o timeout |  |
| deadline | ch-pause | ping-idle | std | 5.0s | 3x | ping: failed to read packet from 127.0.0.1:28245 (conn_id=1, age=5s): read: read tcp 127.0.0.1:37778->127.0.0.1:28245: i/o timeout |  |
| deadline | ch-pause | ping-new | native | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28236 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:42688->127.0.0.1:28236: i/o tim… |  |
| deadline | ch-pause | ping-new | std | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28244 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:40138->127.0.0.1:28244: i/o tim… |  |
| deadline | ch-pause | select-stream | native | 4.7s | 2x | context deadline exceeded |  |
| deadline | ch-pause | select-stream | std | 4.7s | 2x | context deadline exceeded |  |
| deadline | nft-drop | insert-big | native | 4.9s | 2x | context deadline exceeded |  |
| deadline | nft-drop | insert-big | std | 4.9s | 2x | context deadline exceeded |  |
| deadline | nft-drop | ping-idle | native | 5.0s | 3x | ping: failed to read packet from 127.0.0.1:28159 (conn_id=1, age=5s): read: read tcp 127.0.0.1:36504->127.0.0.1:28159: i/o timeout |  |
| deadline | nft-drop | ping-idle | std | 5.0s | 3x | ping: failed to read packet from 127.0.0.1:28225 (conn_id=1, age=5s): read: read tcp 127.0.0.1:53940->127.0.0.1:28225: i/o timeout |  |
| deadline | nft-drop | ping-new | native | 2.0s | 1x | dial tcp 127.0.0.1:28158: i/o timeout |  |
| deadline | nft-drop | ping-new | std | 2.0s | 1x | dial tcp 127.0.0.1:28224: i/o timeout |  |
| deadline | nft-drop | select-stream | native | 4.8s | 2x | context deadline exceeded |  |
| deadline | nft-drop | select-stream | std | 4.8s | 2x | context deadline exceeded |  |
| deadline | nft-drop-in | insert-big | native | 4.9s | 2x | context deadline exceeded |  |
| deadline | nft-drop-in | insert-big | std | 4.9s | 2x | context deadline exceeded |  |
| deadline | nft-drop-in | ping-idle | native | 5.0s | 3x | ping: failed to read packet from 127.0.0.1:28163 (conn_id=1, age=5s): read: read tcp 127.0.0.1:50336->127.0.0.1:28163: i/o timeout |  |
| deadline | nft-drop-in | ping-idle | std | 5.0s | 3x | ping: failed to read packet from 127.0.0.1:28229 (conn_id=1, age=5s): read: read tcp 127.0.0.1:53988->127.0.0.1:28229: i/o timeout |  |
| deadline | nft-drop-in | ping-new | native | 2.0s | 1x | dial tcp 127.0.0.1:28162: i/o timeout |  |
| deadline | nft-drop-in | ping-new | std | 2.0s | 1x | dial tcp 127.0.0.1:28228: i/o timeout |  |
| deadline | nft-drop-in | select-stream | native | 4.8s | 2x | context deadline exceeded |  |
| deadline | nft-drop-in | select-stream | std | 4.8s | 2x | context deadline exceeded |  |
| deadline | none | insert-big | native | 0.1s | 0x | ok (no error) |  |
| deadline | none | insert-big | std | 0.1s | 0x | ok (no error) |  |
| deadline | none | ping-idle | native | 0.0s | 0x | ok (no error) |  |
| deadline | none | ping-idle | std | 0.0s | 0x | ok (no error) |  |
| deadline | none | ping-new | native | 0.0s | 0x | ok (no error) |  |
| deadline | none | ping-new | std | 0.0s | 0x | ok (no error) |  |
| deadline | none | select-slow-body | native | 5.0s | 3x | context deadline exceeded |  |
| deadline | none | select-slow-body | std | 5.0s | 3x | context deadline exceeded |  |
| deadline | none | select-slow-headers | native | 3.0s | 2x | ok (no error) |  |
| deadline | none | select-slow-headers | std | 3.0s | 2x | ok (no error) |  |
| deadline | toxi-bandwidth | insert-big | native | 4.9s | 2x | context deadline exceeded |  |
| deadline | toxi-bandwidth | insert-big | std | 4.9s | 2x | context deadline exceeded |  |
| deadline | toxi-bandwidth | ping-idle | native | 0.0s | 0x | ok (no error) |  |
| deadline | toxi-bandwidth | ping-idle | std | 0.0s | 0x | ok (no error) |  |
| deadline | toxi-bandwidth | ping-new | native | 0.1s | 0x | ok (no error) |  |
| deadline | toxi-bandwidth | ping-new | std | 0.1s | 0x | ok (no error) |  |
| deadline | toxi-bandwidth | select-stream | native | 4.8s | 2x | context deadline exceeded |  |
| deadline | toxi-bandwidth | select-stream | std | 4.8s | 2x | context deadline exceeded |  |
| deadline | toxi-down | insert-big | native | 0.1s | 0x | send data: failed to flush partial block (conn_id=1, col=0): write: write tcp 127.0.0.1:46662->127.0.0.1:28157: write: broken pipe |  |
| deadline | toxi-down | insert-big | std | 0.1s | 0x | driver: bad connection |  |
| deadline | toxi-down | ping-idle | native | 0.2s | 0x | handshake: failed to read packet from 127.0.0.1:28155 (conn_id=2, auth_db=default): read: read tcp 127.0.0.1:41786->127.0.0.1:28155: read: c… |  |
| deadline | toxi-down | ping-idle | std | 0.2s | 0x | handshake: failed to read packet from 127.0.0.1:28221 (conn_id=2, auth_db=default): read: read tcp 127.0.0.1:36636->127.0.0.1:28221: read: c… |  |
| deadline | toxi-down | ping-new | native | 0.2s | 0x | handshake: failed to read packet from 127.0.0.1:28154 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:51948->127.0.0.1:28154: read: c… |  |
| deadline | toxi-down | ping-new | std | 0.2s | 0x | handshake: failed to read packet from 127.0.0.1:28220 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:58166->127.0.0.1:28220: read: c… |  |
| deadline | toxi-down | select-stream | native | 0.0s | 0x | read data: failed to decode block from 127.0.0.1:28156 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 5071: read… |  |
| deadline | toxi-down | select-stream | std | 0.0s | 0x | read data: failed to decode block from 127.0.0.1:28222 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 19461: rea… |  |
| deadline | toxi-reset | insert-big | native | 0.0s | 0x | send data: failed to flush partial block (conn_id=1, col=0): write: write tcp 127.0.0.1:50600->127.0.0.1:28153: write: broken pipe |  |
| deadline | toxi-reset | insert-big | std | 0.1s | 0x | driver: bad connection |  |
| deadline | toxi-reset | ping-idle | native | 0.0s | 0x | ping: failed to read packet from 127.0.0.1:28151 (conn_id=1, age=0s): read: EOF |  |
| deadline | toxi-reset | ping-idle | std | 0.0s | 0x | ping: failed to read packet from 127.0.0.1:28217 (conn_id=1, age=0s): read: EOF |  |
| deadline | toxi-reset | ping-new | native | 0.0s | 0x | handshake: failed to read packet from 127.0.0.1:28150 (conn_id=1, auth_db=default): read: EOF |  |
| deadline | toxi-reset | ping-new | std | 0.0s | 0x | handshake: failed to read packet from 127.0.0.1:28216 (conn_id=1, auth_db=default): read: EOF |  |
| deadline | toxi-reset | select-stream | native | 0.0s | 0x | read data: failed to decode block from 127.0.0.1:28152 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 6305: read… |  |
| deadline | toxi-reset | select-stream | std | 0.0s | 0x | read data: failed to decode block from 127.0.0.1:28218 (conn_id=1, compression=none): clickhouse [Decode]: randomString(1024) row 19885: rea… |  |
| deadline | toxi-timeout | insert-big | native | 4.9s | 2x | context deadline exceeded |  |
| deadline | toxi-timeout | insert-big | std | 4.9s | 2x | context deadline exceeded |  |
| deadline | toxi-timeout | ping-idle | native | 5.0s | 3x | ping: failed to read packet from 127.0.0.1:28139 (conn_id=1, age=5s): read: read tcp 127.0.0.1:33760->127.0.0.1:28139: i/o timeout |  |
| deadline | toxi-timeout | ping-idle | std | 5.0s | 3x | ping: failed to read packet from 127.0.0.1:28205 (conn_id=1, age=5s): read: read tcp 127.0.0.1:48970->127.0.0.1:28205: i/o timeout |  |
| deadline | toxi-timeout | ping-new | native | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28138 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:37570->127.0.0.1:28138: i/o tim… |  |
| deadline | toxi-timeout | ping-new | std | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28204 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:45288->127.0.0.1:28204: i/o tim… |  |
| deadline | toxi-timeout | select-stream | native | 4.8s | 2x | context deadline exceeded |  |
| deadline | toxi-timeout | select-stream | std | 4.8s | 2x | context deadline exceeded |  |
| deadline | toxi-timeout-down | insert-big | native | 4.9s | 2x | context deadline exceeded |  |
| deadline | toxi-timeout-down | insert-big | std | 4.9s | 2x | context deadline exceeded |  |
| deadline | toxi-timeout-down | ping-idle | native | 5.0s | 3x | ping: failed to read packet from 127.0.0.1:28143 (conn_id=1, age=5s): read: read tcp 127.0.0.1:59838->127.0.0.1:28143: i/o timeout |  |
| deadline | toxi-timeout-down | ping-idle | std | 5.0s | 3x | ping: failed to read packet from 127.0.0.1:28209 (conn_id=1, age=5s): read: read tcp 127.0.0.1:40746->127.0.0.1:28209: i/o timeout |  |
| deadline | toxi-timeout-down | ping-new | native | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28142 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:44670->127.0.0.1:28142: i/o tim… |  |
| deadline | toxi-timeout-down | ping-new | std | 2.0s | 1x | handshake: failed to read packet from 127.0.0.1:28208 (conn_id=1, auth_db=default): read: read tcp 127.0.0.1:39622->127.0.0.1:28208: i/o tim… |  |
| deadline | toxi-timeout-down | select-stream | native | 4.8s | 2x | context deadline exceeded |  |
| deadline | toxi-timeout-down | select-stream | std | 4.8s | 2x | context deadline exceeded |  |
