# Squid → Amazon Kinesis Agent → Amazon Data Firehose → S3

既存の2AZ PrivateLink検証を残し、ログ収集の拡張版を `logging/` に追加します。
元の `01-provider-a.yaml` と `02-consumer-b.yaml` は変更しません。

## 構成と構築順序

| 順序 | テンプレート | アカウント | 内容 |
|---|---|---|---|
| 1 | `00-log-storage.json` | A | S3、配信用IAMロール、共通のDirect PUT Firehose 1本 |
| 2 | `01-provider-logging.json` | A | VPC・内部NLB・Squid EC2×2・Endpoint Service、2AZのFirehose用Endpoint、Kinesis Agent設定 |
| 3 | `02-consumer-logging.json` | B | 外部直接経路のないVPC、2AZ Interface Endpoint、有限のHTTPSリクエストを生成するLambda |
| 4 | AでEndpoint接続要求を承認 | A | BのEndpoint IDを照合して承認 |

A/Bは別アカウント、リージョンは `ap-southeast-2`。物理AZ IDは両側で一致させます。
ログ経路は各EC2のKinesis Agent → A側Firehose用Interface Endpoint → Firehose → S3です。
Firehose用Endpointは2AZにENIを作成し、Private DNSを有効にします。通常のFirehoseサービスURLはEndpointのプライベートIPへ解決されます。
Squidから外部サイトへの通信には公開IPv4とIGWを使います。NAT Gateway、Kinesis Data Streams、変換Lambda、動的パーティショニングは作成しません。
B側のプロキシ用Endpoint（3128）と、A側のFirehose用Endpoint（443）は別のリソースです。

## Firehose用Endpointの確認

`01-provider-logging.json`の出力`FirehoseEndpointId`がA側のログ送信用Endpointです。
`02-consumer-logging.json`の`ConsumerEndpointId`はB側のプロキシ接続用で、用途が異なります。
Firehose用Endpointの状態が`available`、`PrivateDnsEnabled=true`、ENIが2AZにあることを確認します。
EC2内で次を実行し、解決先がFirehose用Endpoint ENIのプライベートIPと一致することを確認します。

```bash
# EC2からFirehoseの通常のサービス名を解決する
getent ahostsv4 firehose.ap-southeast-2.amazonaws.com
```

通常のサービス名を使う場合、常に同一AZのENIが選ばれる保証はありません。
VPC内DNSサポートとDNSホスト名はテンプレートで有効にしています。

## 導入と認証

EC2 UserDataがAL2023の公式リポジトリから Corretto 17と `dnf -y install squid aws-kinesis-agent jq` を実行し、
Squid設定・Agent設定・ログの読み取り権限・起動まで自動化します。
EC2はIMDSv2必須。アクセスキーは書かず、インスタンスロールに対象ストリームの `firehose:PutRecordBatch` を追加し、`aws:SourceVpce` で今回のFirehose用Endpoint経由に限定します。
Endpointポリシーも同じロール・ストリーム・操作だけを許可し、Endpoint SGはSquid EC2 SGからのTCP 443だけを受け付けます。
ポリシーがこの状態なら、公開APIへ名前解決された場合には送信権限が成立しません。
管理用の既存SSM権限と、UserData成功通知用の `cloudformation:SignalResource` も使います。
SSHは開放せず、SquidはNLB SGからの3128番ポートだけ受け付けます。
Squidの接続先許可は `checkip.amazonaws.com:443` のCONNECTだけです。

## 主な設定

| 設定 | 初期値 | 意味 |
|---|---|---|
| `cloudwatch.emitMetrics` | `false` | Agent独自の有料カスタムメトリクスを発行しない |
| `aggregatedRecordSizeBytes` | `0` | 0は集約なし。比較では5120、51200も使用 |
| `maxBufferAgeMillis` | `60000` | 作成済みレコードを送信バッファに保持する最大時間 |
| `minTimeBetweenFilePollsMillis` | `100` | ファイルを読む間隔。集約待機時間ではない |
| `initialPosition` | `START_OF_FILE` | 初回に既存行も読む。再起動時の既存チェックポイントを消さない |
| `filePattern` | `/var/log/squid/access.log*` | 非圧縮のローテーション済みファイルも監視 |
| Firehoseバッファ | 60秒・5MiB | S3配信のヒント。Agent内の集約とは別 |

`aggregatedRecordSizeBytes` は「指定サイズに達するまで後続の行を待つ」設定ではありません。
パーサーが読み込んだ範囲にある完全な行を、サイズの目安に従って連結します。
`maxBufferAgeMillis=60000` でも「60秒分のログが1レコードになる」とは限りません。
`PutRecordBatch` の複数レコードを1回のAPI要求に載せる動作と、複数行を1レコードにする集約も別です。

ログには `source=az1/az2`、`instance=...`、テスト通信の `request=...` を付けます。
HTTPSのURLパスはCONNECTの中に隠れるため、検証用のIDはCONNECTヘッダー `X-Lab-Request` に付けてSquidで記録します。
リクエスト生成LambdaはFirehose SDKを使用しません。
NLBのTCPヘルスチェックが `NONE_NONE/000` として記録されたため、この検証では `access_log ... agentlab CONNECT` としてCONNECT通信を収集対象にしています。
初回構築時に記録されたヘルスチェック行も原本として保持し、比較区間からは除いています。

## Windows PowerShellでの操作例

AWS CLIの認証済みプロファイルを `provider` / `consumer` として用意します。
既存のプロファイル名を使う場合は `--a-profile` / `--b-profile` を変更してください。

```powershell
# 検証用の管理・照合ライブラリをインストールする（ログ送信処理ではない）
python -m pip install -r logging/requirements.txt

# 認証と既存スタックを読み取り確認する。A/Bが同じなら中断する
python logging/labctl.py preflight

# ログ保存先を作成する
python logging/labctl.py storage

# 作成状況とエラーを確認する。CREATE_COMPLETEまで次の作成へ進まない
python logging/labctl.py status

# 先に完成した保存先を使ってA側を作成する
python logging/labctl.py provider

# A側がCREATE_COMPLETEになったらB側を作成する
python logging/labctl.py consumer

# B側のEndpointが承認待ちになったらA側で接続を承認する
python logging/labctl.py accept

# SSMで両EC2のバージョン・設定・サービス状態を確認する
python logging/labctl.py inspect --run initial

# 両AZへ少量ずつ実際の通信を行い、ログを生成する
python logging/labctl.py generate --route az1 --count 5 --run smoke-az1
python logging/labctl.py generate --route az2 --count 5 --run smoke-az2

# AgentとS3の配信完了を確認して原本・設定・Agentログを保存する
python logging/labctl.py collect --run smoke

# EC2原本とS3原本を、改行を含む行の出現回数で照合する
python logging/verify_logs.py --snapshot smoke
```

CloudFormationは失敗時の自動ロールバックを無効にし、終了保護を有効にしています。
作成済みの同名スタックは自動更新・削除・再構築しません。
`consumer` の作成は承認待ちでしばらく `CREATE_IN_PROGRESS` になります。別操作で `accept` を実行します。
次の通信前にEndpointが `available`、NLBターゲット2台が `healthy` であることを確認します。

## 比較と継続収集の検証

各方式は同じ台数・行数・送信間隔で比較します。最初は少量の疎通だけを行います。

1. 集約0/5120/51200で、Agentを稼働させたまま各EC2へ同じ数の通信を行う。
2. 必要に応じてAgent停止中に同じ行数を蓄積し、開始後にどう集約されるかを別の実験として確認する。
3. モードを変える前に未送信がなくなるまで確認し、チェックポイントを保持して変更する。
4. 各方式の計測区間を1分境界で区切り、前方式の送信が混ざらない間隔を空ける。
5. `AWS/Firehose` のIncomingRecords/IncomingBytes（Sum）と、Agentログ、EC2/S3原本を照合する。
6. ローテーション前後、Agent再起動前後、一時的な送信失敗と復旧後も、同じ方法で欠損・重複を確認する。

```powershell
# 集約サイズだけを変更する。送信待機60秒とチェックポイントは維持する
python logging/labctl.py mode --size 5120 --poll 100 --run aggregate-5k

# 計測区間を明示して標準メトリクスを保存する（時刻は実験区間に置き換える）
python logging/labctl.py metrics --start <UTC_START> --end <UTC_END> --run aggregate-5k
```

SSM Documentもテンプレート内に作成します。AWS Systems ManagerのRun Commandで、
出力 `SetAgentModeDocument`、`AggregateBytes=0/5120/51200`、`PollMillis=100/1000/5000` を選べます。
変更のためにCloudFormationのUserDataを更新してEC2を再作成する必要はありません。

`logrotate` はrename＋新規作成＋Squidの再オープンを使い、`copytruncate` は使いません。
`logfile_rotate 0` でSquid自身による二重の連番ローテーションを抑えます。
グループ読み取り権限は新規ファイルにも適用します。圧縮せず、検証中は旧ログを自動削除しません。
再送・プロセス異常終了の条件によっては重複し得るため、通常再起動で一度重複がなかっただけでexactly-onceとは説明しません。

## 上限・費用・保持

検証全体を最大1000 HTTPアクセス・Squidログ合計10MiB以内に制限します。
`generate` は実行前に予算を予約し、応答が不明なリクエストを自動再送しません。
同じrun IDの再実行も拒否します。ログサイズ上限は各ホスト5MiB（合計10MiB）として確認します。
100万行の検証は実施しません。

2026-11-15、シドニーのAWS料金APIによる目安（USD、税・割引・無料枠適用前）：

| リソース | 単価・台数 | 時間当たりの目安 |
|---|---|---|
| EC2 Linux t3.micro | $0.0132 ×2 | $0.0264 |
| 内部NLB | $0.0252 ×1 | $0.0252＋使用NLCU分 |
| B側Interface Endpoint | $0.013/AZ ×2 | $0.026 |
| A側Firehose用Interface Endpoint | $0.013/AZ ×2 | $0.026 |
| A側パブリックIPv4 | $0.005 ×2 | $0.010 |
| EBS gp3 | $0.096/GB月 ×16GB | 約$0.0021（730時間換算） |
| 固定部分の合計 | 上記の合計 | 約$0.1157/時、約$2.78/日 |

このほかFirehose Direct PUT、S3保存・リクエスト、Lambda、CloudWatch Logs、
データ処理・転送などが従量で発生します。Agent自体のライセンス料金はありません。
Kinesis Agent独自のCloudWatchメトリクス、カスタムダッシュボード、アラームは使いません。
Firehose用Endpointには上記の固定料金に加えてデータ処理料金が発生します。
標準メトリクスの取得APIにも条件によって料金がかかるため、必要な区間だけ取得します。

Firehose課金対象量は各入力レコードの実サイズを5120B単位で切り上げて合計します。
`IncomingBytes` の合計を最後に一回だけ切り上げる計算は使いません。
レコード境界を実測できない方式では、平均サイズから正確な課金対象量を断定しません。
Cost Explorerへの反映前の計算値は請求実測ではありません。

**検証完了・記事完成・請求反映を削除許可とみなさず、明示的な削除指示まで全リソースとログを保持します。**
S3にはライフサイクル削除を設定せず、バージョニングとRetainを設定します。
Agentログの既定の7日削除も、この検証用設定では使用しません。
EC2、NLB、Endpointなどは保持中も課金が続きます。
このリポジトリに自動削除スクリプトは含めません。

## 証跡と公開前の確認

`private-evidence/` はGit管理対象外。S3原本、EC2原本、API応答、実行記録、
設定、メトリクス、ハッシュ、画面キャプチャを保持します。
S3/ホストの原本は書き換えず、公開用の画像・説明と分けます。
`collect` は既存のバケット内へホスト原本を1ファイルずつ保存するための期限付きPUT URLを使用します。
そのURLは表示せず、公開コードに認証情報や実アカウントIDを埋め込みません。

API応答のJSON・スクリプト出力と、実際の設定画面キャプチャは別の証跡です。
記事用画像は画面で実物を撮影し、アカウントID、利用者名、不要なIPなどを除いて確認します。
初回検証・追加検証・推測・構成の再現図を区別して記載します。
新記事は下書きに留め、確認後の公開指示を待ちます。

## Firehose用Interface Endpoint経由での再検証（2026-11-15 JST）

配布する3テンプレートから、`squidvpce-store`・`squidvpce-a`・`squidvpce-b`を新規作成しました。
3スタックとも`CREATE_COMPLETE`。デプロイ済みテンプレートと配布ファイルの一致も確認しています。
UserDataの設定修正や補完スタックは使わず、2台ともSquid・Kinesis Agentがactive・enabledになりました。
Endpointの接続承認、集約モード変更、ローテーションなど、通常の操作・検証はSSM/APIで行います。

Firehose用Endpointは2AZ・Private DNS有効。両EC2の通常のFirehoseサービス名の解決先は、Endpoint ENIのプライベートIPと一致しました。
Endpoint SGはSquid EC2 SGからのTCP 443のみ、Endpointポリシーは該当ロール・ストリームのPutRecordBatchのみです。
EC2ロールの送信権限は`aws:SourceVpce`で限定しています。配布した送信ポリシー単体のIAMシミュレーションでは、Endpoint条件ありでallowed、なしでimplicitDenyでした。
このシミュレーションと別に、実際のKinesis Agent送信成功記録・S3原本も照合しています。

最初の疎通は各AZ3件、計6件すべてHTTP 200。EC2/S3は6行・1,137 Bで一致しました。
集約比較6条件は各120アクセス、継続収集はローテーション・正常再起動・一時送信失敗の3条件を各20アクセスで確認しました。
最終的にEC2/S3は**786行・146,012 B**で一致し、欠損0・余分な行0、成功ID786件すべてが1回ずつ到着しました。
S3ログオブジェクトは10個。AZ別の行数は{"az1": 393, "az2": 393}です。

| 条件 | 集約B | IncomingRecords | IncomingBytes | 計算上の課金対象B |
|---|---:|---:|---:|---:|
| 通常追記 | 0 | 120 | 22,240 | 614,400 |
| 通常追記 | 5,120 | 120 | 22,240 | 614,400 |
| 通常追記 | 51,200 | 120 | 22,240 | 614,400 |
| 蓄積後 | 0 | 120 | 22,180 | 614,400 |
| 蓄積後 | 5,120 | 6 | 22,240 | 30,720 |
| 蓄積後 | 51,200 | 2 | 22,300 | 30,720 |

全条件で原本のバイト数、Kinesis Agentの送信成功レコード数・バイト数、CloudWatch Sumを照合しています。
課金対象量は各レコードを5,120 B単位で切り上げた計算値で、請求実測ではありません。
テンプレート・API応答・実行記録・EC2/S3原本・照合結果は`private-evidence/vpce-20261115/`に保存しています。
検証後、今回作成したEC2 2台は停止しました。スタック・NLB・Endpoint・S3・ログは保持しています。既存環境は変更していません。

## 過去の初回構築で使った補完構成（Endpoint追加前）

AL2023リポジトリのAgent 2.0.14のRPMにはSysV形式の起動スクリプトが含まれていました。
初回のUserDataがサービス有効化で失敗したため、`squidlog-a` は `CREATE_FAILED` の状態で保持しています。
2台のEC2はSSMでネイティブsystemd unitへ修正し、Corretto 17で実行しています。
単純なCloudFormationの再試行はEC2置換になると変更セットで分かったため、実行していません。

再作成を避けるため、既存VPC・NLB・EC2 IDをパラメーターに渡す `01b-existing-provider-link.json` を
`squidlog-a-link` として作成し、まだ作られていなかったTG・Listener・Endpoint Service・接続許可を補完しました。
この補完スタックはEC2を新規作成しません。
検証時の管理コマンドには `--completion-stack squidlog-a-link` を指定しています。
元のスタックを成功状態として扱うのではなく、この明示オプションで物理リソースIDと補完スタックの出力を読みます。

配布する `01-provider-logging.json` のUserDataには、ネイティブunit、永続チェックポイント、
Corretto 17、CONNECTログのみの収集を反映しています。
初回検証時点では修正版による新規構築は未実施でしたが、追加検証で環境全体の新規作成と実ログのS3到着まで確認しました。詳細は次節に記録しています。
未実行の `native-agent-service` 変更セットはEC2置換を含むため、実行しないでください。

通常の新規構築は00→01→02、今回の保持環境を操作するときは補完スタック付きのコマンドを使います。

## 過去の修正版テンプレート確認（Endpoint追加前、2026-11-15 JST）

配布テンプレートのGitリビジョン `7bfad4c` の3ファイルを変更せず、初回環境とは別名のスタックで新規作成しました。
00→01→02の順に作成し、B側Endpointの接続要求をA側で承認しています。
既存リソースの流用、SSMによる設定修正、`01b-existing-provider-link.json`による補完は行っていません。

| スタック | 内容 | 結果 |
|---|---|---|
| `squidfresh-store`（A） | S3・Firehose | `CREATE_COMPLETE` |
| `squidfresh-a`（A） | VPC・2AZ EC2・NLB・Endpoint Serviceなど | `CREATE_COMPLETE` |
| `squidfresh-b`（B） | VPC・2AZ Endpoint・Lambdaなど | `CREATE_COMPLETE` |

2台のEC2はUserDataの処理だけで初期設定を完了し、CloudFormationがSUCCESSシグナルを受信しました。
読み取りによる確認では、cloud-initはdone、Squid・Kinesis Agentはともにactive・enabled、
Kinesis Agentはネイティブsystemd unitを使用し、NRestarts=0・ExecMainStatus=0でした。

B側Lambdaから各AZへ3回ずつ、計6回の実通信を行い、すべてHTTP 200で成功しました。
EC2原本とS3原本はともに6行・1,134 B、S3ログオブジェクトは1個でした。
欠損0・余分な行0、全行LF、6件のリクエストIDはすべて1回ずつ到着しました。

これは修正版からの新規構築を確認するための追加検証です。
初回の802行・集約6条件・ローテーション等の結果とは分けて記録しています。
テンプレートのSHA-256、全スタックの状態とイベント、SSM応答、Lambda応答、EC2/S3原本、
照合結果と画面キャプチャは`private-evidence/clean-20261115/`に保持しています。
検証後は新規EC2も停止し、初回分を含む4台すべてがstoppedであることを確認しました。
その後、ユーザーの明示指示に基づき、新規構築確認用の`squidfresh-store`・`squidfresh-a`・`squidfresh-b`と、保持設定で残ったS3バケット・CloudWatchログを削除しました。削除前にS3全1バージョン（1,134 B）とLambdaログ7イベントを保存し、原本とのSHA-256一致を再確認しています。3スタックの`DELETE_COMPLETE`、新規のVPC・NLB・Endpoint・EC2・EBS・Firehose・S3・CloudWatchログの不在をAPIで確認しました。初回環境はEC2停止状態で保持し、初回環境に残るNLB・Endpoint・EBS等の課金は継続します。削除確認は`private-evidence/clean-20261115/cleanup/deletion-verification.json`に保存しています。
