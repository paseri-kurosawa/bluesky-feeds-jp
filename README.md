# Bluesky Feed JP

カスタム日本語フィード生成システムです。Blueskyのポストを処理して、2種類のフィードを提供します。

## 概要

このプロジェクトは以下の機能を提供します：

### 1. フィード生成（2種類）

#### Raw フィード
- スコア選定なしの純粋な日本語ポスト時系列
- 言語判定（fastText）のみで日本語を確認
- Denseフィードのベースデータ

#### Dense フィード
- Raw フィードから密度ベースのスコアリングで高品質ポストをキュレーション
- トークン分散度、単語ベクトルノルム、属性調整、バッドワード検出を組み合わせた多層フィルタリング
- 文字のみ（画像・動画なし）かつ15文字以下のポストは除外

### 2. スコアリングシステム（Dense フィード用）

スコアは複数の要素に基づいて計算されます：

- **トークン分散度チェック**: 語彙多様性で低品質テキストを検出
- **単語ベクトルノルム**: fastTextモデルを使用した意味的な密度測定
- **属性調整**:
  - リプライ: スコアを減少（返信は重要度低）
  - 画像: スコアを増加（画像付きは有用）
  - ハッシュタグ: ルールベースの調整（適切な量は有用）
- **バッドワード**: 見出し語ベースのマッチング（形態素解析Janome）、マッチ数に応じて指数関数的にスコア減少
  - バッドワード辞書は S3 (`badwords/dictionary.txt`) から読み込み
  - **定義**: 暴力的表現、差別・ヘイト、人格攻撃、嫌がらせ、ハラスメント、政治的スキャンダルなど、否定的な感情や害を想起させる語彙

## 統計ダッシュボード

リアルタイム処理統計を可視化するダッシュボードが利用可能です。

### アクセス
```
http://bluesky-feed-dashboard-878311109818.s3-website-ap-northeast-1.amazonaws.com
```

### 機能
- **Latest Report**: 最新バッチ実行分の統計
  - Processing Summary: Total Fetched、Invalid Fields、Moderation Labels、Non-Japanese、Spam Hashtags、Passed Filters、Badword Analysis、Dense Posts、Dense Rate
- **Processing Trends**: 過去の日次集計データから時系列グラフを表示
- **Distribution Charts**: 2つの円グラフで可視化
  - Filter Breakdown: Passed Filters / Moderation Labels / Non-Japanese の内訳
  - Dense Feed Ratio: Dense Posts / Other Posts の比率

### 技術スタック
- React SPA (Vite) + Chart.js
- S3静的ホスティング
- JSON形式の統計ファイルをブラウザで解析

### 統計データ構成
統計ログはS3に保存されます。ダッシュボードは自動更新されたコンポーネントJSONから最新データを取得・表示します。

統計更新は自動化されており、手動更新は不要です。

## アーキテクチャ

### AWS リソース

- **API Gateway**: HTTP APIエンドポイント
  - `/.well-known/did.json` - DID設定
  - `/xrpc/app.bsky.feed.describeFeedGenerator` - フィード説明
  - `/xrpc/app.bsky.feed.getFeedSkeleton` - フィードデータ
- **Lambda Functions**:
  - DID Handler: フィード識別情報提供
  - Describe Feed: フィードメタデータ提供（Raw/Dense フィード記述）
  - Get Feed: Valkeyキャッシュ参照で Raw/Dense フィード提供
  - Ingest: Bluesky API検索 → スコアリング → DataControlへ非同期呼び出し（EventBridge 10分ごと）
  - DataControl: スコアリング結果をValkeyに格納 + 統計JSON生成
- **Valkey Serverless**: キャッシュ層（ポストメタデータ: URI、タイムスタンプ、スコア等をメモリ内に保持）
- **S3**:
  - `bluesky-feed-badword-analysis-*`: バッドワードデータ
  - `bluesky-feed-dashboard-*`: ダッシュボード & 統計データ
    - `stats/`: バッチ統計ファイル
    - `components/`: ダッシュボード用コンポーネント JSON
    - `index.html`, `assets/`: React SPA ファイル
- **VPC**: Lambda関数間の通信と Valkey への安全な接続確保
- **CloudWatch Logs**: Lambda実行ログとメトリクス監視

## 開発・デプロイ

### 前提条件

- Node.js 18+
- AWS CDK
- Docker（Lambda コンテナイメージ構築用）
- Python 3.11（Lambda runtime）

### セットアップ

```bash
npm install
```

### デプロイ

```bash
cdk deploy --require-approval=never
```

### 環境変数

`.env` で以下を指定：

```
CDK_DEFAULT_ACCOUNT=<your-aws-account-id>
CDK_DEFAULT_REGION=<your-aws-region>
FEED_DID=did:web:<your-domain>
SERVICE_ENDPOINT=https://<your-domain>
VALKEY_ENDPOINT=<your-valkey-endpoint>
```

Bluesky クレデンシャル（`BSKY_HANDLE`、`BSKY_APP_PASSWORD`）は AWS Secrets Manager に保存します。

## ファイル構成

```
├── lib/
│   └── bluesky-feed-jp-stack.ts    # CDK Stack定義
├── lambda/
│   ├── handlers/                    # HTTP API ハンドラー
│   ├── ingest/                      # Ingest Lambda (ポスト検索・スコアリング)
│   │   ├── handler.py
│   │   ├── density_scorer.py        # スコアリングロジック・バッドワード検出
│   │   └── config.json              # スコアリング設定
│   ├── layers/redis/                # Redis Python層
│   └── ...
├── dashboard/                       # React SPA ダッシュボード
│   ├── src/
│   │   ├── App.jsx                  # メインアプリケーション
│   │   ├── components/              # グラフ・テーブルコンポーネント
│   │   └── ...
│   ├── index.html
│   ├── vite.config.js
│   └── package.json
├── scripts/
│   ├── delete_feeds.py              # フィード削除ツール
│   ├── publish_feeds.py             # フィード公開
│   └── ...
├── badwords/
│   └── dictionary.txt               # バッドワード辞書（1行1単語）
└── ...
```

## 参考資料

- [Bluesky Feed Generator API](https://github.com/bluesky-social/feed-generator)
- [fastText言語判定](https://fasttext.cc/)
- [Janome形態素解析](https://github.com/Kijikaqq/janome)
- [AWS CDK TypeScript](https://docs.aws.amazon.com/cdk/latest/guide/home.html)

## ライセンス

MIT
