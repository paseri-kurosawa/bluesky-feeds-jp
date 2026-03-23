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
  - ハッシュタグ: ルールベースの調整（多いほど影響大）
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
- **Latest Report**: 最新バッチ実行分の統計（Processing Summary、Badword Analysis、Dense Feed Statistics）
- **Processing Trends**: 過去の日次集計データから時系列グラフを表示（Total Fetched、Passed Filters、Dense Rate）
- **Distribution Charts**: 2つの円グラフで可視化
  - Filter Breakdown: Passed Filters / Moderation Labels / Non-Japanese の内訳
  - Dense Feed Ratio: Dense Posts / Other Posts の比率

### 技術スタック
- React SPA (Vite) + Chart.js
- S3静的ホスティング
- JSON形式の統計ファイルをブラウザで解析

### 統計データ構成
統計ログはS3 (`bluesky-feed-dashboard-878311109818/stats/`) に3段階で保存：

- `batch/stats_YYYYMMDD_HHMMSS.json`: 各実行分の生統計
- `daily/stats-YYYY-MM-DD.json`: 日次集計（複数バッチを加算集計）
- `summary/dashboard.json`: ダッシュボード用の統合ファイル（最新・日次データ両方含む）

### 統計インデックス更新
ファイルリストインデックスを手動で更新（オプション）：
```bash
./scripts/update-stats-index.sh
```

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
- **Valkey Serverless**: キャッシュ層（メモリ内スコア保持）
- **S3**:
  - `bluesky-feed-badword-analysis-*`: バッドワード辞書置き場
    - `badwords/dictionary.txt`: バッドワード辞書（無期限保持）
  - `bluesky-feed-dashboard-*`: ダッシュボード & 統計データ一元化
    - `stats/batch/`: 各実行の生統計JSON
    - `stats/daily/`: 日次集計JSON（複数バッチ加算）
    - `stats/summary/`: ダッシュボード用統合ファイル
    - `index.html`, `assets/`: React SPA ファイル
- **VPC**: Lambda間通信の分離・セキュリティ確保
- **CloudWatch Logs**: 実行ログとメトリクス

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

`.env` または環境設定で以下を指定：

```
BSKY_HANDLE=your-handle
BSKY_APP_PASSWORD=your-password
FEED_DID=did:web:your-domain
SERVICE_ENDPOINT=https://your-domain
DENSITY_THRESHOLD=0.6
CDK_DEFAULT_ACCOUNT=your-aws-account-id
CDK_DEFAULT_REGION=ap-northeast-1
```

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
│   │   └── index.css
│   ├── vite.config.js
│   └── package.json
├── scripts/
│   ├── update-stats-index.sh        # S3ファイルリストインデックス更新
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
