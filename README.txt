============================================================
  在庫管理システム
  セットアップ・操作ガイド
============================================================

【動作環境】
  OS      : Windows 10 / 11 / Server 2019 / 2022
  Python  : 3.10 以上（3.12 / 3.13 推奨）
  DB      : PostgreSQL 12 以上
  ブラウザ: Chrome / Edge 推奨

============================================================
【目次】
============================================================

  A. 新サーバー・新PCへのセットアップ（GitHub からインストール）
  B. 初回セットアップ手順（Python / PostgreSQL インストール）
  C. コードの更新方法
  D. 本番運用: Windows サービス登録
  E. 設定ファイル (.env) 項目説明
  F. トラブルシューティング

============================================================
A. 新サーバー・新PCへのセットアップ（GitHub からインストール）
============================================================

--- STEP 1: Git のインストール --------------------------------

  1. https://git-scm.com/download/win を開く
  2. インストーラーをダウンロードして実行
     （設定はすべてデフォルトでOK）
  3. コマンドプロンプトで確認:
       git --version

--- STEP 2: リポジトリの取得 ----------------------------------

  【パターン1】フォルダが存在しない場合（初回クローン）

    cd C:\Users\[ユーザー名]
    git clone https://github.com/r-tanaka3219/my-project.git inventory_system
    cd inventory_system

  【パターン2】同名フォルダが既に存在してエラーになる場合

    ▼ 方法A: 既存フォルダ内で最新を取得（推奨）
      cd C:\Users\[ユーザー名]\inventory_system
      git pull origin master

    ▼ 方法B: 別の場所にクローンする
      git clone https://github.com/r-tanaka3219/my-project.git C:\inventory_system

    ▼ 方法C: 既存フォルダを削除してからクローン
      rmdir /s /q C:\Users\[ユーザー名]\inventory_system
      git clone https://github.com/r-tanaka3219/my-project.git inventory_system
      cd inventory_system

--- STEP 3: .env ファイルを作成 ------------------------------

  ※ .env はセキュリティ上 GitHub に含まれていません。手動で作成します。

  コマンドプロンプトで:
    copy .env.example .env

  .env が存在しない場合は手動作成:
    メモ帳で新規ファイルを作成し、以下の内容を記入して
    「.env」という名前で inventory_system フォルダに保存:

    SECRET_KEY=（任意の英数字16文字以上。例: mysecretkey1234）
    PG_HOST=（PostgreSQL サーバーのIPアドレスまたは localhost）
    PG_PORT=5432
    PG_DBNAME=（データベース名。例: inventory）
    PG_USER=（DBユーザー名。例: inventory_user）
    PG_PASSWORD=（DBパスワード）
    PORT=5000
    USE_WAITRESS=1

--- STEP 4: Python パッケージのインストール ------------------

    pip install -r requirements.txt

  ※ エラーが出る場合は以下を試してください:
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt

--- STEP 5: データベースの初期化 -----------------------------

  ※ 初回のみ。既にDBが存在する場合はスキップ。

    python create_db.py

--- STEP 6: 起動確認 -----------------------------------------

  ▼ 手動起動（開発・確認用）:
    start.bat

  ▼ または直接起動:
    python app.py

  ブラウザで http://localhost:5000 を開いて動作確認

  初期ログイン情報:
    ユーザー名: admin
    パスワード: admin
    ※ ログイン後すぐにパスワードを変更してください


============================================================
B. 初回セットアップ手順（Python / PostgreSQL インストール）
============================================================

--- STEP 1: Python のインストール ----------------------------

  1. https://www.python.org/downloads/ を開く
  2. 最新版（3.12 以上推奨）をダウンロードしてインストール
  3. インストール時「Add Python to PATH」に必ずチェック！
  4. 確認:
       python --version

--- STEP 2: PostgreSQL のインストール ------------------------

  ※ 別サーバーの DB に接続する場合はスキップ

  1. https://www.postgresql.org/download/windows/ を開く
  2. インストーラーをダウンロードして実行
  3. postgres ユーザーのパスワードを設定してメモしておく
  4. インストール完了後、DBとユーザーを作成:

     スタートメニューから「SQL Shell (psql)」を起動し:

       CREATE DATABASE inventory;
       CREATE USER inventory_user WITH PASSWORD 'パスワード';
       GRANT ALL PRIVILEGES ON DATABASE inventory TO inventory_user;
       \q

--- STEP 3: setup.bat を実行（初回自動セットアップ） ---------

  setup.bat をダブルクリックしてください。
  以下を対話形式で設定します:

    ✔ Python バージョン確認
    ✔ 必要パッケージのインストール
    ✔ DB 接続情報の入力（.env ファイルに自動保存）
    ✔ メール送信設定（省略可）
    ✔ データベース・テーブルの自動作成

--- STEP 4: 動作確認 -----------------------------------------

  start.bat をダブルクリック
  ブラウザで http://localhost:5000 を開く


============================================================
C. コードの更新方法（GitHub から最新を取得）
============================================================

  【通常の更新手順】

    cd C:\Users\[ユーザー名]\inventory_system

    1. 最新コードを取得:
         git pull origin master

    2. 必要に応じてパッケージ更新:
         pip install -r requirements.txt

    3. サーバーを再起動:
         ▼ 手動起動の場合:
           stop.bat
           start.bat

         ▼ Windows サービスの場合（管理者コマンドプロンプト）:
           net stop InventorySystem && net start InventorySystem

  【更新後に「conflict」エラーが出る場合】

    ローカルに未コミットの変更がある場合に発生します。
    変更を破棄して強制的に最新にする:

      git fetch origin
      git reset --hard origin/master

    ※ ローカルの変更はすべて消えます。注意してください。

  【現在のバージョン確認】

    git log --oneline -5


============================================================
D. 本番運用: Windows サービスとして登録
============================================================

  PC 起動時に自動で在庫管理システムが起動するようになります。

  --- NSSM のインストール -----------------------------------

  1. https://nssm.cc/download を開く
  2. nssm-x.x.x.zip をダウンロードして解凍
  3. win64\nssm.exe を [inventory_systemフォルダ]\nssm\nssm.exe へコピー

  --- サービス登録 ------------------------------------------

  1. install_service.bat を右クリック → 「管理者として実行」
  2. 確認:
     services.msc を開いて「InventorySystem」が実行中か確認

  --- サービス管理コマンド（管理者コマンドプロンプト） ------

    起動: net start InventorySystem
    停止: net stop InventorySystem
    状態: sc query InventorySystem
    ログ: service_log.bat を実行（最新 50 行）

  --- サービス削除 ------------------------------------------

    uninstall_service.bat を右クリック → 「管理者として実行」


============================================================
E. 設定ファイル (.env) 項目説明
============================================================

  【基本設定】
  SECRET_KEY          セキュリティキー（英数字16文字以上推奨）
  PORT                Web サーバーポート（通常 5000）
  USE_WAITRESS        本番用サーバー使用（1=有効、0=開発用）

  【DB 接続】
  PG_HOST             PostgreSQL サーバーアドレス（例: localhost）
  PG_PORT             PostgreSQL ポート番号（通常 5432）
  PG_DBNAME           データベース名（例: inventory）
  PG_USER             DB ユーザー名
  PG_PASSWORD         DB パスワード

  【メール設定（省略可）】
  MAIL_SERVER         SMTP サーバーアドレス
  MAIL_PORT           SMTP ポート（587=TLS, 465=SSL, 25=なし）
  MAIL_USE_TLS        TLS 使用 (True / False)
  MAIL_USE_SSL        SSL 使用 (True / False)
  MAIL_AUTH           認証あり (True / False)
  MAIL_USERNAME       メールアカウント
  MAIL_PASSWORD       メールパスワード
  MAIL_FROM           送信元メールアドレス
  MAIL_FROM_NAME      送信者名

  【スケジューラー設定（省略可）】
  DAILY_MAIL_HOUR     日次メール送信時刻（時）
  DAILY_MAIL_MINUTE   日次メール送信時刻（分）


============================================================
F. トラブルシューティング
============================================================

  ▶ git clone でエラー「already exists and is not an empty directory」

    → 上記「A. STEP 2 パターン2」を参照

  ▶ pip install でエラーが出る
    → 以下を順に試してください:
       python -m pip install --upgrade pip
       python -m pip install -r requirements.txt --no-cache-dir

  ▶ 起動しない場合
    → service_log.bat を実行してエラー内容を確認
    → python app.py を直接実行してエラーを確認

  ▶ DB 接続エラーの場合
    → .env の PG_HOST / PG_PORT / PG_USER / PG_PASSWORD を確認
    → PostgreSQL サービスが起動しているか確認
       services.msc → postgresql-x64-XX が「実行中」か確認
    → python create_db.py を実行して詳細を確認

  ▶ ポートが使えない場合（Address already in use）
    → .env の PORT=5000 を別の番号に変更（例: 8080）
    → サービスを再起動

  ▶ パスワードを忘れた場合
    → DB に直接接続してパスワードをリセット:
       python -c "
       from database import get_db
       import hashlib
       db = get_db()
       new_pw = hashlib.sha256('新しいパスワード'.encode()).hexdigest()
       db.execute(\"UPDATE users SET password=%s WHERE username='admin'\", [new_pw])
       db.commit()
       print('完了')
       "

  ▶ ファイル更新後の再起動（手動起動の場合）
    1. stop.bat を実行
    2. start.bat を実行

  ▶ ファイル更新後の再起動（サービスの場合）
    管理者コマンドプロンプトで:
    net stop InventorySystem && net start InventorySystem

============================================================
【各ファイルの説明】
============================================================

  setup.bat               ★ 初回セットアップ（最初に実行）
  start.bat               通常起動（開発・テスト用）
  stop.bat                手動停止
  install_service.bat     Windows サービス登録（本番運用）
  uninstall_service.bat   Windows サービス削除
  service_log.bat         サービスログ確認（最新 50 行）
  create_db.py            DB 初期化スクリプト（単独実行可）
  test_performance.py     パフォーマンステスト
  .env                    設定ファイル（手動作成・GitHub 非管理）
  .env.example            .env のテンプレート

============================================================
サポート: システム管理者へ連絡
============================================================
