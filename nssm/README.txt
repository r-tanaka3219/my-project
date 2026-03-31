このフォルダに nssm.exe を配置してください。

【ダウンロード手順】
1. https://nssm.cc/download を開く
2. 最新版（nssm-x.x.x.zip）をダウンロード
3. ZIPを解凍
4. 解凍したフォルダ内の win64\nssm.exe を
   このフォルダ（nssm\nssm.exe）にコピーする

【NSSMとは】
任意のプログラムをWindowsサービスとして登録できる無料ツールです。
サービス化することで：
  ・PCを再起動しても自動起動（遅延自動起動）
  ・黒いコマンドプロンプト画面が不要
  ・クラッシュ時に10秒後自動再起動
  ・ログが logs\stdout.log / stderr.log に記録

【サービス操作コマンド（管理者CMD）】
  起動: net start InventorySystem
  停止: net stop  InventorySystem
  状態: sc query  InventorySystem
  管理: services.msc

【ログ確認】
  service_log.bat をダブルクリック
