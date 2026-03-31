# -*- coding: utf-8 -*-
"""
ライブCSSエディタ パッチスクリプト
実行方法: python apply_css_editor_patch.py
"""
import os, re, sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
APP_PY   = os.path.join(BASE_DIR, 'app.py')
BASE_HTML = os.path.join(BASE_DIR, 'templates', 'base.html')
CSS_FILE  = os.path.join(BASE_DIR, 'static', 'ui_layout_patch.css')

# ─────────────────────────────────────────
# 1. app.py に /css-editor/save エンドポイントを追加
# ─────────────────────────────────────────
ROUTE_CODE = r'''
# ── ライブCSSエディタ 保存エンドポイント ──
@app.route('/css-editor/save', methods=['POST'])
def css_editor_save():
    import json, os
    try:
        data = request.get_json(force=True)
        css_content = data.get('css', '')
        css_path = os.path.join(app.static_folder, 'ui_layout_patch.css')
        with open(css_path, 'w', encoding='utf-8') as f:
            f.write(css_content)
        return json.dumps({'ok': True})
    except Exception as e:
        return json.dumps({'ok': False, 'error': str(e)}), 500

@app.route('/css-editor/load')
def css_editor_load():
    import json, os
    try:
        css_path = os.path.join(app.static_folder, 'ui_layout_patch.css')
        if os.path.exists(css_path):
            with open(css_path, encoding='utf-8') as f:
                css = f.read()
        else:
            css = ''
        return json.dumps({'ok': True, 'css': css})
    except Exception as e:
        return json.dumps({'ok': False, 'error': str(e)}), 500

'''

with open(APP_PY, encoding='utf-8') as f:
    app_content = f.read()

marker = "if __name__ == '__main__':"
if '/css-editor/save' in app_content:
    print('[app.py] CSSエディタルートは既に存在します。スキップ。')
elif marker in app_content:
    app_content = app_content.replace(marker, ROUTE_CODE + marker)
    with open(APP_PY, 'w', encoding='utf-8') as f:
        f.write(app_content)
    print('[app.py] CSSエディタルートを追加しました。')
else:
    print('[ERROR] app.py の挿入位置が見つかりません。')
    sys.exit(1)

# ─────────────────────────────────────────
# 2. base.html にライブエディタUIを埋め込む
# ─────────────────────────────────────────
EDITOR_HTML = r'''
<!-- ══ ライブCSSエディタ ══ -->
<div id="css-editor-fab" title="CSSエディタを開く"
     onclick="cssEditorToggle()"
     style="position:fixed;bottom:20px;right:20px;z-index:9999;
            width:48px;height:48px;border-radius:50%;
            background:#2563eb;color:#fff;border:none;cursor:pointer;
            font-size:20px;display:flex;align-items:center;justify-content:center;
            box-shadow:0 4px 12px rgba(37,99,235,.5);transition:transform .2s">
  🎨
</div>

<div id="css-editor-panel"
     style="display:none;position:fixed;top:0;right:0;bottom:0;z-index:9998;
            width:420px;background:#1a1d27;color:#e2e8f0;
            box-shadow:-4px 0 24px rgba(0,0,0,.4);
            display:none;flex-direction:column;font-family:monospace;font-size:12px">

  <!-- ヘッダー -->
  <div style="display:flex;align-items:center;gap:8px;padding:10px 14px;
              background:#0f1117;border-bottom:1px solid #2e3350;flex-shrink:0">
    <span style="font-weight:700;color:#4f7cff;font-size:13px">🎨 CSSライブエディタ</span>
    <span id="css-editor-status" style="font-size:11px;color:#8892a4;margin-left:4px"></span>
    <div style="margin-left:auto;display:flex;gap:6px">
      <button onclick="cssEditorSave()"
              style="padding:4px 12px;background:#34d399;color:#0f1117;border:none;
                     border-radius:5px;cursor:pointer;font-weight:700;font-size:11px">
        💾 保存
      </button>
      <button onclick="cssEditorReset()"
              style="padding:4px 10px;background:#374151;color:#e2e8f0;border:none;
                     border-radius:5px;cursor:pointer;font-size:11px">
        ↩ 元に戻す
      </button>
      <button onclick="cssEditorToggle()"
              style="padding:4px 10px;background:#374151;color:#e2e8f0;border:none;
                     border-radius:5px;cursor:pointer;font-size:11px">
        ✕
      </button>
    </div>
  </div>

  <!-- ツールバー：よく使うプロパティ -->
  <div style="padding:8px 12px;background:#22263a;border-bottom:1px solid #2e3350;
              flex-shrink:0;display:flex;flex-wrap:wrap;gap:6px;align-items:center">
    <span style="font-size:10px;color:#8892a4;width:100%;margin-bottom:2px">クイック挿入 ▼</span>
    <button class="css-snippet" data-css="table{table-layout:fixed;width:100%}">テーブル固定幅</button>
    <button class="css-snippet" data-css="td,th{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}">セル省略</button>
    <button class="css-snippet" data-css=".btn{min-height:34px;padding:6px 14px;font-size:13px}">ボタン高さ</button>
    <button class="css-snippet" data-css=".card{padding:16px;margin-bottom:16px;border-radius:10px}">カード余白</button>
    <button class="css-snippet" data-css=".page-title{font-size:17px;margin-bottom:14px}">タイトル</button>
    <button class="css-snippet" data-css="nav{min-height:52px}">ナビ高さ</button>
    <button class="css-snippet" data-css=".main{padding:18px 24px}">ページ余白</button>
    <button class="css-snippet" data-css="th{padding:8px 10px}.td{padding:7px 10px}">セル余白</button>
  </div>

  <!-- テキストエリア -->
  <textarea id="css-editor-area"
            spellcheck="false"
            placeholder="/* ここにCSSを入力 */&#10;/* 入力中はリアルタイムでプレビューに反映されます */&#10;&#10;例:&#10;table { table-layout: fixed; width: 100%; }&#10;.btn { min-height: 36px; }&#10;th { background: #e8f0fe; }"
            style="flex:1;background:#0a0d14;color:#a8d8a8;border:none;padding:12px;
                   resize:none;outline:none;line-height:1.6;tab-size:2;
                   font-family:'Consolas','Courier New',monospace;font-size:12px"></textarea>

  <!-- フッター -->
  <div style="padding:6px 12px;background:#0f1117;border-top:1px solid #2e3350;
              font-size:10px;color:#4a5568;flex-shrink:0">
    ※ 「保存」で static/ui_layout_patch.css に書き込みます。ページリロード後も維持されます。
  </div>
</div>

<script>
(function() {
  var panel = document.getElementById('css-editor-panel');
  var area  = document.getElementById('css-editor-area');
  var fab   = document.getElementById('css-editor-fab');
  var status = document.getElementById('css-editor-status');
  var liveStyle = document.createElement('style');
  liveStyle.id = 'css-editor-live';
  document.head.appendChild(liveStyle);
  var originalCSS = '';
  var open = false;

  // 既存のCSSを読み込む
  fetch('/css-editor/load')
    .then(function(r){ return r.json(); })
    .then(function(d){
      if(d.ok && d.css){
        area.value = d.css;
        originalCSS = d.css;
        applyLive(d.css);
        status.textContent = '（保存済みCSSを読み込みました）';
        setTimeout(function(){ status.textContent=''; }, 3000);
      }
    }).catch(function(){});

  // リアルタイム反映
  area.addEventListener('input', function(){
    applyLive(area.value);
  });

  function applyLive(css){
    liveStyle.textContent = css;
  }

  // スニペット挿入
  document.querySelectorAll('.css-snippet').forEach(function(btn){
    btn.style.cssText = 'padding:2px 8px;background:#2e3350;color:#a78bfa;border:1px solid #4f7cff33;border-radius:4px;cursor:pointer;font-size:10px;font-family:monospace';
    btn.addEventListener('click', function(){
      var snippet = '\n' + btn.dataset.css + '\n';
      var start = area.selectionStart;
      area.value = area.value.slice(0,start) + snippet + area.value.slice(start);
      area.selectionStart = area.selectionEnd = start + snippet.length;
      area.focus();
      applyLive(area.value);
    });
  });

  // 保存
  window.cssEditorSave = function(){
    status.textContent = '保存中...';
    fetch('/css-editor/save', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({css: area.value})
    }).then(function(r){ return r.json(); })
      .then(function(d){
        if(d.ok){
          originalCSS = area.value;
          status.textContent = '✅ 保存しました';
          setTimeout(function(){ status.textContent=''; }, 3000);
        } else {
          status.textContent = '❌ 保存失敗: ' + (d.error||'');
        }
      }).catch(function(e){
        status.textContent = '❌ エラー: ' + e;
      });
  };

  // 元に戻す
  window.cssEditorReset = function(){
    area.value = originalCSS;
    applyLive(originalCSS);
    status.textContent = '元に戻しました';
    setTimeout(function(){ status.textContent=''; }, 2000);
  };

  // パネル開閉
  window.cssEditorToggle = function(){
    open = !open;
    panel.style.display = open ? 'flex' : 'none';
    fab.style.transform = open ? 'scale(0.85)' : 'scale(1)';
    if(open) area.focus();
  };
})();
</script>
<!-- ══ /ライブCSSエディタ ══ -->
'''

with open(BASE_HTML, encoding='utf-8') as f:
    base_content = f.read()

if 'css-editor-fab' in base_content:
    print('[base.html] エディタUIは既に存在します。スキップ。')
elif '</body>' in base_content:
    base_content = base_content.replace('</body>', EDITOR_HTML + '\n</body>')
    with open(BASE_HTML, 'w', encoding='utf-8') as f:
        f.write(base_content)
    print('[base.html] ライブCSSエディタUIを追加しました。')
else:
    print('[ERROR] base.html に </body> タグが見つかりません。')
    sys.exit(1)

# ─────────────────────────────────────────
# 3. ui_layout_patch.css が存在しない場合は空ファイル作成
# ─────────────────────────────────────────
os.makedirs(os.path.join(BASE_DIR, 'static'), exist_ok=True)
if not os.path.exists(CSS_FILE):
    with open(CSS_FILE, 'w', encoding='utf-8') as f:
        f.write('/* ui_layout_patch.css - ライブCSSエディタで編集 */\n')
    print('[static/ui_layout_patch.css] ファイルを作成しました。')
else:
    print('[static/ui_layout_patch.css] 既存ファイルを使用します。')

print()
print('=' * 50)
print('パッチ適用完了！')
print()
print('使い方:')
print('  1. Flaskサーバーを起動')
print('  2. ブラウザ右下の 🎨 ボタンをクリック')
print('  3. CSSを入力 → リアルタイムで反映')
print('  4. 「💾 保存」で ui_layout_patch.css に書き込み')
print('=' * 50)
