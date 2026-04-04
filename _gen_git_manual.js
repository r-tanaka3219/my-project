const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  HeadingLevel, AlignmentType, BorderStyle, WidthType, ShadingType,
  VerticalAlign, PageBreak, Header, Footer, PageNumber
} = require('docx');
const fs = require('fs');

// ── ユーティリティ ──────────────────────────────────────
const BORDER = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const BORDERS = { top: BORDER, bottom: BORDER, left: BORDER, right: BORDER };
const NO_BORDER = { style: BorderStyle.NONE, size: 0, color: "FFFFFF" };
const NO_BORDERS = { top: NO_BORDER, bottom: NO_BORDER, left: NO_BORDER, right: NO_BORDER };

function h1(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_1,
    children: [new TextRun({ text, font: "Meiryo", size: 32, bold: true, color: "1F4E79" })],
    spacing: { before: 300, after: 160 },
  });
}

function h2(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_2,
    children: [new TextRun({ text, font: "Meiryo", size: 28, bold: true, color: "2E75B6" })],
    spacing: { before: 240, after: 120 },
  });
}

function h3(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_3,
    children: [new TextRun({ text, font: "Meiryo", size: 24, bold: true, color: "404040" })],
    spacing: { before: 180, after: 80 },
  });
}

function body(text, opts = {}) {
  return new Paragraph({
    children: [new TextRun({ text, font: "Meiryo", size: 22, ...opts })],
    spacing: { before: 60, after: 60 },
  });
}

function bullet(text, indent = 360) {
  return new Paragraph({
    children: [new TextRun({ text: "● " + text, font: "Meiryo", size: 22 })],
    spacing: { before: 60, after: 60 },
    indent: { left: indent },
  });
}

function warn(text) {
  return new Paragraph({
    children: [new TextRun({ text: "⚠ " + text, font: "Meiryo", size: 22, color: "C00000", bold: true })],
    spacing: { before: 80, after: 80 },
    indent: { left: 360 },
  });
}

function ok(text) {
  return new Paragraph({
    children: [new TextRun({ text: "✓ " + text, font: "Meiryo", size: 22, color: "375623" })],
    spacing: { before: 60, after: 60 },
    indent: { left: 360 },
  });
}

function code(text) {
  return new Paragraph({
    children: [new TextRun({ text, font: "Courier New", size: 20, color: "1F4E79" })],
    spacing: { before: 40, after: 40 },
    indent: { left: 720 },
    shading: { type: ShadingType.CLEAR, fill: "EEF3F8" },
  });
}

function gap(n = 1) {
  return Array.from({ length: n }, () => new Paragraph({
    children: [new TextRun("")],
    spacing: { before: 40, after: 40 },
  }));
}

function pageBreak() {
  return new Paragraph({ children: [new PageBreak()] });
}

// ── テーブルヘルパー ────────────────────────────────────
function makeTable(headers, rows, colWidths) {
  const totalWidth = colWidths.reduce((a, b) => a + b, 0);
  const headerCells = headers.map((h, i) =>
    new TableCell({
      borders: BORDERS,
      width: { size: colWidths[i], type: WidthType.DXA },
      shading: { type: ShadingType.CLEAR, fill: "1F4E79" },
      margins: { top: 80, bottom: 80, left: 120, right: 120 },
      verticalAlign: VerticalAlign.CENTER,
      children: [new Paragraph({
        alignment: AlignmentType.CENTER,
        children: [new TextRun({ text: h, font: "Meiryo", size: 20, bold: true, color: "FFFFFF" })],
      })],
    })
  );

  const dataRows = rows.map((row, ri) =>
    new TableRow({
      children: row.map((cell, ci) =>
        new TableCell({
          borders: BORDERS,
          width: { size: colWidths[ci], type: WidthType.DXA },
          shading: { type: ShadingType.CLEAR, fill: ri % 2 === 0 ? "FFFFFF" : "F5F9FF" },
          margins: { top: 80, bottom: 80, left: 120, right: 120 },
          verticalAlign: VerticalAlign.CENTER,
          children: [new Paragraph({
            children: [new TextRun({ text: cell, font: "Meiryo", size: 20 })],
          })],
        })
      ),
    })
  );

  return new Table({
    width: { size: totalWidth, type: WidthType.DXA },
    columnWidths: colWidths,
    rows: [
      new TableRow({ children: headerCells, tableHeader: true }),
      ...dataRows,
    ],
  });
}

// ── 注意ボックス ─────────────────────────────────────────
function noteBox(title, lines) {
  const cells = [
    new TableCell({
      borders: {
        top: { style: BorderStyle.SINGLE, size: 4, color: "ED7D31" },
        bottom: { style: BorderStyle.SINGLE, size: 4, color: "ED7D31" },
        left: { style: BorderStyle.SINGLE, size: 12, color: "ED7D31" },
        right: { style: BorderStyle.SINGLE, size: 4, color: "ED7D31" },
      },
      shading: { type: ShadingType.CLEAR, fill: "FFF3E8" },
      margins: { top: 100, bottom: 100, left: 160, right: 160 },
      children: [
        new Paragraph({
          children: [new TextRun({ text: title, font: "Meiryo", size: 22, bold: true, color: "C55A11" })],
          spacing: { before: 0, after: 60 },
        }),
        ...lines.map(l => new Paragraph({
          children: [new TextRun({ text: l, font: "Meiryo", size: 21 })],
          spacing: { before: 40, after: 40 },
        })),
      ],
    }),
  ];

  return new Table({
    width: { size: 9026, type: WidthType.DXA },
    columnWidths: [9026],
    rows: [new TableRow({ children: cells })],
  });
}

function infoBox(title, lines) {
  const cells = [
    new TableCell({
      borders: {
        top: { style: BorderStyle.SINGLE, size: 4, color: "2E75B6" },
        bottom: { style: BorderStyle.SINGLE, size: 4, color: "2E75B6" },
        left: { style: BorderStyle.SINGLE, size: 12, color: "2E75B6" },
        right: { style: BorderStyle.SINGLE, size: 4, color: "2E75B6" },
      },
      shading: { type: ShadingType.CLEAR, fill: "EEF3FB" },
      margins: { top: 100, bottom: 100, left: 160, right: 160 },
      children: [
        new Paragraph({
          children: [new TextRun({ text: title, font: "Meiryo", size: 22, bold: true, color: "1F4E79" })],
          spacing: { before: 0, after: 60 },
        }),
        ...lines.map(l => new Paragraph({
          children: [new TextRun({ text: l, font: "Meiryo", size: 21 })],
          spacing: { before: 40, after: 40 },
        })),
      ],
    }),
  ];

  return new Table({
    width: { size: 9026, type: WidthType.DXA },
    columnWidths: [9026],
    rows: [new TableRow({ children: cells })],
  });
}

// ══════════════════════════════════════════════════════════
// ドキュメント構築
// ══════════════════════════════════════════════════════════
const children = [

  // ── 表紙 ──────────────────────────────────────────────
  ...gap(4),
  new Paragraph({
    alignment: AlignmentType.CENTER,
    children: [new TextRun({ text: "在庫管理システム", font: "Meiryo", size: 56, bold: true, color: "1F4E79" })],
    spacing: { before: 0, after: 160 },
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER,
    children: [new TextRun({ text: "Git / GitHub 運用マニュアル", font: "Meiryo", size: 40, bold: true, color: "2E75B6" })],
    spacing: { before: 0, after: 120 },
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER,
    children: [new TextRun({ text: "PC-A（開発専用）・PC-B（開発 + 本番）共通", font: "Meiryo", size: 26, color: "595959" })],
    spacing: { before: 0, after: 240 },
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER,
    children: [new TextRun({ text: "作成日：2026年4月3日", font: "Meiryo", size: 22, color: "808080" })],
  }),
  pageBreak(),

  // ── 1. 全体構成 ────────────────────────────────────────
  h1("1. システム全体の構成"),
  body("2台のPCとGitHubで在庫管理システムの開発・本番運用を行います。"),
  ...gap(),
  makeTable(
    ["項目", "PC-A（このPC）", "PC-B（本番PC）"],
    [
      ["役割",          "開発専用",                    "開発 + 本番環境"],
      ["ブランチ",      "feature/dev",                 "master（本番） / feature/dev（開発）"],
      ["GitHub push",   "feature/dev へ push",         "master へ pull / feature/dev へ push"],
      ["本番サービス",  "なし",                        "InventorySystem（NSSM管理）"],
      ["Claude Code",   "feature/dev → origin/feature/dev", "master or feature/dev"],
    ],
    [3000, 3013, 3013]
  ),
  ...gap(),
  infoBox("ブランチ運用のポイント", [
    "● master  … 本番用。直接コミットしない。PR（プルリクエスト）経由でのみ更新する",
    "● feature/dev … 開発用。PC-A / PC-B 両方でこのブランチを使って開発する",
    "● PR マージ後、PC-B で git pull origin master → 本番サービス再起動",
  ]),
  pageBreak(),

  // ── 2. PC-A 毎回の手順 ────────────────────────────────
  h1("2. PC-A（開発専用）毎回の作業手順"),

  h2("2-1. 作業開始時（必ず最初に行う）"),
  body("作業を始める前に、必ず最新のコードを取得してください。"),
  ...gap(),
  body("① feature/dev に切り替え（既にいる場合はスキップ可）", { bold: true }),
  code("git checkout feature/dev"),
  ...gap(),
  body("② リモートの最新を取得", { bold: true }),
  code("git pull origin feature/dev"),
  ...gap(),
  noteBox("注意", [
    "⚠ git pull を忘れると、PC-B の変更と競合（コンフリクト）が発生します",
    "⚠ 必ず作業開始の度に git pull を実行してください",
  ]),
  ...gap(),

  h2("2-2. 作業中（Claude Code でコーディング）"),
  body("Claude Code に作業を依頼する際の流れ："),
  ...gap(),
  body("① Claude Code のチャットで修正内容を指示する"),
  body("② Claude Code が自動的にコードを修正・保存する"),
  body("③ 動作確認を行う（ブラウザで画面確認など）"),
  ...gap(),
  ok("feature/dev ブランチ上で作業しているか確認：git branch"),
  ...gap(),

  h2("2-3. 作業完了時（GitHub へ push）"),
  body("修正が完了したら GitHub に push してください。"),
  ...gap(),
  body("① 変更ファイルの確認", { bold: true }),
  code("git status"),
  ...gap(),
  body("② ステージング（変更をコミット対象に追加）", { bold: true }),
  code("git add -A"),
  ...gap(),
  body("③ コミット（変更内容を記録）", { bold: true }),
  code('git commit -m "fix: 修正内容を簡潔に記述"'),
  ...gap(),
  body("④ GitHub へ push", { bold: true }),
  code("git push origin feature/dev"),
  ...gap(),

  h2("2-4. 本番に反映したいとき（PR作成）"),
  body("feature/dev の内容を master に反映するには、GitHubでPR（プルリクエスト）を作成します。"),
  ...gap(),
  body("① GitHub（ブラウザ）を開く", { bold: true }),
  code("https://github.com/r-tanaka3219/my-project"),
  ...gap(),
  body("② 「Compare & pull request」ボタンをクリック", { bold: true }),
  body("③ タイトルと内容を記入して「Create pull request」", { bold: true }),
  body("④ 「Merge pull request」でmasterに取り込む", { bold: true }),
  ...gap(),
  noteBox("PR後にPC-Bでやること", [
    "PC-B で以下を実行して本番に反映する",
    "  git checkout master",
    "  git pull origin master",
    "  → 本番サービスの再起動",
  ]),
  pageBreak(),

  // ── 3. PC-B 毎回の手順 ────────────────────────────────
  h1("3. PC-B（開発 + 本番環境）毎回の作業手順"),

  h2("3-1. 作業開始時（必ず最初に行う）"),
  body("PC-B でも開発する場合は、必ず最新を pull してから始めてください。"),
  ...gap(),
  body("① feature/dev に切り替え", { bold: true }),
  code("git checkout feature/dev"),
  ...gap(),
  body("② 最新取得", { bold: true }),
  code("git pull origin feature/dev"),
  ...gap(),

  h2("3-2. 開発作業（Claude Code でコーディング）"),
  body("PC-A と同じ手順で Claude Code に依頼して開発します。"),
  ...gap(),
  body("完了後 GitHub へ push："),
  code("git add -A"),
  code('git commit -m "fix: 修正内容"'),
  code("git push origin feature/dev"),
  ...gap(),

  h2("3-3. 本番への反映手順"),
  body("GitHub で PR がマージされた後、以下の手順で本番を更新します。"),
  ...gap(),
  body("① master ブランチに切り替え", { bold: true }),
  code("git checkout master"),
  ...gap(),
  body("② 最新の master を取得", { bold: true }),
  code("git pull origin master"),
  ...gap(),
  body("③ 本番サービスを再起動", { bold: true }),
  code("# PowerShell で実行"),
  code('$svc = Get-WmiObject -Class Win32_Service -Filter "Name=\'InventorySystem\'"'),
  code("$svc.StopService(); Start-Sleep -Seconds 4; $svc.StartService()"),
  ...gap(),
  body("④ 本番の動作確認", { bold: true }),
  bullet("ブラウザで画面を確認"),
  bullet("エラーログを確認：logs/ フォルダ"),
  ...gap(),
  body("⑤ 開発ブランチに戻す（次の開発のため）", { bold: true }),
  code("git checkout feature/dev"),
  ...gap(),
  noteBox("重要", [
    "⚠ 本番反映後は必ず feature/dev に戻すこと",
    "⚠ master ブランチのままにしておくと、次回開発時に誤って master に push するリスクがある",
  ]),
  pageBreak(),

  // ── 4. コンフリクト対処 ───────────────────────────────
  h1("4. コンフリクト（競合）が発生した場合"),
  body("PC-A と PC-B が同じファイルを同時に編集すると競合が発生することがあります。"),
  ...gap(),

  h2("4-1. コンフリクトの確認"),
  code("git pull origin feature/dev"),
  body("↓ 下記のようなメッセージが出た場合はコンフリクト発生"),
  code("CONFLICT (content): Merge conflict in blueprints/orders.py"),
  ...gap(),

  h2("4-2. 対処手順"),
  body("① Claude Code に「コンフリクトを解消してください」と依頼する"),
  body("② Claude Code が対象ファイルを確認・修正する"),
  body("③ 修正後にコミット・push する"),
  code("git add -A"),
  code('git commit -m "fix: コンフリクト解消"'),
  code("git push origin feature/dev"),
  ...gap(),
  infoBox("コンフリクト防止のベストプラクティス", [
    "● 作業開始前に必ず git pull を実行する",
    "● PC-A と PC-B で同じファイルを同時に編集しない",
    "● 大きな作業は片方のPCに集中させる（例：PC-AはDB修正、PC-Bは画面修正）",
    "● 作業が終わったらすぐに push する",
  ]),
  pageBreak(),

  // ── 5. よくある作業パターン ───────────────────────────
  h1("5. よくある作業パターン"),

  h2("5-1. 通常の機能追加・バグ修正"),
  makeTable(
    ["ステップ", "作業内容", "担当PC"],
    [
      ["1", "git pull origin feature/dev（最新取得）",    "PC-A or PC-B"],
      ["2", "Claude Code で修正・開発",                   "PC-A or PC-B"],
      ["3", "動作確認",                                   "PC-A or PC-B"],
      ["4", "git push origin feature/dev",               "PC-A or PC-B"],
      ["5", "GitHub で PR 作成 → マージ",                "どちらでも可"],
      ["6", "git pull origin master（本番取得）",        "PC-B のみ"],
      ["7", "本番サービス再起動",                         "PC-B のみ"],
      ["8", "git checkout feature/dev（開発に戻す）",    "PC-B のみ"],
    ],
    [1000, 5500, 2526]
  ),
  ...gap(),

  h2("5-2. 緊急の本番修正（ホットフィックス）"),
  body("本番でバグが発生し、すぐに修正が必要な場合："),
  ...gap(),
  body("PC-B で作業（本番PCで直接修正）："),
  code("git checkout feature/dev"),
  code("git pull origin feature/dev"),
  body("↓ Claude Code で緊急修正"),
  code("git add -A"),
  code('git commit -m "hotfix: 緊急修正内容"'),
  code("git push origin feature/dev"),
  body("↓ GitHub で PR 作成 → 即マージ"),
  code("git checkout master"),
  code("git pull origin master"),
  body("↓ サービス再起動 → 動作確認"),
  code("git checkout feature/dev"),
  ...gap(),

  h2("5-3. 現在のブランチ確認"),
  body("今どのブランチにいるかわからない場合："),
  code("git branch"),
  body("「* feature/dev」と表示されていれば正常です。"),
  pageBreak(),

  // ── 6. チェックリスト ─────────────────────────────────
  h1("6. 作業チェックリスト"),

  h2("6-1. PC-A（開発専用）チェックリスト"),
  makeTable(
    ["確認項目", "コマンド / 操作"],
    [
      ["作業開始：ブランチが feature/dev か確認",  "git branch"],
      ["作業開始：最新コードを取得した",            "git pull origin feature/dev"],
      ["作業完了：変更をコミットした",              "git commit -m \"修正内容\""],
      ["作業完了：GitHub に push した",            "git push origin feature/dev"],
      ["本番反映：GitHub で PR を作成した",         "ブラウザで操作"],
      ["本番反映：PC-B に連絡（必要な場合）",       "口頭・チャット等"],
    ],
    [5000, 4026]
  ),
  ...gap(),

  h2("6-2. PC-B（開発+本番）チェックリスト"),
  makeTable(
    ["確認項目", "コマンド / 操作"],
    [
      ["開発開始：feature/dev に切り替え済み",      "git checkout feature/dev"],
      ["開発開始：最新コードを取得した",             "git pull origin feature/dev"],
      ["本番反映前：PR がマージ済みか確認",          "GitHub で確認"],
      ["本番反映：master に切り替えた",             "git checkout master"],
      ["本番反映：git pull origin master 実行",    "git pull origin master"],
      ["本番反映：サービス再起動した",               "PowerShell コマンド実行"],
      ["本番反映：動作確認した",                    "ブラウザで画面確認"],
      ["本番反映後：feature/dev に戻した",          "git checkout feature/dev"],
    ],
    [5000, 4026]
  ),
  pageBreak(),

  // ── 7. トラブルシューティング ─────────────────────────
  h1("7. トラブルシューティング"),

  makeTable(
    ["症状", "原因", "対処"],
    [
      [
        "git push が失敗する",
        "リモートに新しいコミットがある",
        "git pull origin feature/dev を先に実行"
      ],
      [
        "コンフリクトが発生した",
        "同じファイルを両PCで編集",
        "Claude Code に「コンフリクト解消」を依頼"
      ],
      [
        "本番が更新されない",
        "サービス再起動を忘れた",
        "PowerShell でサービスを再起動"
      ],
      [
        "master に直接 commit してしまった",
        "ブランチ確認を忘れた",
        "git reset HEAD~1 で取り消し → feature/dev に移動"
      ],
      [
        "feature/dev に PR 後の変更が反映されない",
        "pull 忘れ",
        "git pull origin feature/dev を実行"
      ],
      [
        "どのブランチにいるか分からない",
        "-",
        "git branch で確認（* が現在のブランチ）"
      ],
    ],
    [2500, 2800, 3726]
  ),
  ...gap(2),

  // ── 8. 重要コマンド一覧 ───────────────────────────────
  h1("8. 重要コマンド一覧"),

  makeTable(
    ["目的", "コマンド"],
    [
      ["現在のブランチ確認",                    "git branch"],
      ["ブランチ切り替え（feature/dev）",        "git checkout feature/dev"],
      ["ブランチ切り替え（master）",             "git checkout master"],
      ["最新取得（feature/dev）",               "git pull origin feature/dev"],
      ["最新取得（master）",                    "git pull origin master"],
      ["変更確認",                              "git status"],
      ["全変更をステージ",                       "git add -A"],
      ["コミット",                              'git commit -m "メッセージ"'],
      ["GitHub へ push",                       "git push origin feature/dev"],
      ["コミット履歴確認",                       "git log --oneline -10"],
      ["本番サービス再起動（PowerShell）",       '$svc=Get-WmiObject Win32_Service -Filter "Name=\'InventorySystem\'"; $svc.StopService(); sleep 4; $svc.StartService()'],
    ],
    [4000, 5026]
  ),
];

// ── フッター ────────────────────────────────────────────
const footer = new Footer({
  children: [new Paragraph({
    alignment: AlignmentType.CENTER,
    children: [
      new TextRun({ text: "在庫管理システム Git/GitHub 運用マニュアル　　", font: "Meiryo", size: 18, color: "808080" }),
      new TextRun({ children: [PageNumber.CURRENT], font: "Meiryo", size: 18, color: "808080" }),
      new TextRun({ text: " / ", font: "Meiryo", size: 18, color: "808080" }),
      new TextRun({ children: [PageNumber.TOTAL_PAGES], font: "Meiryo", size: 18, color: "808080" }),
    ],
    border: { top: { style: BorderStyle.SINGLE, size: 4, color: "CCCCCC" } },
  })],
});

const doc = new Document({
  sections: [{
    properties: {
      page: {
        size: { width: 11906, height: 16838 },
        margin: { top: 1440, right: 1260, bottom: 1440, left: 1260 },
      },
    },
    footers: { default: footer },
    children,
  }],
});

Packer.toBuffer(doc).then(buf => {
  const out = "C:/Users/sato-mzk-002/inventory_system/在庫管理システム_Git運用マニュアル.docx";
  fs.writeFileSync(out, buf);
  console.log("✓ 作成完了:", out);
});
