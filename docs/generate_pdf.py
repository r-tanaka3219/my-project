"""
在庫管理システム ドキュメント PDF 生成スクリプト
reportlab + Windows日本語フォント (MS Gothic) 使用
"""
import os
import re
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, black, white, grey
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak, KeepTogether
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ── フォント登録 ─────────────────────────────────────────────
FONT_DIR = "C:/Windows/Fonts"
FONT_REG  = "MSGothic"
FONT_BOLD = "MSGothicBold"

def register_fonts():
    # MS Gothic (TTC index 0 = MS Gothic, index 1 = MS PGothic)
    pdfmetrics.registerFont(TTFont(FONT_REG,  FONT_DIR + "/msgothic.ttc", subfontIndex=0))
    pdfmetrics.registerFont(TTFont(FONT_BOLD, FONT_DIR + "/meiryob.ttc"))
    pdfmetrics.registerFontFamily(FONT_REG, normal=FONT_REG, bold=FONT_BOLD)

# ── スタイル定義 ─────────────────────────────────────────────
def make_styles():
    base_size  = 9.5
    styles = {}

    styles['h1'] = ParagraphStyle(
        'h1', fontName=FONT_BOLD, fontSize=18, leading=24,
        textColor=HexColor('#1e3a5f'), spaceAfter=10, spaceBefore=16,
        borderPad=4,
    )
    styles['h2'] = ParagraphStyle(
        'h2', fontName=FONT_BOLD, fontSize=14, leading=18,
        textColor=HexColor('#1e40af'), spaceAfter=6, spaceBefore=14,
        borderPad=2,
    )
    styles['h3'] = ParagraphStyle(
        'h3', fontName=FONT_BOLD, fontSize=11.5, leading=15,
        textColor=HexColor('#374151'), spaceAfter=4, spaceBefore=10,
    )
    styles['h4'] = ParagraphStyle(
        'h4', fontName=FONT_BOLD, fontSize=10, leading=13,
        textColor=HexColor('#6b7280'), spaceAfter=3, spaceBefore=8,
    )
    styles['body'] = ParagraphStyle(
        'body', fontName=FONT_REG, fontSize=base_size, leading=15,
        spaceAfter=5, wordWrap='CJK',
    )
    styles['bullet'] = ParagraphStyle(
        'bullet', fontName=FONT_REG, fontSize=base_size, leading=15,
        leftIndent=14, bulletIndent=4, spaceAfter=3, wordWrap='CJK',
    )
    styles['bullet2'] = ParagraphStyle(
        'bullet2', fontName=FONT_REG, fontSize=base_size-0.5, leading=14,
        leftIndent=28, bulletIndent=16, spaceAfter=2, wordWrap='CJK',
    )
    styles['code'] = ParagraphStyle(
        'code', fontName=FONT_REG, fontSize=8, leading=12,
        backColor=HexColor('#f3f4f6'), borderColor=HexColor('#d1d5db'),
        borderWidth=0.5, borderPad=6,
        leftIndent=8, rightIndent=8, spaceAfter=6, spaceBefore=4,
        wordWrap='CJK',
    )
    styles['table_header'] = ParagraphStyle(
        'th', fontName=FONT_BOLD, fontSize=8.5, leading=12,
        textColor=white, wordWrap='CJK', alignment=1,
    )
    styles['table_cell'] = ParagraphStyle(
        'td', fontName=FONT_REG, fontSize=8.5, leading=12,
        wordWrap='CJK',
    )
    return styles

# ── インラインマークアップ変換 ────────────────────────────────
def inline(text, bold_font=FONT_BOLD, reg_font=FONT_REG):
    """**bold**, `code` を reportlab XML タグに変換"""
    # エスケープ
    text = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    # **bold**
    text = re.sub(r'\*\*(.+?)\*\*', r'<font name="' + bold_font + r'">\1</font>', text)
    # *italic* (same font, color)
    text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)
    # `code`
    text = re.sub(r'`([^`]+)`', r'<font name="' + reg_font + r'" size="8" color="#374151">\1</font>', text)
    return text

# ── Markdown パーサー → Flowable リスト ──────────────────────
def md_to_flowables(md_text, styles):
    lines   = md_text.splitlines()
    story   = []
    i       = 0
    in_code = False
    code_buf = []

    def flush_code():
        nonlocal code_buf
        if code_buf:
            # Paragraph で日本語対応コードブロックを描画
            lines_escaped = []
            for cl in code_buf:
                cl_esc = cl.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                lines_escaped.append(cl_esc if cl_esc else ' ')
            content = '<br/>'.join(lines_escaped)
            story.append(Paragraph(content, styles['code']))
            code_buf = []

    while i < len(lines):
        line = lines[i]

        # ── コードブロック ──
        if line.strip().startswith('```'):
            if in_code:
                flush_code()
                in_code = False
            else:
                in_code = True
            i += 1
            continue

        if in_code:
            code_buf.append(line)
            i += 1
            continue

        # ── 空行 ──
        if not line.strip():
            story.append(Spacer(1, 4))
            i += 1
            continue

        # ── 見出し ──
        m = re.match(r'^(#{1,4})\s+(.*)', line)
        if m:
            level = len(m.group(1))
            text  = inline(m.group(2))
            key   = f'h{level}' if level <= 4 else 'h4'
            if level == 1:
                story.append(HRFlowable(width='100%', thickness=2,
                                        color=HexColor('#1e3a5f'), spaceAfter=4))
            story.append(Paragraph(text, styles[key]))
            if level == 1:
                story.append(HRFlowable(width='100%', thickness=0.5,
                                        color=HexColor('#93c5fd'), spaceAfter=6))
            i += 1
            continue

        # ── 水平線 ──
        if re.match(r'^---+\s*$', line) or re.match(r'^===+\s*$', line):
            story.append(HRFlowable(width='100%', thickness=0.5,
                                    color=HexColor('#e5e7eb'), spaceBefore=4, spaceAfter=4))
            i += 1
            continue

        # ── テーブル ──
        if '|' in line and line.strip().startswith('|'):
            table_lines = []
            while i < len(lines) and '|' in lines[i] and lines[i].strip().startswith('|'):
                table_lines.append(lines[i])
                i += 1
            flowable = parse_table(table_lines, styles)
            if flowable:
                story.append(flowable)
                story.append(Spacer(1, 6))
            continue

        # ── 番号付きリスト ──
        m = re.match(r'^(\s*)(\d+)\.\s+(.*)', line)
        if m:
            indent = len(m.group(1)) // 2
            text   = inline(m.group(3))
            num    = m.group(2)
            sty    = styles['bullet2'] if indent > 0 else styles['bullet']
            story.append(Paragraph(f'{num}. {text}', sty))
            i += 1
            continue

        # ── 箇条書き ──
        m = re.match(r'^(\s*)[-*]\s+(.*)', line)
        if m:
            indent = len(m.group(1)) // 2
            text   = inline(m.group(2))
            sty    = styles['bullet2'] if indent > 0 else styles['bullet']
            story.append(Paragraph(f'• {text}', sty))
            i += 1
            continue

        # ── 通常段落 ──
        # 連続する行は1段落にまとめる
        para_lines = [line]
        i += 1
        while i < len(lines):
            nl = lines[i]
            if (not nl.strip() or nl.strip().startswith('#') or
                    nl.strip().startswith('|') or nl.strip().startswith('```') or
                    re.match(r'^(\s*)[-*]\s', nl) or re.match(r'^(\s*)\d+\.\s', nl) or
                    re.match(r'^---+\s*$', nl)):
                break
            para_lines.append(nl)
            i += 1
        text = inline(' '.join(para_lines))
        story.append(Paragraph(text, styles['body']))

    if in_code:
        flush_code()

    return story

# ── テーブルパーサー ─────────────────────────────────────────
def parse_table(lines, styles):
    rows = []
    for line in lines:
        # 区切り行はスキップ
        if re.match(r'^\s*\|[\s\-:|]+\|\s*$', line):
            continue
        cells = [c.strip() for c in line.strip().strip('|').split('|')]
        rows.append(cells)

    if not rows:
        return None

    max_cols = max(len(r) for r in rows)
    # 列数を揃える
    for r in rows:
        while len(r) < max_cols:
            r.append('')

    # パーセント列幅（ヘッダー行の長さで概算）
    page_w = A4[0] - 32*mm
    col_w  = page_w / max_cols

    table_data = []
    for ri, row in enumerate(rows):
        sty = styles['table_header'] if ri == 0 else styles['table_cell']
        table_data.append([Paragraph(inline(c), sty) for c in row])

    t = Table(table_data, colWidths=[col_w] * max_cols, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND',   (0, 0), (-1, 0),  HexColor('#1e40af')),
        ('TEXTCOLOR',    (0, 0), (-1, 0),  white),
        ('FONTNAME',     (0, 0), (-1, 0),  FONT_BOLD),
        ('FONTNAME',     (0, 1), (-1, -1), FONT_REG),
        ('FONTSIZE',     (0, 0), (-1, -1), 8),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [white, HexColor('#f8fafc')]),
        ('GRID',         (0, 0), (-1, -1), 0.4, HexColor('#d1d5db')),
        ('VALIGN',       (0, 0), (-1, -1), 'TOP'),
        ('TOPPADDING',   (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 4),
        ('LEFTPADDING',  (0, 0), (-1, -1), 5),
        ('RIGHTPADDING', (0, 0), (-1, -1), 5),
    ]))
    return t

# ── ページ番号フッター ────────────────────────────────────────
def make_footer(title):
    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFont(FONT_REG, 8)
        canvas.setFillColor(HexColor('#9ca3af'))
        w, h = A4
        page_num = doc.page
        canvas.drawString(16*mm, 12*mm, title)
        canvas.drawRightString(w - 16*mm, 12*mm, f'- {page_num} -')
        canvas.setStrokeColor(HexColor('#e5e7eb'))
        canvas.setLineWidth(0.5)
        canvas.line(16*mm, 15*mm, w - 16*mm, 15*mm)
        canvas.restoreState()
    return footer

# ── PDF 生成 ─────────────────────────────────────────────────
def generate_pdf(md_path, pdf_path, doc_title):
    print(f"  生成中: {pdf_path}")
    with open(md_path, encoding='utf-8') as f:
        md_text = f.read()

    styles = make_styles()
    story  = md_to_flowables(md_text, styles)

    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        leftMargin=16*mm, rightMargin=16*mm,
        topMargin=16*mm,  bottomMargin=22*mm,
        title=doc_title,
        author='在庫管理システム',
    )
    doc.build(story, onFirstPage=make_footer(doc_title),
              onLaterPages=make_footer(doc_title))
    size_kb = os.path.getsize(pdf_path) // 1024
    print(f"  完了: {pdf_path} ({size_kb} KB)")

# ── メイン ────────────────────────────────────────────────────
if __name__ == '__main__':
    register_fonts()

    BASE = os.path.dirname(os.path.abspath(__file__))
    docs = [
        ('要件定義書.md',  '要件定義書.pdf',  '在庫管理システム 要件定義書'),
        ('仕様設計書.md',  '仕様設計書.pdf',  '在庫管理システム 仕様設計書'),
        ('取扱説明書.md',  '取扱説明書.pdf',  '在庫管理システム 取扱説明書'),
    ]

    for md_name, pdf_name, title in docs:
        md_path  = os.path.join(BASE, md_name)
        pdf_path = os.path.join(BASE, pdf_name)
        generate_pdf(md_path, pdf_path, title)

    print('\n✅ PDF 3点 生成完了')
