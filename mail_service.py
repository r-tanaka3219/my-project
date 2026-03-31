# -*- coding: utf-8 -*-
"""Mail service - 社内SMTPサーバー対応（Gmail / 社内メール 両対応）"""

import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
from datetime import date
from database import get_db

def _read_env() -> dict:
    """.envファイルを毎回直接読み込む（再起動不要）"""
    import pathlib
    env = {}
    env_path = pathlib.Path(__file__).parent / '.env'
    if env_path.exists():
        for line in env_path.read_text(encoding='utf-8', errors='ignore').splitlines():
            if '=' in line and not line.startswith('#'):
                k, _, v = line.partition('=')
                env[k.strip()] = v.strip()
    return env

def _get_cfg():
    """メール設定を.envから毎回読み込む（設定保存後に再起動不要）"""
    e = _read_env()
    return {
        'server':    e.get('MAIL_SERVER',   os.getenv('MAIL_SERVER',   '')),
        'port':      int(e.get('MAIL_PORT', os.getenv('MAIL_PORT', '25'))),
        'use_tls':   e.get('MAIL_USE_TLS',  os.getenv('MAIL_USE_TLS',  'False')).lower() == 'true',
        'use_ssl':   e.get('MAIL_USE_SSL',  os.getenv('MAIL_USE_SSL',  'False')).lower() == 'true',
        'auth':      e.get('MAIL_AUTH',     os.getenv('MAIL_AUTH',     'False')).lower() == 'true',
        'username':  e.get('MAIL_USERNAME', os.getenv('MAIL_USERNAME', '')),
        'password':  e.get('MAIL_PASSWORD', os.getenv('MAIL_PASSWORD', '')),
        'from_addr': e.get('MAIL_FROM',     os.getenv('MAIL_FROM',     '')),
        'from_name': e.get('MAIL_FROM_NAME',os.getenv('MAIL_FROM_NAME','在庫管理システム')),
    }

def _get_addrs(send_type: str, supplier_cd: str = None) -> list:
    """送信区分に一致する有効な宛先を返す。
    supplier_cdが指定された場合：その仕入先CD専用宛先のみ返す
    supplier_cdが'__common__'の場合：空白（専用指定なし）宛先のみ返す
    supplier_cdがNoneの場合：全宛先
    """
    db = get_db()
    try:
        db.execute("ALTER TABLE mail_recipients ADD COLUMN IF NOT EXISTS supplier_cd TEXT DEFAULT ''")
        db.commit()
    except Exception:
        pass
    if supplier_cd == '__common__':
        # 空白宛先のみ返す（専用指定なし宛先）
        rows = db.execute(
            """SELECT email FROM mail_recipients
               WHERE is_active=1 AND (send_type=%s OR send_type='both')
               AND (supplier_cd='' OR supplier_cd IS NULL)
               ORDER BY id""",
            [send_type]
        ).fetchall()
        db.close()
        return [r['email'] for r in rows]
    elif supplier_cd:
        # 全宛先を取得
        all_rows = db.execute(
            """SELECT email, supplier_cd FROM mail_recipients
               WHERE is_active=1 AND (send_type=%s OR send_type='both')
               ORDER BY id""",
            [send_type]
        ).fetchall()

        # 専用指定されている全仕入先CDセットを作成（カンマ区切り対応）
        dedicated_cds = set()
        for r in all_rows:
            scd = r['supplier_cd'] or ''
            if scd:
                for cd in [c.strip() for c in scd.split(',') if c.strip()]:
                    dedicated_cds.add(cd)

        result = []
        for r in all_rows:
            scd = r['supplier_cd'] or ''
            if scd:
                # 専用宛先：カンマ区切りで複数仕入先CD指定に対応
                cds = [c.strip() for c in scd.split(',') if c.strip()]
                if supplier_cd in cds:
                    result.append(r['email'])
            else:
                # 共通宛先：この仕入先に専用宛先がある場合は除外
                if supplier_cd not in dedicated_cds:
                    result.append(r['email'])

        db.close()
        return list(dict.fromkeys(result))  # 重複除去・順序維持
    else:
        rows = db.execute(
            "SELECT email FROM mail_recipients WHERE is_active=1 AND (send_type=%s OR send_type='both') ORDER BY id",
            [send_type]
        ).fetchall()
        db.close()
        return [r['email'] for r in rows]

def _build_smtp(cfg):
    """設定に応じてSMTP接続を返す（接続・認証済みオブジェクト）"""
    if cfg['use_ssl']:
        smtp = smtplib.SMTP_SSL(cfg['server'], cfg['port'], timeout=15)
    else:
        smtp = smtplib.SMTP(cfg['server'], cfg['port'], timeout=15)
        if cfg['use_tls']:
            smtp.starttls()
    if cfg['auth'] and cfg['username']:
        smtp.login(cfg['username'], cfg['password'])
    return smtp

def _build_from(cfg):
    """文字化けしないFromヘッダーを生成"""
    from_addr = cfg['from_addr'] or cfg['username']
    from_name = cfg['from_name'] or '在庫管理システム'
    return formataddr((str(Header(from_name, 'utf-8')), from_addr))

def _send_smtp(cfg, from_addr, recipients, msg_string):
    """SMTP送信共通ヘルパー"""
    smtp = _build_smtp(cfg)
    try:
        smtp.sendmail(from_addr, recipients, msg_string)
    finally:
        try:
            smtp.quit()
        except Exception:
            pass


# ─── メールテンプレート ─────────────────────────────────────────
_ORDER_SUBJECT_DEFAULT  = "【発注一覧】{date} ({count}件)"
_ORDER_HEADER_DEFAULT   = "発注日: {date}\n件数: {count}件\n"
_ORDER_ITEM_DEFAULT     = "{supplier_cd}  {supplier_name}  {jan}  {product_cd}  {product_name}  {order_qty}個  {trigger}"
_ORDER_FOOTER_DEFAULT   = "--\n{from_name}"
_EXPIRY_SUBJECT_DEFAULT = "【賞味期限アラート】期限切れ間近の在庫があります - {date}"
_EXPIRY_HEADER_DEFAULT  = "以下の商品の賞味期限が近づいています。早めの対応をお願いします。\n\n確認日: {date}\n"
_EXPIRY_ITEM_DEFAULT    = "  ・[{supplier_cd}]{supplier_name}　{product_cd}　{jan}　{product_name}　LOT:{lot_no}　残り{days_left}日 (期限: {expiry_date})　在庫{quantity}個"
_EXPIRY_FOOTER_DEFAULT  = ""

def _get_template(mail_type):
    """DBからテンプレートを取得、なければデフォルトを返す"""
    try:
        db = get_db()
        row = db.execute("SELECT * FROM mail_templates WHERE mail_type=%s", [mail_type]).fetchone()
        db.close()
        if row:
            return dict(row)
    except Exception:
        pass
    if mail_type == 'order':
        return {'subject': _ORDER_SUBJECT_DEFAULT, 'body_header': _ORDER_HEADER_DEFAULT,
                'body_item': _ORDER_ITEM_DEFAULT, 'body_footer': _ORDER_FOOTER_DEFAULT}
    else:
        return {'subject': _EXPIRY_SUBJECT_DEFAULT, 'body_header': _EXPIRY_HEADER_DEFAULT,
                'body_item': _EXPIRY_ITEM_DEFAULT, 'body_footer': _EXPIRY_FOOTER_DEFAULT}


# 発注キュー（1回の発注チェックで複数商品をまとめて送信するためのバッファ）
_order_queue = []

def queue_order(product: dict, order_qty: int, trigger: str):
    """発注情報をキューに追加"""
    _order_queue.append({
        'product': product,
        'order_qty': order_qty,
        'trigger': trigger,
    })

def flush_order_mail() -> tuple:
    """キューに溜まった発注をまとめてメール送信
    ロジック：
    - 宛先マスターで仕入先CD指定あり → その仕入先の発注のみ個別メール送信
    - 宛先マスターで仕入先CD指定なし（空白）→ 専用宛先指定の仕入先を除いた全仕入先をまとめて1通送信
    """
    global _order_queue
    if not _order_queue:
        return True, '発注なし'

    cfg = _get_cfg()
    from_addr = cfg['from_addr'] or cfg['username']
    if not cfg['server']:
        _order_queue = []
        return False, 'SMTPサーバーが未設定です'

    today = date.today().strftime('%Y-%m-%d')
    trigger_map = {'reorder': '発注点', 'mixed': '混載ロット', 'forced': '強制発注', 'lot': 'ロット数'}
    tmpl = _get_template('order')

    # 宛先マスターから専用指定されている仕入先CDセットを取得
    db = get_db()
    try:
        db.execute("ALTER TABLE mail_recipients ADD COLUMN IF NOT EXISTS supplier_cd TEXT DEFAULT ''")
        db.commit()
    except Exception:
        pass
    dedicated_rows = db.execute(
        "SELECT supplier_cd FROM mail_recipients WHERE is_active=1 AND supplier_cd != '' AND supplier_cd IS NOT NULL"
    ).fetchall()
    db.close()

    # 専用指定されている全仕入先CDをフラットなセットに展開（カンマ区切り対応）
    dedicated_cds = set()
    for r in dedicated_rows:
        for cd in [c.strip() for c in (r['supplier_cd'] or '').split(',') if c.strip()]:
            dedicated_cds.add(cd)

    # 発注アイテムを仕入先CDでグループ化
    from collections import defaultdict
    supplier_groups = defaultdict(list)
    for item in _order_queue:
        scd = item['product'].get('supplier_cd', '') or ''
        supplier_groups[scd].append(item)

    sent_count = 0
    errors = []

    def _build_mail_body(items, tmpl, today, cfg):
        """発注アイテムリストからメール本文を生成"""
        fmt_vars = dict(date=today, count=len(items), from_name=cfg['from_name'])
        skip_keys = {k: '{'+k+'}' for k in ['supplier_cd','supplier_name','jan','product_cd','product_name','order_qty','trigger']}
        try:
            header = tmpl['body_header'].format_map({**fmt_vars, **skip_keys})
        except Exception:
            header = tmpl['body_header']
        item_lines = []
        for item in items:
            p = item['product']
            trig = trigger_map.get(item['trigger'], item['trigger'])
            try:
                item_lines.append(tmpl['body_item'].format(
                    supplier_cd=p.get('supplier_cd',''), supplier_name=p.get('supplier_name',''),
                    jan=p.get('jan',''), product_cd=p.get('product_cd',''),
                    product_name=p.get('product_name',''),
                    order_qty=item['order_qty'], trigger=trig,
                    date=today, count=len(items), from_name=cfg['from_name']
                ))
            except Exception:
                item_lines.append(str(p.get('product_name','')) + ' ' + str(item['order_qty']) + '個')
        try:
            footer = tmpl['body_footer'].format_map({**fmt_vars, **skip_keys})
        except Exception:
            footer = tmpl['body_footer']
        return header + '\n'.join(item_lines) + ('\n' + footer if footer else '')

    def _send_mail(rcpt, items, label):
        nonlocal sent_count
        body = _build_mail_body(items, tmpl, today, cfg)
        try:
            subject = tmpl['subject'].format(date=today, count=len(items))
        except Exception:
            subject = tmpl['subject']
        msg = MIMEMultipart()
        msg['Subject'] = Header(subject, 'utf-8')
        msg['From'] = _build_from(cfg)
        msg['To'] = ', '.join(rcpt)
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        try:
            _send_smtp(cfg, from_addr, rcpt, msg.as_string())
            sent_count += len(items)
        except smtplib.SMTPAuthenticationError:
            errors.append(f'{label}: 認証エラー')
        except smtplib.SMTPException as e:
            errors.append(f'{label}: {e}')
        except Exception as e:
            errors.append(f'{label}: {e}')

    # ① 専用宛先がある仕入先：その仕入先の発注のみ個別メール送信
    for scd, group_items in supplier_groups.items():
        if scd not in dedicated_cds:
            continue  # 専用宛先なし → 共通メールで処理
        rcpt = _get_addrs('order', scd)
        if not rcpt:
            errors.append(f'仕入先{scd}: 宛先なし')
            continue
        _send_mail(rcpt, group_items, f'仕入先{scd}')

    # ② 専用宛先がない仕入先：全部まとめて1通の共通メール送信
    common_items = []
    for scd, group_items in supplier_groups.items():
        if scd not in dedicated_cds:
            common_items.extend(group_items)

    if common_items:
        rcpt = _get_addrs('order', '__common__')  # 空白（専用指定なし）宛先のみ取得
        if rcpt:
            _send_mail(rcpt, common_items, '共通')
        else:
            errors.append('共通宛先: 宛先なし')

    _order_queue = []
    if errors:
        return len(errors) == 0, f'送信完了{sent_count}件 エラー: {"; ".join(errors)}'
    return True, f'発注メール送信完了 {sent_count}件'


def send_order_mail(db, product: dict, order_qty: int, trigger: str) -> tuple:
    """後方互換用：キューに追加して即時送信"""
    queue_order(product, order_qty, trigger)
    return flush_order_mail()


def send_expiry_alert(db, alerts: list) -> tuple:
    cfg = _get_cfg()
    from_addr = cfg['from_addr'] or cfg['username']
    if not cfg['server'] or not alerts:
        return False, 'SMTPサーバー未設定またはアラートなし'

    to_addrs = _get_addrs('expiry')
    if not to_addrs:
        return False, '期限アラートメールの宛先が登録されていません'

    today = date.today().strftime('%Y-%m-%d')
    tmpl = _get_template('expiry')
    header = tmpl['body_header'].format(date=today, count=len(alerts))
    item_lines = []
    for a in alerts:
        try:
            item_lines.append(tmpl['body_item'].format(
                product_name  = a.get('product_name',''),
                product_cd    = a.get('product_cd',''),
                supplier_cd   = a.get('supplier_cd',''),
                supplier_name = a.get('supplier_name',''),
                jan           = a.get('jan',''),
                lot_no        = a.get('lot_no') or '-',
                days_left     = a.get('days_left',''),
                expiry_date   = a.get('expiry_date',''),
                quantity      = a.get('quantity',''),
                date          = today,
                count         = len(alerts),
            ))
        except Exception:
            item_lines.append(str(a.get('product_name','')) + ' 残り' + str(a.get('days_left','')) + '日')
    footer = tmpl['body_footer'].format(date=today, count=len(alerts)) if tmpl['body_footer'] else ''
    body = header + '\n'.join(item_lines) + ('\n' + footer if footer else '')
    subject = tmpl['subject'].format(date=today, count=len(alerts))

    msg = MIMEMultipart()
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = _build_from(cfg)
    msg['To']   = ', '.join(to_addrs)
    msg.attach(MIMEText(body, 'plain', 'utf-8'))

    try:
        _send_smtp(cfg, from_addr, to_addrs, msg.as_string())
        return True, f'期限アラートメール送信完了（{len(alerts)}件）'
    except Exception as e:
        return False, f'送信エラー: {e}'
